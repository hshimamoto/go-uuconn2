// MIT License Copyright(c) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "encoding/binary"
    "fmt"
    "math/rand"
    "net"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/sirupsen/logrus"
    "github.com/hshimamoto/go-session"
)

const BlockPartSize = 1280
const BlockPartNumber = 32
const BlockBufferSize = BlockPartSize * BlockPartNumber

func Kick(q chan bool) {
    if len(q) == 0 {
	q <- true
    }
}

func MessageMask(msg []byte) {
    l := len(msg)
    if l < 12 {
	// TODO warn here?
	return
    }
    mask := msg[4:8]
    //logrus.Infof("Mask with %s: %s", msg[4:8], msg[0:4])
    for i := 0; i < 4; i++ { msg[i] ^= mask[i] } // mask code
    //logrus.Infof("Mask after: %s", msg[0:4])
    for i := 8; i < l; i++ {
	msg[i] ^= mask[i % 4]
    }
}

type UDPMessage struct {
    addr *net.UDPAddr
    msg []byte
}

type LocalSocket struct {
    sock *net.UDPConn
    global string
    running bool
    working bool
    retiring bool
    retired bool
    sender_dead bool
    dead bool
    created time.Time
    m sync.Mutex
    q_sendmsg chan UDPMessage
    // stats
    s_recv, s_recverr uint32
    s_send, s_senderr uint32
}

func NewLocalSocket() *LocalSocket {
    s := &LocalSocket{}
    sock, err := net.ListenUDP("udp", nil)
    if err != nil {
	return nil
    }
    s.sock = sock
    s.global = ""
    s.created = time.Now()
    s.q_sendmsg = make(chan UDPMessage, 64)
    return s
}

func (s *LocalSocket)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("sock:%v ", s.sock.LocalAddr())
    logrus.Infof(header + f, args...)
}

func (s *LocalSocket)UpdateGlobal(global string) bool {
    updated := false
    old := ""
    s.m.Lock()
    if s.global != global {
	old = s.global
	s.global = global
	updated = true
    }
    s.m.Unlock()
    if updated {
	s.Infof("update global [%s] to [%s]", old, global)
    }
    return updated
}

func (s *LocalSocket)String() string {
    s_qlen := fmt.Sprintf("[qlen %d]", len(s.q_sendmsg))
    s_send := fmt.Sprintf("[send %d %d]",
	s.s_send, s.s_senderr)
    s_recv := fmt.Sprintf("[recv %d %d]",
	s.s_recv, s.s_recverr)
    s_state := "running"
    if s.dead {
	s_state = "dead"
    } else if s.retired {
	s_state = "retired"
    } else if s.retiring {
	s_state = "retiring"
    }
    return fmt.Sprintf("localsocket %v %s %s %s %s %s",
	    s.sock.LocalAddr(), s.global,
	    s_qlen, s_send, s_recv, s_state)
}

func (s *LocalSocket)Sender() {
    for s.running {
	msg := <-s.q_sendmsg
	if s.retired {
	    continue
	}
	if msg.addr != nil && len(msg.msg) >= 12 {
	    // make copy
	    sendmsg := make([]byte, len(msg.msg))
	    copy(sendmsg, msg.msg)
	    // mask msg
	    MessageMask(sendmsg)
	    s.sock.WriteToUDP(sendmsg, msg.addr)
	    s.s_send++
	}
    }
    s.sender_dead = true
}

func (s *LocalSocket)Run(cb func(*LocalSocket, *net.UDPAddr, []byte)) {
    s.running = true
    // start sender goroutine
    go s.Sender()
    buf := make([]byte, 1500)
    for s.running {
	// ReadFromUDP
	// TODO remove Deadline
	s.sock.SetReadDeadline(time.Now().Add(time.Second))
	n, addr, err := s.sock.ReadFromUDP(buf)
	if n <= 0 || err != nil {
	    if e, ok := err.(net.Error); ok && e.Timeout() {
		continue
	    }
	    s.s_recverr++
	    s.Infof("ReadFromUDP: %v", err)
	    continue
	}
	s.s_recv++
	//msg := make([]byte, n)
	//copy(msg, buf[:n])
	msg := buf[:n]
	if msg[0] == 'P' {
	    // v1 "Probe" ?
	    if len(msg) >= 5 && string(msg[0:5]) == "Probe" {
		cb(s, addr, msg)
		continue
	    }
	}
	if n >= 12 {
	    // mask msg
	    MessageMask(msg)
	    prev := time.Now()
	    cb(s, addr, msg)
	    d := time.Since(prev)
	    if d > time.Second {
		s.Infof("handler takes too long %v %s", d, msg[0:4])
	    }
	}
    }
    // stop sender
    s.q_sendmsg <- UDPMessage{}
    // wait sender has been dead
    for ! s.sender_dead {
	time.Sleep(time.Second)
    }
    s.sock.Close()
    s.dead = true
}

func (s *LocalSocket)Stop() {
    s.running = false
}

func (s *LocalSocket)Retire() {
    if s.retiring {
	s.retired = true
	s.Infof("Retired")
	return
    }
    s.Infof("Retiring")
    s.retiring = true
}

func (s *LocalSocket)IsReady() bool {
    if s.retiring {
	return false
    }
    return s.working
}

type DataBlock struct {
    blkid uint32
    data []byte
    rest uint32
    sz uint32
    msgs [BlockPartNumber]([]byte)
    //
    tag string
    // stats
    s_getblk, s_oldblkid, s_badblkid, s_baddata, s_dup uint32
}

func NewDataBlock(tag string) *DataBlock {
    blk := &DataBlock{
	blkid: 0,
	data: nil,
	rest: 0,
	sz: 0,
	tag: tag,
    }
    return blk
}

func (blk *DataBlock)Infof(f string, args ...interface{}) {
    logrus.Infof(blk.tag + f, args...)
}

func (blk *DataBlock)SetupMessages(data []byte) {
    nparts := (len(data) + BlockPartSize - 1) / BlockPartSize
    l := len(data)
    c := 0
    for i := 0; i < BlockPartNumber; i++ {
	// create msg template
	n := l
	if n > BlockPartSize {
	    n = BlockPartSize
	}
	msg := make([]byte, 12+4+16+n)
	binary.LittleEndian.PutUint32(msg[16 +  0:], blk.blkid)
	binary.LittleEndian.PutUint32(msg[16 +  4:], uint32(nparts))
	binary.LittleEndian.PutUint32(msg[16 +  8:], uint32(i))
	binary.LittleEndian.PutUint32(msg[16 + 12:], uint32(n))
	copy(msg[16 + 16:], data[c:c+n])
	blk.msgs[i] = msg
	blk.rest |= (1 << i)
	l -= n
	c += n
	if l <= 0 {
	    break
	}
    }
    //blk.Infof("%d parts rest 0x%x", nparts, blk.rest)
}

func (blk *DataBlock)GetBlock(data []byte) {
    blkid := binary.LittleEndian.Uint32(data[0:4])
    nr_parts := binary.LittleEndian.Uint32(data[4:8])
    partid := binary.LittleEndian.Uint32(data[8:12])
    partlen := binary.LittleEndian.Uint32(data[12:16])
    data = data[16:]
    //blk.Infof("getblock %d %d %d %d %d", blkid, nr_parts, partid, partlen, len(data))
    if blk.blkid < blkid {
	// ignore
	blk.s_oldblkid++
	return
    }
    if blk.blkid > blkid {
	//blk.Infof("blkid mismatch %d vs %d", blk.blkid, blkid)
	// ignore
	blk.s_badblkid++
	return
    }
    // check
    if nr_parts > BlockPartNumber || partid >= BlockPartNumber || partlen > BlockPartSize {
	// bad data
	blk.s_baddata++
	return
    }
    // fill bits
    for i := nr_parts; i < BlockPartNumber; i++ {
	blk.rest |= 1 << i
    }
    // already have?
    if (blk.rest & (1 << partid)) != 0 {
	blk.s_dup++
	return
    }
    blk.s_getblk++
    if blk.data == nil {
	blk.data = make([]byte, BlockBufferSize)
    }
    offset := partid * BlockPartSize
    copy(blk.data[offset:], data[:partlen])
    //logrus.Infof("copied [%s]", string(data[:partlen]))
    blk.rest |= 1 << partid
    if blk.sz < (offset + partlen) {
	blk.sz = offset + partlen
    }
}

func (blk *DataBlock)NextBlock() {
    blk.blkid++
    blk.rest = 0
    blk.sz = 0
}

func (blk *DataBlock)MarkClose() {
    blk.blkid = 0xffffffff
    blk.rest = 3 // 3 close packets
}

type Buffer struct {
    data []byte
    idx int
}

func NewBuffer() *Buffer {
    return &Buffer{
	data: make([]byte, BlockBufferSize),
	idx: 0,
    }
}

type Stream struct {
    streamid uint32
    lopen, ropen bool
    createdTime time.Time
    running bool
    // outgoing block
    oblk *DataBlock
    oblkacks uint32 // number of acks in this block
    oblkack uint32  // current ack bits
    oblkAcked time.Time
    oblkLastSend time.Time
    oblkResend bool
    oblkTrigSend bool
    oblkMaxSize, oblkNextMaxSize uint32
    oblkRTTCheckTime time.Time
    oblkRTT time.Duration
    // incoming block
    iblk *DataBlock
    // mutex
    m sync.Mutex
    //
    q_work chan bool
    q_acked chan bool
    q_flush chan bool
    //
    sweep bool
    // stats
    s_sendmsg, s_resendmsg, s_sendack uint32
    s_recvack, s_recvunkack uint32
    s_resendtrigger, s_getblock, s_getack uint32
    s_selfreader, s_selfreaderclose uint32
    s_selfread, s_selfwrite uint32
    // misc
    s_bufswap, s_curridx, s_drain uint32
    s_bufszdown, s_bufszup uint32
}

func NewStream(streamid uint32) *Stream {
    st := &Stream{
	streamid: streamid,
	createdTime: time.Now(),
    }
    st.oblk = NewDataBlock(fmt.Sprintf("oblk st:0x%x ", streamid))
    st.iblk = NewDataBlock(fmt.Sprintf("iblk st:0x%x ", streamid))
    st.q_work = make(chan bool, 64)
    st.q_acked = make(chan bool, 64)
    st.q_flush = make(chan bool, 64)
    return st
}

func (st *Stream)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("stream:0x%x ", st.streamid)
    logrus.Infof(header + f, args...)
}

func (st *Stream)StatString() string {
    s_live := fmt.Sprintf("[live %v]", time.Since(st.createdTime))
    s_rw := fmt.Sprintf("[read %d write %d]",
	st.s_selfread, st.s_selfwrite)
    s_send := fmt.Sprintf("[send %d (%d resend) msgs %d acks RTT(%v)]",
	st.s_sendmsg, st.s_resendmsg, st.s_sendack, st.oblkRTT)
    s_recv := fmt.Sprintf("[recv %d (%d unknown) acks]",
	st.s_recvack, st.s_recvunkack)
    s_oblk := fmt.Sprintf("[oblk %d 0x%x 0x%x %d %d]",
	st.oblk.blkid, st.oblk.rest, st.oblkack,
	st.oblkMaxSize, st.oblkNextMaxSize)
    s_iblk := fmt.Sprintf("[iblk %d get %d err %d %d %d %d]",
	st.iblk.blkid,
	st.iblk.s_getblk,
	st.iblk.s_oldblkid, st.iblk.s_badblkid,
	st.iblk.s_baddata, st.iblk.s_dup)
    s_wakeup := fmt.Sprintf("[wakeup %d %d %d %d %d]",
	st.s_resendmsg, st.s_getblock, st.s_getack,
	st.s_selfreader, st.s_selfreaderclose)
    s_misc := fmt.Sprintf("[misc %d %d %d %d %d]",
	st.s_bufswap, st.s_curridx, st.s_drain,
	st.s_bufszdown, st.s_bufszup)
    return fmt.Sprintf("%s %s %s %s %s %s %s %s",
	s_live, s_rw, s_send, s_recv, s_oblk, s_iblk, s_wakeup, s_misc)
}

func (st *Stream)FlushInblock(conn net.Conn) {
    //st.Infof("Flush %d bytes", st.iblk.sz)
    sz := 0
    for uint32(sz) < st.iblk.sz {
	n, _ := conn.Write(st.iblk.data[sz:st.iblk.sz])
	sz += n
    }
}

func (st *Stream)SendBlock(code string, q chan []byte) {
    st.m.Lock()
    oblk := st.oblk
    acked := st.oblkack
    resend := st.oblkResend
    rest := st.oblk.rest
    st.m.Unlock()
    if oblk == nil {
	return
    }
    if rest == 0 {
	return
    }
    if resend {
	d := 10 * time.Millisecond
	if time.Since(st.oblkLastSend) < d {
	    st.m.Lock()
	    trig := st.oblkTrigSend
	    st.oblkTrigSend = true
	    st.m.Unlock()
	    if trig == false {
		go func() {
		    time.Sleep(10 * time.Millisecond)
		    st.m.Lock()
		    st.oblkTrigSend = false
		    st.m.Unlock()
		    Kick(st.q_work)
		    st.s_resendtrigger++
		}()
	    }
	    return
	}
	st.m.Lock()
	if st.oblkacks > 1 {
	    st.s_bufszdown++
	    if st.oblkNextMaxSize > BlockBufferSize / 8 {
		st.oblkNextMaxSize -= st.oblkNextMaxSize / 8
	    }
	    if st.oblkNextMaxSize < BlockBufferSize / 8 {
		st.oblkNextMaxSize = BlockBufferSize / 8
	    }
	}
	st.m.Unlock()
    } else {
	st.m.Lock()
	st.s_bufszup++
	if st.oblkNextMaxSize > BlockBufferSize / 8 {
	    st.oblkNextMaxSize += st.oblkNextMaxSize / 8
	}
	if st.oblkNextMaxSize > BlockBufferSize {
	    st.oblkNextMaxSize = BlockBufferSize
	}
	st.m.Unlock()
    }
    st.oblkLastSend = time.Now()
    if oblk.blkid == 0xffffffff {
	if oblk.rest == 0 {
	    return
	}
	oblk.rest--
	msg := []byte("sendSSSSDDDDXXXXBBBBXXXXXXXXXXXX")
	copy(msg[0:4], []byte(code))
	binary.LittleEndian.PutUint32(msg[12:], st.streamid)
	binary.LittleEndian.PutUint32(msg[16:], oblk.blkid)
	q <- msg
	return
    }
    if resend == false {
	st.oblkRTTCheckTime = time.Now()
    }
    cnt := uint32(0)
    maxresend := (st.oblkMaxSize / BlockPartSize) / 2
    if acked == 0 {
	maxresend = 1
    }
    for i := 0; i < BlockPartNumber; i++ {
	if oblk.rest & (1 << i) == 0 {
	    continue
	}
	if acked & (1 << i) != 0 {
	    continue
	}
	msg := oblk.msgs[i]
	// fixup msg code
	copy(msg[0:4], []byte(code))
	binary.LittleEndian.PutUint32(msg[12:], st.streamid)
	q <- msg
	st.s_sendmsg++
	if resend {
	    st.s_resendmsg++
	    cnt++
	    if cnt >= maxresend {
		break
	    }
	}
    }
    st.m.Lock()
    st.oblkResend = true
    st.m.Unlock()
}

func (st *Stream)GetBlock(data []byte) {
    // blockdata
    // |blkid|nr parts|part id|part len|data...|
    if len(data) < 16 {
	return
    }
    blkid := binary.LittleEndian.Uint32(data[0:4])
    if blkid == 0xffffffff {
	st.Infof("remote closed")
	st.m.Lock()
	st.ropen = false
	st.m.Unlock()
	return
    }
    // check incoming block
    wakeup := false
    st.m.Lock()
    prevack := st.iblk.rest
    st.iblk.GetBlock(data)
    if st.iblk.rest != prevack {
	wakeup = true
    }
    st.m.Unlock()
    if wakeup {
	Kick(st.q_work)
	st.s_getblock++
    }
}

func (st *Stream)GetAck(blkid, ack uint32) {
    wakeup := false
    st.m.Lock()
    st.oblkAcked = time.Now()
    if st.oblk.blkid == blkid {
	if st.oblkack == 0 {
	    st.oblkRTT = (st.oblkRTT + time.Since(st.oblkRTTCheckTime)) / 2
	}
	st.oblkacks++
	st.oblkack |= ack
	wakeup = true
	st.s_recvack++
    } else if st.oblk.blkid == blkid + 1 {
	// ack for prevous block
	// counts but ignore
	st.s_recvack++
    } else {
	st.s_recvunkack++
    }
    st.m.Unlock()
    if wakeup {
	Kick(st.q_work)
	st.s_getack++
    }
}

func (st *Stream)CheckOutblockAck() {
    acked := false
    st.m.Lock()
    if st.oblkack == 0xffffffff {
	st.oblk.NextBlock()
	st.oblkacks = 0
	st.oblkack = 0
	st.oblkResend = false
	acked = true
    }
    st.m.Unlock()
    if acked {
	Kick(st.q_acked)
    }
}

func (st *Stream)SetupMessages(data []byte) {
    st.m.Lock()
    defer st.m.Unlock()
    st.oblk.SetupMessages(data)
    st.oblkAcked = time.Now()
}

func (st *Stream)SelfReader(conn net.Conn) {
    var m sync.Mutex
    curr := NewBuffer()
    prev := NewBuffer()
    closed := false
    reading := false
    q_wait := make(chan bool, 16)
    q_read := make(chan bool, 16)
    go func() {
	for st.running {
	    m.Lock()
	    rest := int(st.oblkMaxSize) - curr.idx
	    if rest <= 0 {
		m.Unlock()
		<-q_wait // wait buffer
		continue
	    }
	    reading = true
	    b := curr
	    m.Unlock()
	    conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	    n, err := conn.Read(b.data[b.idx:])
	    st.s_selfread++
	    m.Lock()
	    reading = false
	    if n > 0 {
		b.idx += n
		m.Unlock()
		Kick(q_read)
		continue
	    }
	    if e, ok := err.(net.Error); ok && e.Timeout() {
		m.Unlock()
		continue
	    }
	    m.Unlock()
	    st.Infof("Read: %v", err)
	    closed = true
	    Kick(q_read)
	    break
	}
    }()
    drain := 0
    for st.running {
	wakeup := false
	m.Lock()
	if curr.idx > drain {
	    st.m.Lock()
	    rest := st.oblk.rest
	    st.m.Unlock()
	    if rest == 0 {
		// create DataBlock
		st.SetupMessages(curr.data[drain:curr.idx])
		Kick(st.q_work)
		st.s_selfreader++
		// check
		if reading {
		    drain = curr.idx
		} else {
		    // swap buffer
		    tmp := curr
		    curr = prev
		    prev = tmp
		    curr.idx = 0
		    drain = 0
		    st.m.Lock()
		    st.s_bufswap++
		    st.oblkMaxSize = st.oblkNextMaxSize
		    st.m.Unlock()
		}
		wakeup = true
	    }
	}
	st.m.Lock()
	st.s_curridx = uint32(curr.idx)
	st.s_drain = uint32(drain)
	st.m.Unlock()
	m.Unlock()
	if wakeup {
	    Kick(q_wait)
	}
	if closed {
	    st.m.Lock()
	    rest := st.oblk.rest
	    st.m.Unlock()
	    // check data to send
	    if curr.idx == drain {
		curr.idx = 0  // FORCE to reset
	    }
	    if curr.idx == 0 && rest == 0 {
		// no data
		st.Infof("closed and no data")
		st.oblk.MarkClose()
		st.m.Lock()
		st.lopen = false
		st.m.Unlock()
		Kick(st.q_work)
		st.s_selfreaderclose++
		return
	    }
	    // self side connection closed
	    // wakeup from housekeeping
	}
	select {
	case <-st.q_acked:
	case <-q_read:
	}
    }
}

func (st *Stream)SelfWriter(conn net.Conn) {
    for st.running {
	st.m.Lock()
	rest := st.iblk.rest
	st.m.Unlock()
	if rest == 0xffffffff {
	    st.FlushInblock(conn)
	    st.s_selfwrite++
	    st.m.Lock()
	    st.iblk.NextBlock()
	    st.m.Unlock()
	}
	<-st.q_flush
    }
}

func (st *Stream)Run(code, ackcode string, q_sendmsg, q_broadcast chan []byte, conn net.Conn) {
    ackmsg := []byte("rackSSSSDDDDXXXXBBBBAAAA")
    copy(ackmsg[0:4], []byte(ackcode))
    binary.LittleEndian.PutUint32(ackmsg[12:], st.streamid)
    lastAck := time.Now()
    prevack := uint32(0)
    st.oblkAcked = time.Now()
    st.oblkLastSend = time.Now()
    st.oblkMaxSize = BlockBufferSize
    st.oblkNextMaxSize = BlockBufferSize
    st.oblkRTT = 0
    st.running = true
    broadcast := false
    bcastStart := time.Now()
    // start SelfReader
    go st.SelfReader(conn)
    go st.SelfWriter(conn)
    for st.running {
	// no acks ?
	if time.Since(st.oblkAcked) > 10 * time.Second {
	    // start broadcast
	    broadcast = true
	    bcastStart = time.Now()
	} else {
	    if broadcast && time.Since(bcastStart) > time.Minute {
		broadcast = false
	    }
	}
	if time.Since(st.oblkAcked) > time.Minute {
	    // TODO: check in case no send from myside...
	    st.Infof("no acks from remote: last 0x%x/0x%x", st.oblk.blkid, st.oblkack)
	    // assume remote closed
	    st.m.Lock()
	    st.ropen = false
	    st.lopen = false
	    st.m.Unlock()
	    st.oblkAcked = time.Now()
	}
	st.SendBlock(code, q_sendmsg)
	st.CheckOutblockAck()
	sendack := false
	flush := false
	st.m.Lock()
	blkid := st.iblk.blkid
	ack := st.iblk.rest
	if ack == 0 && blkid > 0 {
	    blkid--
	    ack = 0xffffffff
	}
	if (ack == 0xffffffff && prevack != 0xffffffff) || time.Since(lastAck) > time.Millisecond {
	    binary.LittleEndian.PutUint32(ackmsg[16:], blkid)
	    binary.LittleEndian.PutUint32(ackmsg[20:], ack)
	    prevack = ack
	    sendack = true
	}
	if st.iblk.rest == 0xffffffff {
	    flush = true
	}
	if st.ropen == false {
	    sendack = false
	}
	st.m.Unlock()
	if flush {
	    Kick(st.q_flush)
	}
	if sendack {
	    st.s_sendack++
	    if broadcast {
		q_broadcast <- ackmsg
	    } else {
		q_sendmsg <- ackmsg
	    }
	    lastAck = time.Now()
	}
	<-st.q_work
	for len(st.q_work) > 0 {
	    <-st.q_work
	}
	/*
	st.Infof("wakeup [%d %d %d] o:0x%x:0x%x i:0x%x",
		st.s_sendmsg, st.s_sendack, st.s_recvack,
		st.oblk.rest, st.oblkack, st.iblk.rest)
	*/
    }
    st.Infof("done")
}

func (st *Stream)Stop() {
    if st.running == false {
	// nothing to do
	return
    }
    st.running = false
    // kick workers
    st.KickWorkers()
}

func (st *Stream)KickWorkers() {
    Kick(st.q_work)
    Kick(st.q_acked)
    Kick(st.q_flush)
}

func (st *Stream)Destroy() {
    if st.sweep == false {
	st.Infof("not sweeped")
	return
    }
    // close queue
    close(st.q_work)
    close(st.q_acked)
    close(st.q_flush)
    // show stats
    stat := st.StatString()
    st.Infof("total %s", stat)
}

func (st *Stream)Stat() string {
    stat := st.StatString()
    return fmt.Sprintf("0x%x %s", st.streamid, stat)
}

type RemoteAddr struct {
    addr string
    lastUpdate time.Time
}

type RemotePeer struct {
    remotes []*RemoteAddr
    peerid uint32
}

func (p *RemotePeer)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("remotepeer:0x%x ", p.peerid)
    logrus.Infof(header + f, args...)
}

func (p *RemotePeer)StringRemotes() string {
    remotes := []string{}
    for _, r := range p.remotes {
	s := fmt.Sprintf("%s[%v]", r.addr, time.Since(r.lastUpdate))
	remotes = append(remotes, s)
    }
    return fmt.Sprintf("%v", remotes)
}

func (p *RemotePeer)String() string {
    return fmt.Sprintf("remotepeer:0x%x %s", p.peerid, p.StringRemotes())
}

func (p *RemotePeer)Update(addr string) {
    remotes := []*RemoteAddr{}
    for _, r := range p.remotes {
	if r.addr != addr {
	    remotes = append(remotes, r)
	}
    }
    r := &RemoteAddr{
	addr: addr,
	lastUpdate: time.Now(),
    }
    p.remotes = append(remotes, r)
}

func (p *RemotePeer)Freshers() []*RemoteAddr {
    remotes := []*RemoteAddr{}
    for _, r := range p.remotes {
	if time.Since(r.lastUpdate) < 30 * time.Second {
	    remotes = append(remotes, r)
	}
    }
    if len(remotes) == 0 {
	p.Infof("no freshers %s", p.StringRemotes())
	return p.remotes
    }
    return remotes
}

func (p *RemotePeer)CheckRemoteAddrs() {
    // check remotes and remove
    remotes := []*RemoteAddr{}
    for _, r := range p.remotes {
	if time.Since(r.lastUpdate) < time.Minute {
	    remotes = append(remotes, r)
	}
    }
    p.remotes = remotes
}

type Connection struct {
    remote *RemotePeer
    hostname string
    lstreams []*Stream
    rstreams []*Stream
    streamid uint32
    startTime time.Time
    lastProbe time.Time
    lastRecvProbe time.Time
    lastInform time.Time
    lastShow time.Time
    sockidx int
    m sync.Mutex
    q_sendmsg chan []byte
    q_broadcast chan []byte
    running bool
    //
    updateTime time.Time
    stopTime time.Time
    // stats
    s_sendmsg uint32
    s_sendbytes uint64
    s_broadcast uint32
    s_lookuplocalstream, s_lookupremotestream uint32
}

func NewConnection(peerid uint32) *Connection {
    now := time.Now()
    c := &Connection{
	remote: &RemotePeer{ remotes: []*RemoteAddr{}, peerid: peerid },
	startTime: now,
	updateTime: now,
	lastRecvProbe: now,
	stopTime: now,
	lastShow: now,
	q_sendmsg: make(chan []byte, 64),
	q_broadcast: make(chan []byte, 32),
    }
    return c
}

func (c *Connection)StringRemotes() string {
    return c.remote.StringRemotes()
}

func (c *Connection)String() string {
    c.m.Lock()
    defer c.m.Unlock()
    stats := fmt.Sprintf("[send %d msgs %d bytes %d bcast] [recv %d locals %d remotes]",
	    c.s_sendmsg, c.s_sendbytes, c.s_broadcast,
	    c.s_lookuplocalstream, c.s_lookupremotestream)
    remotes := c.StringRemotes()
    return fmt.Sprintf("0x%x %s [%v] local:%d remote:%d %s %s",
	    c.remote.peerid, c.hostname, time.Since(c.startTime),
	    len(c.lstreams), len(c.rstreams),
	    remotes, stats)
}

func (c *Connection)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("connection:0x%x ", c.remote.peerid)
    logrus.Infof(header + f, args...)
}

func (c *Connection)Update(addr string) {
    c.m.Lock()
    defer c.m.Unlock()
    c.remote.Update(addr)
    c.updateTime = time.Now()
}

func (c *Connection)Freshers() []*RemoteAddr {
    c.m.Lock()
    defer c.m.Unlock()
    return c.remote.Freshers()
}

func (c *Connection)NewLocalStream() *Stream {
    c.m.Lock()
    streamid := c.streamid
    c.streamid++
    c.m.Unlock()
    st := NewStream(streamid)
    c.m.Lock()
    c.lstreams = append(c.lstreams, st)
    c.m.Unlock()
    return st
}

func (c *Connection)LookupLocalStream(lid uint32) *Stream {
    c.m.Lock()
    defer c.m.Unlock()
    c.s_lookuplocalstream++
    for _, s := range c.lstreams {
	if s.streamid == lid {
	    return s
	}
    }
    return nil
}

func (c *Connection)NewRemoteStream(rid uint32) (*Stream, bool) {
    c.m.Lock()
    defer c.m.Unlock()
    for _, st := range c.rstreams {
	if st.streamid == rid {
	    return st, false
	}
    }
    st := NewStream(rid)
    c.rstreams = append(c.rstreams, st)
    return st, true
}

func (c *Connection)LookupRemoteStream(rid uint32) *Stream {
    c.m.Lock()
    defer c.m.Unlock()
    c.s_lookupremotestream++
    for _, s := range c.rstreams {
	if s.streamid == rid {
	    return s
	}
    }
    return nil
}

func (c *Connection)KickStreams() {
    if len(c.lstreams) == 0 && len(c.rstreams) == 0 {
	return
    }
    //c.Infof("KickStreams")
    for _, st := range c.lstreams {
	st.KickWorkers()
    }
    for _, st := range c.rstreams {
	st.KickWorkers()
    }
}

func (c *Connection)SweepStreams() {
    if len(c.lstreams) == 0 && len(c.rstreams) == 0 {
	return
    }
    //c.Infof("SweepStreams")
    sweeped := []*Stream{}
    streams := []*Stream{}
    for _, st := range c.lstreams {
	del := false
	stop := false
	st.m.Lock()
	if st.sweep {
	    del = true
	} else if st.running == false {
	    st.sweep = true
	} else if st.ropen == false && st.lopen == false {
	    stop = true
	}
	st.m.Unlock()
	if stop {
	    st.Stop()
	}
	if del {
	    sweeped = append(sweeped, st)
	} else {
	    streams = append(streams, st)
	}
    }
    if len(streams) < len(c.lstreams) {
	c.lstreams = streams
	c.Infof("lstreams sweeped")
    }
    streams = []*Stream{}
    for _, st := range c.rstreams {
	del := false
	stop := false
	st.m.Lock()
	if st.sweep {
	    del = true
	} else if st.running == false {
	    st.sweep = true
	} else if st.ropen == false && st.lopen == false {
	    stop = true
	}
	st.m.Unlock()
	if stop {
	    st.Stop()
	}
	if del {
	    sweeped = append(sweeped, st)
	} else {
	    streams = append(streams, st)
	}
    }
    if len(streams) < len(c.rstreams) {
	c.rstreams = streams
	c.Infof("rstreams sweeped")
    }
    for _, st := range sweeped {
	st.Destroy()
    }
}

func (c *Connection)CheckRemotePeer() {
    // check remotes and remove
    c.m.Lock()
    c.remote.CheckRemoteAddrs()
    c.m.Unlock()
}

func (c *Connection)Run(q chan UDPMessage) {
    c.running = true
    for c.running {
	broadcast := false
	var sendmsg []byte = nil
	select {
	case sendmsg = <-c.q_sendmsg:
	case sendmsg = <-c.q_broadcast:
	    broadcast = true
	    c.s_broadcast++
	}
	if len(sendmsg) > 12 {
	    c.s_sendmsg++
	    c.s_sendbytes += uint64(len(sendmsg))
	    remotes := c.Freshers()
	    if len(remotes) == 0 {
		continue
	    }
	    for i := len(remotes) - 1; i >= 0; i-- {
		r := remotes[i]
		addr, err := net.ResolveUDPAddr("udp", r.addr)
		if err != nil {
		    continue
		}
		// update msg
		// replace dest peerid
		binary.LittleEndian.PutUint32(sendmsg[8:], c.remote.peerid)
		q <- UDPMessage{ msg:sendmsg, addr: addr }
		if ! broadcast {
		    break
		}
	    }
	}
    }
    c.Infof("stopped")
    c.stopTime = time.Now()
}

func (c *Connection)Stop() {
    if c.running == false {
	return
    }
    c.running = false
    c.q_sendmsg <- []byte{}
    c.Infof("stopping")
}

type LocalServer struct {
    remote *Connection
    laddr, raddr string
    serv *session.Server
    running bool
    streams int
    m sync.Mutex
    // stats
    s_accept uint32
}

func NewLocalServer(laddr, raddr string, remote *Connection) (*LocalServer, error) {
    ls := &LocalServer{
	remote: remote,
	laddr: laddr,
	raddr: raddr,
    }
    serv, err := session.NewServer(laddr, func(conn net.Conn) {
	ls.Handle_Session(conn)
    })
    if err != nil {
	return nil, err
    }
    ls.serv = serv
    return ls, nil
}

func (ls *LocalServer)String() string {
    peerid := uint32(0)
    if ls.remote != nil {
	peerid = ls.remote.remote.peerid
    }
    stats := fmt.Sprintf("%d", ls.s_accept)
    return fmt.Sprintf("localserver %s %s 0x%x %s", ls.laddr, ls.raddr, peerid, stats)
}

func (ls *LocalServer)Handle_Session(lconn net.Conn) {
    defer lconn.Close()
    if ls.remote == nil {
	return
    }

    ls.m.Lock()
    ls.streams++
    ls.s_accept++
    ls.m.Unlock()
    defer func() {
	ls.m.Lock()
	ls.streams--
	ls.m.Unlock()
    }()

    // prepare stream
    st := ls.remote.NewLocalStream()
    st.lopen = true
    // prepare message
    msg := []byte("openSSSSDDDDXXXX" + ls.raddr)
    binary.LittleEndian.PutUint32(msg[12:], st.streamid)
    for st.ropen == false {
	// try to send
	ls.remote.q_broadcast <- msg
	// wait
	<-st.q_work
    }

    // forground runner
    st.Run("rsnd", "rrck", ls.remote.q_sendmsg, ls.remote.q_broadcast, lconn)
    logrus.Infof("LocalServer: handler done")
}

func (ls *LocalServer)Run() {
    ls.running = true
    ls.serv.Run()
}

func (ls *LocalServer)Stop() {
    ls.running = false
    ls.serv.Stop()
}

type RemoteServer struct {
    remote *Connection
    stream *Stream
    laddr, raddr string
    lastUpdate time.Time
    running bool
}

func NewRemoteServer(laddr, raddr string, remote *Connection, stream *Stream) (*RemoteServer, error) {
    rs := &RemoteServer{
	remote: remote,
	stream: stream,
	laddr: laddr,
	raddr: raddr,
	lastUpdate: time.Now(),
    }
    return rs, nil
}

func (rs *RemoteServer)String() string {
    peerid := uint32(0)
    if rs.remote != nil {
	peerid = rs.remote.remote.peerid
    }
    return fmt.Sprintf("remoteserver %s %s 0x%x", rs.laddr, rs.raddr, peerid)
}

func (rs *RemoteServer)Run() {
    rs.running = true

    rs.stream.lopen = true
    // dial to "local addr" (ask from remote)
    conn, err := session.Dial(rs.raddr)
    if err != nil {
	logrus.Infof("Dial: %v", err)
	return
    }
    defer conn.Close()

    // ack message
    ack := []byte("oackSSSSDDDDXXXXRRRR")
    binary.LittleEndian.PutUint32(ack[12:], rs.stream.streamid)
    // use connection queue
    rs.remote.q_broadcast <- ack

    st := rs.stream

    // forground runner
    st.Run("rrcv", "rsck", rs.remote.q_sendmsg, rs.remote.q_broadcast, conn)

    // stop streams
    st.Stop()

    rs.running = false
    rs.lastUpdate = time.Now()
}

func (rs *RemoteServer)Stop() {
    rs.running = false
    rs.lastUpdate = time.Now()
}

type Peer struct {
    lsocks []*LocalSocket
    checkers []string
    conns []*Connection
    peers []*RemotePeer
    lservs []*LocalServer
    rservs []*RemoteServer
    serv *session.Server
    peerid uint32
    hostname string
    running bool
    password string
    lastCheck time.Time
    lastShow time.Time
    sockRetireTime time.Time
    d_retire time.Duration
    d_housekeep time.Duration
    // flag global addr changed
    globalChanged bool
    q_sendmsg chan UDPMessage
    m sync.Mutex
    // stats
    s_udp uint32
    s_ignore uint32
    s_probe uint32
    s_inform uint32
    s_peer uint32
    s_open, s_openack uint32
    s_rsnd, s_rrcv, s_rsck, s_rrck uint32
    s_housekeep uint32
    s_badpass uint32
}

func NewPeer(laddr string) (*Peer, error) {
    p := &Peer{}
    // create local API server
    serv, err := session.NewServer(laddr, func(conn net.Conn) {
	p.API_handler(conn)
    })
    if err != nil {
	return nil, fmt.Errorf("NewServer: %v", err)
    }
    p.serv = serv
    p.lsocks = []*LocalSocket{}
    // start at most 3 sockets
    for i := 0; i < 3; i++ {
	s := NewLocalSocket()
	if s != nil {
	    p.lsocks = append(p.lsocks, s)
	}
    }
    logrus.Infof("created %d local sockets", len(p.lsocks))
    rand.Seed(time.Now().Unix() + int64(os.Getpid()))
    p.peerid = rand.Uint32()
    if hostname, err := os.Hostname(); err == nil {
	p.hostname = hostname
    }
    p.lastCheck = time.Now()
    p.lastShow = time.Now()
    p.q_sendmsg = make(chan UDPMessage, 128)
    p.d_housekeep = 5 * time.Second
    p.sockRetireTime = time.Now()
    p.d_retire = 60 * time.Minute
    return p, nil
}

func (p *Peer)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("peer:0x%x ", p.peerid)
    logrus.Infof(header + f, args...)
}

func (p *Peer)FindConnection(peerid uint32) *Connection {
    p.m.Lock()
    defer p.m.Unlock()
    for _, c := range p.conns {
	if c.remote.peerid == peerid {
	    return c
	}
    }
    c := NewConnection(peerid)
    // TODO start here?
    go c.Run(p.q_sendmsg)
    p.conns = append(p.conns, c)
    return c
}

func (p *Peer)LookupConnectionById(peerid uint32) *Connection {
    p.m.Lock()
    defer p.m.Unlock()
    for _, c := range p.conns {
	if c.remote.peerid == peerid {
	    return c
	}
    }
    return nil
}

func (p *Peer)LookupConnectionByName(name string) *Connection {
    p.m.Lock()
    defer p.m.Unlock()
    for _, c := range p.conns {
	if c.hostname == name {
	    return c
	}
    }
    return nil
}

func (p *Peer)LookupConnection(name string) *Connection {
    c := p.LookupConnectionByName(name)
    if c != nil {
	return c
    }
    if name[0:2] == "0x" {
	name = name[2:]
    }
    peerid64, err := strconv.ParseUint(name, 16, 32)
    if err != nil {
	return nil
    }
    peerid := uint32(peerid64)
    return p.LookupConnectionById(peerid)
}

// data must contain streamid
func (p *Peer)LookupConnectionAndLocalStream(peerid uint32, data []byte) (*Connection, *Stream) {
    if len(data) < 4 {
	return nil, nil
    }
    c := p.LookupConnectionById(peerid)
    if c == nil {
	return nil, nil
    }
    streamid := binary.LittleEndian.Uint32(data[0:4])
    // Lookup Remote Stream
    return c, c.LookupLocalStream(streamid)
}

// data must contain streamid
func (p *Peer)LookupConnectionAndRemoteStream(peerid uint32, data []byte) (*Connection, *Stream) {
    if len(data) < 4 {
	return nil, nil
    }
    c := p.LookupConnectionById(peerid)
    if c == nil {
	return nil, nil
    }
    streamid := binary.LittleEndian.Uint32(data[0:4])
    // Lookup Remote Stream
    return c, c.LookupRemoteStream(streamid)
}

func (p *Peer)SendFromAll(udpmsg UDPMessage) {
    p.m.Lock()
    socks := p.lsocks
    p.m.Unlock()
    for _, sock := range socks {
	if sock.retiring {
	    continue
	}
	sock.q_sendmsg <- udpmsg
    }
}

func (p *Peer)ProbeTo(addr *net.UDPAddr, dstpid uint32) {
    msg := []byte(fmt.Sprintf("probSSSSDDDD%v %v", addr, p.password))
    binary.LittleEndian.PutUint32(msg[4:], p.peerid)
    binary.LittleEndian.PutUint32(msg[8:], dstpid)
    p.SendFromAll(UDPMessage{
	addr: addr,
	msg: msg,
    })
}

func (p *Peer)InformTo(addr *net.UDPAddr, dstpid uint32) {
    // Inform Message
    // |info|spid|dpid|hostname global...|
    addrs := []string{p.hostname}
    p.m.Lock()
    for _, sock := range p.lsocks {
	if sock.retiring {
	    // Don't inform retiring socket
	    continue
	}
	if sock.global != "" {
	    addrs = append(addrs, sock.global)
	}
    }
    p.m.Unlock()
    if len(addrs) == 1 {
	return
    }
    msg := []byte(fmt.Sprintf("infoSSSSDDDD%s", strings.Join(addrs, " ")))
    binary.LittleEndian.PutUint32(msg[4:], p.peerid)
    binary.LittleEndian.PutUint32(msg[8:], dstpid)
    p.SendFromAll(UDPMessage{
	addr: addr,
	msg: msg,
    })
}

func (p *Peer)ProbeToChecker() {
    p.m.Lock()
    checkers := p.checkers
    p.m.Unlock()
    for _, ch := range checkers {
	if addr, err := net.ResolveUDPAddr("udp", ch); err == nil {
	    p.ProbeTo(addr, 0)
	}
    }
}

func (p *Peer)SendPeerInfo() {
    // PeerInfo Message
    // |peer|spid|dpid|peerid|hostname global...|
    p.m.Lock()
    conns := p.conns
    p.m.Unlock()
    for _, dest := range conns {
	for _, conn := range conns {
	    if dest == conn {
		continue
	    }
	    // setup message
	    peerid := conn.remote.peerid
	    addrs := []string{conn.hostname}
	    for _, addr := range conn.remote.remotes {
		addrs = append(addrs, addr.addr)
	    }
	    if len(addrs) == 1 {
		// something wrong
		continue
	    }
	    msg := []byte(fmt.Sprintf("peerSSSSDDDDPPPP%s", strings.Join(addrs, " ")))
	    binary.LittleEndian.PutUint32(msg[12:], peerid)
	    dest.q_sendmsg <- msg
	}
    }
}

func (p *Peer)UnknownStream(c *Connection, code string, data []byte) {
    if c == nil || len(data) < 4 {
	return
    }
    streamid := binary.LittleEndian.Uint32(data[0:4])
    msg := []byte("sendSSSSDDDDXXXXBBBBXXXXXXXXXXXX")
    copy(msg[0:4], []byte(code))
    binary.LittleEndian.PutUint32(msg[12:], streamid)
    binary.LittleEndian.PutUint32(msg[16:], 0xffffffff)
    c.q_sendmsg <- msg
}

func (p *Peer)UDP_handler_Probe(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_probe++
    // TODO
    if spid == dpid {
	p.Infof("self communication")
	// WILL BE IGNORED
    }
    // data may contain global addr string and password
    words := strings.Split(string(data), " ")
    if len(words) > 1 {
	// check password
	password := strings.TrimSpace(words[1])
	if p.password != password {
	    // bad password
	    // ignore
	    p.s_badpass++
	    return
	}
    }
    globaladdr := ""
    if len(words) > 0 {
	globaladdr = strings.TrimSpace(words[0])
    }
    // lookup spid?
    remote := p.FindConnection(spid)
    if remote != nil {
	remote.Update(addr.String())
    }
    if dpid == 0 {
	// recv probe request
	// need to resp multi route
	p.ProbeTo(addr, spid)
	return
    } else if dpid != p.peerid {
	// not to me
	return
    }
    // recv probe response
    s.working = true
    remote.lastRecvProbe = time.Now()
    if globaladdr != "" && s.UpdateGlobal(globaladdr) {
	p.globalChanged = true
    }
}

// Inform Message
// |info|spid|dpid|hostname global...|
func (p *Peer)UDP_handler_Inform(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_inform++
    remote := p.LookupConnectionById(spid)
    if remote == nil {
	// ignore
	return
    }
    remotes := strings.Split(string(data), " ")
    if len(remotes) < 2 {
	// ignore
	return
    }
    hostname := remotes[0]
    // TODO avoid direct access
    remote.hostname = hostname
    remotes = remotes[1:]
    for _, r := range remotes {
	remote.Update(r)
    }
}

// Peer Message
// |peer|spid|dpid|peerid|hostname global...|
func (p *Peer)UDP_handler_Peer(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_peer++
    remote := p.LookupConnectionById(spid)
    if remote == nil {
	// ignore
	return
    }
    peerid := binary.LittleEndian.Uint32(data[0:4])
    data = data[4:]
    var targetpeer *RemotePeer = nil
    p.m.Lock()
    for _, peer := range p.peers {
	if peer.peerid == peerid {
	    targetpeer = peer
	    break
	}
    }
    if targetpeer == nil {
	targetpeer = &RemotePeer{ peerid: peerid }
	p.peers = append(p.peers, targetpeer)
    }
    p.m.Unlock()
    addrs := strings.Split(string(data), " ")
    if len(addrs) <= 1 {
	return
    }
    for _, addr := range addrs[1:] {
	targetpeer.Update(addr)
    }
}

// Open Message
// |open|spid|dpid|stream id|remote addr|
func (p *Peer)UDP_handler_Open(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_open++
    // bad message?
    if len(data) < 4 {
	return
    }
    // create stream and replay
    // Lookup Connection
    c := p.LookupConnectionById(spid)
    if c == nil {
	// ignore
	return
    }
    streamid := binary.LittleEndian.Uint32(data[0:4])
    // Lookup Stream
    st := c.LookupRemoteStream(streamid)
    if st != nil {
	// already open
	return
    }
    raddr := string(data[4:])
    p.Infof("Open from 0x%x stream:0x%x %s", spid, streamid, raddr)
    // New
    st, created := c.NewRemoteStream(streamid)
    if ! created {
	// already created
	// sendback oack
	ack := []byte("oackSSSSDDDDXXXXRRRR")
	binary.LittleEndian.PutUint32(ack[12:], streamid)
	c.q_broadcast <- ack
	return
    }
    // New RemoteServer
    rs, err := NewRemoteServer("laddr", raddr, c, st)
    if err != nil {
	// never happen
	return
    }
    // Start RemoteServer here
    p.m.Lock()
    p.rservs = append(p.rservs, rs)
    p.m.Unlock()
    st.ropen = true
    go rs.Run()
}

// Open Ack/Nack Message
// |oack|spid|dpid|stream id|result|
func (p *Peer)UDP_handler_OpenAck(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_openack++
    // bad message?
    if len(data) < 4 {
	return
    }
    p.Infof("recv oack")
    // Lookup Connection
    c := p.LookupConnectionById(spid)
    if c == nil {
	// ignore
	p.Infof("no connection")
	return
    }
    streamid := binary.LittleEndian.Uint32(data[0:4])
    // Lookup Local Stream
    st := c.LookupLocalStream(streamid)
    if st == nil {
	p.Infof("no local stream 0x%x", streamid)
	return
    }
    if st.ropen {
	// ignore already opened
	return
    }
    // okay, remote was opened
    st.ropen = true
    p.Infof("OpenAck from 0x%x stream:0x%x", spid, streamid)
    Kick(st.q_work)
}

// Remote Send
//  LocalServer read and transfer data to remote stream
// |rsnd|spid|dpid|stream id|blockdata...|
func (p *Peer)UDP_handler_RemoteSend(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    //streamid := binary.LittleEndian.Uint32(data[0:4])
    //blkid := binary.LittleEndian.Uint32(data[4:8])
    //p.Infof("recv rsnd streamid:0x%x blkid:%d", streamid, blkid)
    p.s_rsnd++
    c, st := p.LookupConnectionAndRemoteStream(spid, data)
    if st == nil {
	if c != nil {
	    p.UnknownStream(c, "rrcv", data)
	}
	return
    }
    st.GetBlock(data[4:])
}

// Remote Recv
//  LocalServer read and transfer data to remote stream
// |rrcv|spid|dpid|stream id|blockdata...|
func (p *Peer)UDP_handler_RemoteRecv(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    //streamid := binary.LittleEndian.Uint32(data[0:4])
    //blkid := binary.LittleEndian.Uint32(data[4:8])
    //p.Infof("recv rrcv streamid:0x%x blkid:%d", streamid, blkid)
    p.s_rrcv++
    c, st := p.LookupConnectionAndLocalStream(spid, data)
    if st == nil {
	if c != nil {
	    p.UnknownStream(c, "rsnd", data)
	}
	return
    }
    st.GetBlock(data[4:])
}

// Remote Send Ack
//  LocalServer read and transfer data to remote stream
// |rsck|spid|dpid|stream id|blkid|ack|
func (p *Peer)UDP_handler_RemoteSendAck(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    blkid := binary.LittleEndian.Uint32(data[4:])
    ack := binary.LittleEndian.Uint32(data[8:])
    //p.Infof("recv rsck streamid:0x%x %d 0x%x", st.streamid, blkid, ack)
    p.s_rsck++
    c, st := p.LookupConnectionAndLocalStream(spid, data)
    if st == nil {
	if c != nil {
	    p.UnknownStream(c, "rsnd", data)
	}
	return
    }
    st.GetAck(blkid, ack)
    // update Remote
    c.Update(addr.String())
}

// Remote Recv Ack
//  LocalServer read and transfer data to remote stream
// |rrck|spid|dpid|stream id|blockid|ack|
func (p *Peer)UDP_handler_RemoteRecvAck(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    blkid := binary.LittleEndian.Uint32(data[4:])
    ack := binary.LittleEndian.Uint32(data[8:])
    //p.Infof("recv rrck streamid:0x%x %d 0x%x", st.streamid, blkid, ack)
    p.s_rrck++
    c, st := p.LookupConnectionAndRemoteStream(spid, data)
    if st == nil {
	if c != nil {
	    p.UnknownStream(c, "rrcv", data)
	}
	return
    }
    st.GetAck(blkid, ack)
    // update Remote
    c.Update(addr.String())
}

func (p *Peer)UDP_handler(s *LocalSocket, addr *net.UDPAddr, msg []byte) {
    p.s_udp++
    //logrus.Infof("recv %d bytes from %v on %v", len(msg), addr, s.sock.LocalAddr())
    if msg[0] == 'P' {
	// Probe? reuse v1 protocol
	if string(msg) == "Probe" {
	    msg := []byte(fmt.Sprintf("Probe %v", addr))
	    s.sock.WriteToUDP(msg, addr)
	    s.sock.WriteToUDP(msg, addr)
	    s.sock.WriteToUDP(msg, addr)
	} else if len(msg) > 7 {
	    w := strings.Split(string(msg), " ")
	    // Probe addr
	    if s.UpdateGlobal(w[1]) {
		p.globalChanged = true
	    }
	}
	return
    }
    // UDP Packet Format
    // parse
    // |code|src peer id|dst peer id|
    if len(msg) < 16 {
	// ignore
	p.s_ignore++
	return
    }
    code := msg[0:4]
    spid := binary.LittleEndian.Uint32(msg[4:8])
    dpid := binary.LittleEndian.Uint32(msg[8:12])
    data := msg[12:]
    switch string(code) {
    case "prob": p.UDP_handler_Probe(s, addr, spid, dpid, data)
    case "info": p.UDP_handler_Inform(s, addr, spid, dpid, data)
    case "peer": p.UDP_handler_Peer(s, addr, spid, dpid, data)
    case "open": p.UDP_handler_Open(s, addr, spid, dpid, data)
    case "oack": p.UDP_handler_OpenAck(s, addr, spid, dpid, data)
    case "rsnd": p.UDP_handler_RemoteSend(s, addr, spid, dpid, data)
    case "rrcv": p.UDP_handler_RemoteRecv(s, addr, spid, dpid, data)
    case "rsck": p.UDP_handler_RemoteSendAck(s, addr, spid, dpid, data)
    case "rrck": p.UDP_handler_RemoteRecvAck(s, addr, spid, dpid, data)
    default:
	p.Infof("msg code [%s] 0x%x 0x%x", code, spid, dpid)
    }
}

func (p *Peer)API_handler_CONFIG(conn net.Conn, words []string) {
    if len(words) == 0{
	return
    }
    target := words[0]
    ops := words[1:]
    switch target {
    case "HOUSEKEEPER":
	if ops[0] == "short" {
	    p.d_housekeep = time.Second
	}
	if ops[0] == "long" {
	    p.d_housekeep = 5 * time.Second
	}
    case "PASSWORD":
	password := ""
	if len(ops) > 0 {
	    password = ops[0]
	}
	p.password = password
    }
}

func (p *Peer)API_handler(conn net.Conn) {
    defer conn.Close()
    buf := make([]byte, 256)
    n, err := conn.Read(buf)
    if n <= 0 {
	p.Infof("API: Read: %v", err)
	return
    }
    firstline := strings.Split(string(buf[:n]), "\n")[0]
    req := strings.TrimSpace(firstline)
    words := strings.Fields(req)
    p.Infof("API: %v", words)
    switch words[0] {
    case "INFO":
	resp := ""
	resp += fmt.Sprintf("%s 0x%x\n", p.hostname, p.peerid)
	// sockets
	resp += "sockets:\n"
	p.m.Lock()
	for _, sock := range p.lsocks {
	    resp += fmt.Sprintf("%s\n", sock.String())
	}
	p.m.Unlock()
	// connections
	resp += "connections:\n"
	p.m.Lock()
	for _, c := range p.conns {
	    resp += fmt.Sprintf("%s\n", c.String())
	}
	p.m.Unlock()
	// peers
	resp += "peers:\n"
	p.m.Lock()
	for _, peer := range p.peers {
	    resp += fmt.Sprintf("%s\n", peer.String())
	}
	p.m.Unlock()
	// local servers
	resp += "local:\n"
	p.m.Lock()
	for _, serv := range p.lservs {
	    resp += fmt.Sprintf("%s\n", serv.String())
	}
	p.m.Unlock()
	// local servers
	resp += "remote:\n"
	p.m.Lock()
	for _, serv := range p.rservs {
	    resp += fmt.Sprintf("%s\n", serv.String())
	}
	p.m.Unlock()
	// stats
	s_recv := fmt.Sprintf("[recv %d udp %d ignore %d %d %d %d %d %d %d %d %d]",
	    p.s_udp, p.s_ignore,
	    p.s_probe, p.s_inform, p.s_peer, p.s_open, p.s_openack,
	    p.s_rsnd, p.s_rrcv, p.s_rsck, p.s_rrck)
	s_hk := fmt.Sprintf("[housekeep %d]", p.s_housekeep)
	s_misc := fmt.Sprintf("[misc %d]", p.s_badpass)
	resp += fmt.Sprintf("stats %s %s %s\n", s_recv, s_hk, s_misc)
	conn.Write([]byte(resp))
    case "SHOW":
	// SHOW <connection>
	if len(words) == 1 {
	    return
	}
	c := p.LookupConnection(words[1])
	if c == nil {
	    return
	}
	head := c.String()
	lss := []string{}
	rss := []string{}
	c.m.Lock()
	for _, s := range c.lstreams {
	    lss = append(lss, s.Stat())
	}
	for _, s := range c.rstreams {
	    rss = append(rss, s.Stat())
	}
	c.m.Unlock()
	ls := ""
	if len(lss) > 0 {
	    ls = fmt.Sprintf("local streams:\n%s\n", strings.Join(lss, "\n"))
	}
	rs := ""
	if len(rss) > 0 {
	    rs = fmt.Sprintf("remote streams:\n%s\n", strings.Join(rss, "\n"))
	}
	resp := fmt.Sprintf("%s\n%s%s", head, ls, rs)
	conn.Write([]byte(resp))
    case "CONNECT":
	// need target
	if len(words) == 1 {
	    return
	}
	addr, err := net.ResolveUDPAddr("udp", words[1])
	if err != nil {
	    p.Infof("ResolveUDPAddr: %v", err)
	    return
	}
	// probe target to connect
	p.ProbeTo(addr, 0)
    case "ADD":
	// need local and remote
	if len(words) < 3 {
	    conn.Write([]byte("Bad Request"))
	    return
	}
	laddr := words[1]
	// lookup remote first name:addr:port
	r := strings.SplitN(words[2], ":", 2)
	rname := r[0]
	raddr := r[1]
	remote := p.LookupConnection(rname)
	if remote == nil {
	    p.Infof("unknown remote %s", rname)
	    resp := fmt.Sprintf("Unknown: %s", rname)
	    conn.Write([]byte(resp))
	    return
	}
	ls, err := NewLocalServer(laddr, raddr, remote)
	if err != nil {
	    p.Infof("NewLocalServer: %v", err)
	    resp := fmt.Sprintf("Error: %v", err)
	    conn.Write([]byte(resp))
	    return
	}
	// start LocalServer here
	go ls.Run()
	p.m.Lock()
	p.lservs = append(p.lservs, ls)
	p.m.Unlock()
    case "DEL":
	// need local
	if len(words) < 2 {
	    conn.Write([]byte("Bad Request"))
	    return
	}
	laddr := words[1]
	var target *LocalServer = nil
	p.m.Lock()
	for _, ls := range p.lservs {
	    if ls.laddr == laddr {
		target = ls
		break
	    }
	}
	p.m.Unlock()
	if target != nil {
	    target.Stop()
	}
    case "CHECKER":
	if len(words) == 1 {
	    return
	}
	// new checker?
	hit := false
	p.m.Lock()
	for _, c := range p.checkers {
	    if c == words[1] {
		hit = true
		break
	    }
	}
	if ! hit {
	    p.Infof("new checker: %s", words[1])
	    p.checkers = append(p.checkers, words[1])
	}
	p.m.Unlock()
	if ! hit {
	    p.ProbeToChecker()
	}
    case "CONFIG":
	p.API_handler_CONFIG(conn, words[1:])
    }
}

func (p *Peer)ShowSocketsStats() {
    p.m.Lock()
    socks := p.lsocks
    p.m.Unlock()
    for _, sock := range socks {
	p.Infof("%s", sock.String())
    }
}

func (p *Peer)Housekeeper_Sockets() {
    p.m.Lock()
    for _, sock := range p.lsocks {
	if sock.running == false && sock.dead == false {
	    // start local socket
	    go sock.Run(p.UDP_handler)
	}
    }
    p.m.Unlock()
    // sweep
    p.m.Lock()
    socks := []*LocalSocket{}
    for _, sock := range p.lsocks {
	if sock.dead == false {
	    socks = append(socks, sock)
	}
    }
    p.lsocks = socks
    p.m.Unlock()
    // check retired socket
    p.m.Lock()
    for _, sock := range p.lsocks {
	if sock.retired {
	    sock.Stop()
	}
    }
    p.m.Unlock()
    // retire socket
    p.m.Lock()
    for _, sock := range p.lsocks {
	if sock.running && sock.retiring {
	    sock.Retire()
	}
    }
    p.m.Unlock()
    if time.Since(p.sockRetireTime) < p.d_retire {
	return
    }
    p.sockRetireTime = time.Now()
    p.m.Lock()
    // prepare 4th socket
    if len(p.lsocks) < 4 {
	p.m.Unlock()
	sock := NewLocalSocket()
	p.m.Lock()
	if sock != nil {
	    p.lsocks = append(p.lsocks, sock)
	}
    }
    // retire oldest socket
    var sock *LocalSocket = p.lsocks[0]
    for _, s := range p.lsocks {
	if s.created.Before(sock.created) {
	    sock = s
	}
    }
    sock.Retire()
    p.m.Unlock()
    // Force to check
    p.ProbeToChecker()
    p.lastCheck = time.Now()
}

func (p *Peer)Housekeeper_Connection(c *Connection) {
    c.CheckRemotePeer()
    if time.Since(c.lastRecvProbe) > 30 * time.Second {
	c.Infof("missing probe response in %v", time.Since(c.lastRecvProbe))
	// show socket stats here
	p.ShowSocketsStats()
    }
    now := time.Now()
    if time.Since(c.updateTime) > 5 * time.Minute {
	c.Stop()
	// TODO remove it
	return
    }
    if p.globalChanged {
	for _, r := range c.remote.remotes {
	    if addr, err := net.ResolveUDPAddr("udp", r.addr); err == nil {
		p.InformTo(addr, p.peerid)
		p.ProbeTo(addr, 0)
	    }
	}
    }
    remotes := c.Freshers()
    if now.After(c.lastProbe.Add(time.Second * 10)) {
	for _, r := range remotes {
	    if addr, err := net.ResolveUDPAddr("udp", r.addr); err == nil {
		p.ProbeTo(addr, 0)
	    }
	}
	c.lastProbe = now
    }
    if now.After(c.lastInform.Add(time.Minute)) {
	for _, r := range remotes {
	    if addr, err := net.ResolveUDPAddr("udp", r.addr); err == nil {
		p.InformTo(addr, p.peerid)
	    }
	}
	c.lastInform = now
    }
    // finally show connection stats
    if time.Since(c.lastShow) > 60 * time.Minute {
	c.Infof("show %s", c.String())
	c.lastShow = time.Now()
    }
}

func (p *Peer)Housekeeper_Connections() {
    // probe/inform connections
    p.m.Lock()
    conns := p.conns
    p.m.Unlock()
    for _, c := range conns {
	p.Housekeeper_Connection(c)
    }
}

func (p *Peer)Housekeeper_Kick() {
    p.m.Lock()
    for _, serv := range p.lservs {
	serv.remote.KickStreams()
    }
    for _, serv := range p.rservs {
	serv.remote.KickStreams()
    }
    p.m.Unlock()
}

func (p *Peer)Housekeeper_Sweeper() {
    // sweep streams
    p.m.Lock()
    for _, serv := range p.lservs {
	serv.remote.SweepStreams()
    }
    for _, serv := range p.rservs {
	serv.remote.SweepStreams()
    }
    p.m.Unlock()
    // sweep local servers
    p.m.Lock()
    lservs := []*LocalServer{}
    for _, serv := range p.lservs {
	if serv.running || serv.streams > 0 {
	    lservs = append(lservs, serv)
	}
    }
    p.lservs = lservs
    p.m.Unlock()
    // sweep remote servers
    p.m.Lock()
    rservs := []*RemoteServer{}
    for _, serv := range p.rservs {
	if serv.running || time.Since(serv.lastUpdate) < time.Minute {
	    rservs = append(rservs, serv)
	}
    }
    p.rservs = rservs
    p.m.Unlock()
    // sweep connection
    p.m.Lock()
    conns := []*Connection{}
    for _, c := range p.conns {
	if c.running || time.Since(c.stopTime) < time.Second {
	    conns = append(conns, c)
	}
    }
    p.conns = conns
    p.m.Unlock()
}

func (p *Peer)Housekeeper() {
    for p.running {
	// check sockets
	p.Housekeeper_Sockets()
	if time.Since(p.lastShow) > 60 * time.Minute {
	    p.ShowSocketsStats()
	    p.lastShow = time.Now()
	}
	// check connections
	p.Housekeeper_Connections()
	// checker?
	if p.globalChanged || time.Since(p.lastCheck) > time.Minute {
	    p.ProbeToChecker()
	    p.lastCheck = time.Now()
	    p.globalChanged = false
	}
	// peer info exchange
	p.SendPeerInfo()
	// kick streams
	p.Housekeeper_Kick()
	// sweeper
	p.Housekeeper_Sweeper()
	p.s_housekeep++
	time.Sleep(p.d_housekeep)
    }
}

func (p *Peer)Run() {
    p.running = true
    p.Infof("peer running")
    go p.serv.Run()
    go p.Housekeeper()
    idx := 0
    for p.running {
	// handle q_sendmsg here
	msg := <-p.q_sendmsg
	// okay send it
	if msg.addr != nil && len(msg.msg) > 12 {
	    // replace src peerid
	    binary.LittleEndian.PutUint32(msg.msg[4:], p.peerid)
	    p.m.Lock()
	    var sock *LocalSocket = nil
	    l := len(p.lsocks)
	    for i := 0; i < l; i++ {
		s := p.lsocks[idx % l]
		idx++
		if idx >= l {
		    idx = 0
		}
		if ! s.IsReady() {
		    continue
		}
		sock = s
		break
	    }
	    p.m.Unlock()
	    if sock == nil {
		p.Infof("no sockets are available")
		continue
	    }
	    sock.q_sendmsg <- msg
	}
    }
    time.Sleep(time.Second)
    p.serv.Stop()
    // stop remote servers
    for _, serv := range p.rservs {
	serv.Stop()
    }
    // stop local servers
    for _, serv := range p.lservs {
	serv.Stop()
    }
    // stop connections
    for _, conn := range p.conns {
	conn.Stop()
    }
    // stop local sockets
    for _, sock := range p.lsocks {
	sock.Stop()
    }
    p.Infof("peer stopped")
}

func peer(laddr string) {
    logrus.Infof("start peer")
    if laddr == "" {
	logrus.Infof("no local addr")
	return
    }
    p, err := NewPeer(laddr)
    if err != nil {
	logrus.Infof("NewPeer: %v", err)
	return
    }
    p.Run()
    logrus.Infof("end peer")
}
