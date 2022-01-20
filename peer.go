// MIT License Copyright(c) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "encoding/binary"
    "fmt"
    "io"
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
    retired bool
    dead bool
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
    s.q_sendmsg = make(chan UDPMessage, 64)
    return s
}

func (s *LocalSocket)UpdateGlobal(global string) {
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
	logrus.Infof("update global %s to %s", old, global)
    }
}

func (s *LocalSocket)String() string {
    return fmt.Sprintf("localsocket %v %s %d %d %d %d",
	    s.sock.LocalAddr(), s.global,
	    s.s_send, s.s_senderr, s.s_recv, s.s_recverr)
}

func (s *LocalSocket)Sender() {
    for s.running {
	msg := <-s.q_sendmsg
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
}

func (s *LocalSocket)Run(cb func(*LocalSocket, *net.UDPAddr, []byte)) {
    defer func() { s.dead = true } ()
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
	    logrus.Infof("ReadFromUDP: %v", err)
	    continue
	}
	s.s_recv++
	//msg := make([]byte, n)
	//copy(msg, buf[:n])
	msg := buf[:n]
	if msg[0] == 'P' {
	    // v1 "Probe" ?
	    if string(msg[0:6]) == "Probe " {
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
		logrus.Infof("handler takes too long %v %s", d, msg[0:4])
	    }
	}
    }
    // stop sender
    s.q_sendmsg <- UDPMessage{}
}

func (s *LocalSocket)Stop() {
    s.running = false
    s.retired = true
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
    s_oldblkid, s_badblkid, s_baddata, s_dup uint32
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
    oblkack uint32
    oblkAcked time.Time
    // incoming block
    iblk *DataBlock
    writer io.Writer
    // mutex
    m sync.Mutex
    //
    q_work chan bool
    q_acked chan bool
    //
    sweep bool
    // stats
    s_sendmsg, s_sendack, s_recvack uint32
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
    return st
}

func (st *Stream)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("stream:0x%x ", st.streamid)
    logrus.Infof(header + f, args...)
}

func (st *Stream)SetWriter(w io.Writer) {
    st.writer = w
}

func (st *Stream)FlushInblock() {
    //st.Infof("Flush %d bytes", st.iblk.sz)
    st.writer.Write(st.iblk.data[:st.iblk.sz])
}

func (st *Stream)SendBlock(code string, q chan []byte) {
    st.m.Lock()
    oblk := st.oblk
    acked := st.oblkack
    st.m.Unlock()
    if oblk == nil {
	return
    }
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
    }
}

func (st *Stream)GetBlock(data []byte) {
    // blockdata
    // |blkid|nr parts|part id|part len|data...|
    if len(data) < 16 {
	return
    }
    blkid := binary.LittleEndian.Uint32(data[0:4])
    if blkid == 0xffffffff {
	logrus.Infof("remote closed")
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
	st.q_work <- true
    }
}

func (st *Stream)GetAck(blkid, ack uint32) {
    wakeup := false
    st.m.Lock()
    if st.oblk.blkid == blkid {
	st.oblkack |= ack
	st.oblkAcked = time.Now()
	wakeup = true
	st.s_recvack++
    } else if st.oblk.blkid + 1== blkid {
	if ack == 0 {
	    st.Infof("missing prev ack")
	    st.oblkack = 0xffffffff
	    st.oblkAcked = time.Now()
	    wakeup = true
	    st.s_recvack++
	}
    }
    st.m.Unlock()
    if wakeup {
	st.q_work <- true
    }
}

func (st *Stream)CheckOutblockAck() {
    acked := false
    st.m.Lock()
    if st.oblkack == 0xffffffff {
	st.oblk.NextBlock()
	st.oblkack = 0
	acked = true
    }
    st.m.Unlock()
    if acked {
	st.q_acked <- true
    }
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
	    rest := len(curr.data) - curr.idx
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
	    m.Lock()
	    reading = false
	    if n > 0 {
		b.idx += n
		m.Unlock()
		if len(q_read) == 0 {
		    q_read <- true
		}
		continue
	    }
	    if e, ok := err.(net.Error); ok && e.Timeout() {
		m.Unlock()
		continue
	    }
	    m.Unlock()
	    st.Infof("Read: %v", err)
	    closed = true
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
		st.m.Lock()
		st.oblk.SetupMessages(curr.data[drain:curr.idx])
		st.oblkAcked = time.Now()
		st.m.Unlock()
		st.q_work <- true
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
		    wakeup = true
		}
	    }
	}
	m.Unlock()
	if wakeup {
	    if len(q_wait) == 0 {
		q_wait <- true
	    }
	}
	if closed {
	    st.m.Lock()
	    rest := st.oblk.rest
	    st.m.Unlock()
	    // check data to send
	    if curr.idx == 0 && rest == 0 {
		// no data
		st.Infof("closed and no data")
		st.oblk.MarkClose()
		st.m.Lock()
		st.lopen = false
		st.m.Unlock()
		st.q_work <- true
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

func (st *Stream)Run(code, ackcode string, q_sendmsg chan []byte, conn net.Conn) {
    ackmsg := []byte("rackSSSSDDDDXXXXBBBBAAAA")
    copy(ackmsg[0:4], []byte(ackcode))
    binary.LittleEndian.PutUint32(ackmsg[12:], st.streamid)
    lastAck := time.Now()
    st.oblkAcked = time.Now()
    st.running = true
    // start SelfReader
    go st.SelfReader(conn)
    for st.running {
	// no acks ?
	if time.Since(st.oblkAcked) > time.Minute {
	    // TODO: check in case no send from myside...
	    st.Infof("no acks from remote")
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
	st.m.Lock()
	blkid := st.iblk.blkid
	ack := st.iblk.rest
	if ack == 0 && blkid > 0 {
	    blkid--
	    ack = 0xffffffff
	}
	if ack == 0xffffffff || time.Since(lastAck) > time.Millisecond * 5 {
	    binary.LittleEndian.PutUint32(ackmsg[16:], blkid)
	    binary.LittleEndian.PutUint32(ackmsg[20:], ack)
	    sendack = true
	}
	if st.iblk.rest == 0xffffffff {
	    st.FlushInblock()
	    st.iblk.NextBlock()
	}
	st.m.Unlock()
	if sendack {
	    st.s_sendack++
	    q_sendmsg <- ackmsg
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
    if len(st.q_work) == 0 {
	st.q_work <- true
    }
    if len(st.q_acked) == 0 {
	st.q_acked <- true
    }
}

func (st *Stream)Destroy() {
    if st.sweep == false {
	st.Infof("not sweeped")
	return
    }
    // close queue
    close(st.q_work)
    close(st.q_acked)
    // show stats
    st.Infof("total [send %d msgs %d acks] [recv %d acks]", st.s_sendmsg, st.s_sendack, st.s_recvack)
    st.Infof("oblk errors %d %d %d %d",
	st.oblk.s_oldblkid, st.oblk.s_badblkid,
	st.oblk.s_baddata, st.oblk.s_dup)
}

type Connection struct {
    remotes []string
    peerid uint32
    hostname string
    lstreams []*Stream
    rstreams []*Stream
    streamid uint32
    startTime time.Time
    lastProbe time.Time
    lastInform time.Time
    sockidx int
    m sync.Mutex
    q_sendmsg chan []byte
    running bool
    //
    updateTime time.Time
    // stats
    s_sendmsg uint32
    s_sendbytes uint64
    s_lookuplocalstream, s_lookupremotestream uint32
}

func NewConnection(peerid uint32) *Connection {
    c := &Connection{
	remotes: []string{},
	peerid: peerid,
	startTime: time.Now(),
	updateTime: time.Now(),
	q_sendmsg: make(chan []byte, 64),
    }
    return c
}

func (c *Connection)String() string {
    c.m.Lock()
    defer c.m.Unlock()
    stats := fmt.Sprintf("[send %d msgs %d bytes] [recv %d locals %d remotes]",
	    c.s_sendmsg, c.s_sendbytes,
	    c.s_lookuplocalstream, c.s_lookupremotestream)
    return fmt.Sprintf("0x%x %s [%v] local:%d remote:%d %v %s",
	    c.peerid, c.hostname, time.Since(c.startTime),
	    len(c.lstreams), len(c.rstreams),
	    c.remotes, stats)
}

func (c *Connection)Infof(f string, args ...interface{}) {
    header := fmt.Sprintf("connection:0x%x ", c.peerid)
    logrus.Infof(header + f, args...)
}

func (c *Connection)Update(addr string) {
    c.m.Lock()
    defer c.m.Unlock()
    remotes := []string{}
    for _, a := range c.remotes {
	if a != addr {
	    remotes = append(remotes, a)
	}
    }
    c.remotes = append(remotes, addr)
    c.updateTime = time.Now()
}

func (c *Connection)Freshers() []string {
    c.m.Lock()
    defer c.m.Unlock()
    l := len(c.remotes)
    if l < 3 {
	return c.remotes
    }
    return c.remotes[l-3:]
}

func (c *Connection)NewLocalStream() *Stream {
    c.m.Lock()
    streamid := c.streamid
    c.streamid++
    c.m.Unlock()
    c.m.Lock()
    st := NewStream(streamid)
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

func (c *Connection)NewRemoteStream(rid uint32) *Stream {
    c.m.Lock()
    st := NewStream(rid)
    c.rstreams = append(c.rstreams, st)
    c.m.Unlock()
    return st
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
    c.Infof("KickStreams")
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
    c.Infof("SweepStreams")
    sweeped := []*Stream{}
    streams := []*Stream{}
    for _, st := range c.lstreams {
	del := false
	st.m.Lock()
	if st.sweep {
	    del = true
	} else if st.running == false {
	    st.sweep = true
	} else if st.ropen == false && st.lopen == false {
	    st.Stop()
	}
	st.m.Unlock()
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
	st.m.Lock()
	if st.sweep {
	    del = true
	} else if st.running == false {
	    st.sweep = true
	} else if st.ropen == false && st.lopen == false {
	    st.Stop()
	}
	st.m.Unlock()
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

func (c *Connection)Run(q chan UDPMessage) {
    c.running = true
    for c.running {
	sendmsg := <-c.q_sendmsg
	if len(sendmsg) > 12 {
	    c.s_sendmsg++
	    c.s_sendbytes += uint64(len(sendmsg))
	    r := c.Freshers()[0]
	    addr, err := net.ResolveUDPAddr("udp", r)
	    if err != nil {
		continue
	    }
	    // update msg
	    // replace dest peerid
	    binary.LittleEndian.PutUint32(sendmsg[8:], c.peerid)
	    q <- UDPMessage{ msg:sendmsg, addr: addr }
	}
    }
}

func (c *Connection)Stop() {
    c.running = false
    c.q_sendmsg <- []byte{}
    c.Infof("stopped")
}

type LocalServer struct {
    remote *Connection
    laddr, raddr string
    serv *session.Server
    running bool
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
	peerid = ls.remote.peerid
    }
    stats := fmt.Sprintf("%d", ls.s_accept)
    return fmt.Sprintf("localserver %s %s 0x%x %s", ls.laddr, ls.raddr, peerid, stats)
}

func (ls *LocalServer)Handle_Session(lconn net.Conn) {
    defer lconn.Close()
    if ls.remote == nil {
	return
    }
    ls.s_accept++
    // prepare stream
    st := ls.remote.NewLocalStream()
    st.SetWriter(lconn)
    st.lopen = true
    // prepare message
    msg := []byte("openSSSSDDDDXXXX" + ls.raddr)
    binary.LittleEndian.PutUint32(msg[12:], st.streamid)
    // try to send
    ls.remote.q_sendmsg <- msg
    // wait a 1sec right now
    <-st.q_work
    if st.ropen == false {
	st.lopen = false
	logrus.Infof("stream not opened")
	return
    }

    // forground runner
    st.Run("rsnd", "rrck", ls.remote.q_sendmsg, lconn)
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
	peerid = rs.remote.peerid
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
    rs.remote.q_sendmsg <- ack

    st := rs.stream
    st.SetWriter(conn)

    // forground runner
    st.Run("rrcv", "rsck", rs.remote.q_sendmsg, conn)

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
    lservs []*LocalServer
    rservs []*RemoteServer
    serv *session.Server
    peerid uint32
    hostname string
    running bool
    lastCheck time.Time
    q_sendmsg chan UDPMessage
    m sync.Mutex
    // stats
    s_udp uint32
    s_ignore uint32
    s_probe uint32
    s_inform uint32
    s_open, s_openack uint32
    s_rsnd, s_rrcv, s_rsck, s_rrck uint32
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
    p.q_sendmsg = make(chan UDPMessage, 64)
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
	if c.peerid == peerid {
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
	if c.peerid == peerid {
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

func (p *Peer)ProbeTo(addr *net.UDPAddr, dstpid uint32) {
    msg := []byte(fmt.Sprintf("probSSSSDDDD%v", addr))
    binary.LittleEndian.PutUint32(msg[4:], p.peerid)
    binary.LittleEndian.PutUint32(msg[8:], dstpid)
    p.m.Lock()
    defer p.m.Unlock()
    for _, sock := range p.lsocks {
	udpmsg := UDPMessage {
	    addr: addr,
	    msg: msg,
	}
	sock.q_sendmsg <- udpmsg
    }
}

func (p *Peer)InformTo(addr *net.UDPAddr, dstpid uint32) {
    // Inform Message
    // |info|spid|dpid|hostname global...|
    addrs := []string{p.hostname}
    p.m.Lock()
    for _, sock := range p.lsocks {
	if sock.global != "" {
	    addrs = append(addrs, sock.global)
	}
    }
    p.m.Unlock()
    if len(addrs) == 0 {
	return
    }
    msg := []byte(fmt.Sprintf("infoSSSSDDDD%s", strings.Join(addrs, " ")))
    binary.LittleEndian.PutUint32(msg[4:], p.peerid)
    binary.LittleEndian.PutUint32(msg[8:], dstpid)
    p.m.Lock()
    defer p.m.Unlock()
    for _, sock := range p.lsocks {
	udpmsg := UDPMessage {
	    addr: addr,
	    msg: msg,
	}
	sock.q_sendmsg <- udpmsg
    }
}

func (p *Peer)UDP_handler_Probe(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_probe++
    // TODO
    if spid == dpid {
	p.Infof("self communication")
	// WILL BE IGNORED
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
    // data may contain global addr string
    hostname := strings.TrimSpace(strings.Split(string(data), "\n")[0])
    s.UpdateGlobal(hostname)
}

// Inform Message
// |info|spid|dpid|hostname global...|
func (p *Peer)UDP_handler_Inform(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_inform++
    remote := p.FindConnection(spid)
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
    st = c.NewRemoteStream(streamid)
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
    // okay, remote was opened
    st.ropen = true
    p.Infof("OpenAck from 0x%x stream:0x%x", spid, streamid)
    st.q_work <- true
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
    if c == nil || st == nil {
	p.Infof("unknown stream")
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
    if c == nil || st == nil {
	p.Infof("unknown stream")
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
    if c == nil || st == nil {
	p.Infof("unknown stream")
	return
    }
    st.GetAck(blkid, ack)
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
    if c == nil || st == nil {
	p.Infof("unknown stream")
	return
    }
    st.GetAck(blkid, ack)
}

func (p *Peer)UDP_handler(s *LocalSocket, addr *net.UDPAddr, msg []byte) {
    p.s_udp++
    //logrus.Infof("recv %d bytes from %v on %v", len(msg), addr, s.sock.LocalAddr())
    if msg[0] == 'P' {
	// Probe? reuse v1 protocol
	if len(msg) > 7 {
	    w := strings.Split(string(msg), " ")
	    // Probe addr
	    s.UpdateGlobal(w[1])
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
	resp += fmt.Sprintf("stats %d udp %d ignore\n", p.s_udp, p.s_ignore)
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
    }
}

func (p *Peer)Housekeeper_Connection(c *Connection) {
    remotes := c.Freshers()
    now := time.Now()
    if time.Since(c.updateTime) > 5 * time.Minute {
	c.Stop()
	// TODO remove it
	return
    }
    if now.After(c.lastProbe.Add(time.Second * 10)) {
	for _, r := range remotes {
	    if addr, err := net.ResolveUDPAddr("udp", r); err == nil {
		p.ProbeTo(addr, 0)
	    }
	}
	c.lastProbe = now
    }
    if now.After(c.lastInform.Add(time.Minute)) {
	for _, r := range remotes {
	    if addr, err := net.ResolveUDPAddr("udp", r); err == nil {
		p.InformTo(addr, p.peerid)
	    }
	}
	c.lastInform = now
    }
}

func (p *Peer)Housekeeper() {
    for p.running {
	// check sockets
	p.m.Lock()
	for _, sock := range p.lsocks {
	    if sock.running == false && sock.dead == false {
		// start local socket
		go sock.Run(p.UDP_handler)
	    }
	}
	p.m.Unlock()
	// probe/inform connections
	p.m.Lock()
	conns := p.conns
	p.m.Unlock()
	for _, c := range conns {
	    p.Housekeeper_Connection(c)
	}
	// checker?
	if time.Now().After(p.lastCheck.Add(time.Minute)) {
	    p.m.Lock()
	    checkers := p.checkers
	    p.m.Unlock()
	    for _, ch := range checkers {
		if addr, err := net.ResolveUDPAddr("udp", ch); err == nil {
		    p.ProbeTo(addr, 0)
		}
		p.lastCheck = time.Now()
	    }
	}
	// kick streams
	p.m.Lock()
	for _, serv := range p.lservs {
	    serv.remote.KickStreams()
	}
	for _, serv := range p.rservs {
	    serv.remote.KickStreams()
	}
	p.m.Unlock()
	// sweep streams
	p.m.Lock()
	for _, serv := range p.lservs {
	    serv.remote.SweepStreams()
	}
	for _, serv := range p.rservs {
	    serv.remote.SweepStreams()
	}
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
	time.Sleep(time.Second * 5)
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
	    if idx >= len(p.lsocks) {
		idx = 0
	    }
	    sock := p.lsocks[idx]
	    p.m.Unlock()
	    idx++
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
