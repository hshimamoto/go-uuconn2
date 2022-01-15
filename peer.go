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
	    cb(s, addr, msg)
	}
    }
    // stop sender
    s.q_sendmsg <- UDPMessage{}
}

func (s *LocalSocket)Stop() {
    s.running = false
    s.retired = true
}

type Stream struct {
    streamid uint32
}

func NewStream(sid uint32) *Stream {
    st := &Stream{ streamid: sid }
    return st
}

type Connection struct {
    remotes []string
    peerid uint32
    hostname string
    startTime time.Time
    lastProbe time.Time
    lastInform time.Time
    sockidx int
    m sync.Mutex
    q_sendmsg chan []byte
    running bool
}

func NewConnection(peerid uint32) *Connection {
    c := &Connection{
	remotes: []string{},
	peerid: peerid,
	startTime: time.Now(),
	q_sendmsg: make(chan []byte, 64),
    }
    return c
}

func (c *Connection)String() string {
    c.m.Lock()
    defer c.m.Unlock()
    return fmt.Sprintf("0x%x %s [%v] %v", c.peerid, c.hostname, time.Since(c.startTime), c.remotes)
}

func (c *Connection)Update(addr string) {
    c.m.Lock()
    defer c.m.Unlock()
    l := len(c.remotes)
    if l < 1 || c.remotes[l-1] != addr {
	c.remotes = append(c.remotes, addr)
    }
    // shrink
    if l > 30 {
	c.remotes = c.remotes[l-10:l]
    }
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

func (c *Connection)Run(q chan UDPMessage) {
    c.running = true
    for c.running {
	sendmsg := <-c.q_sendmsg
	if len(sendmsg) > 12 {
	    logrus.Infof("sendmsg %d bytes", len(sendmsg))
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
}

type LocalServer struct {
    remote *Connection
    laddr, raddr string
    serv *session.Server
    streamid uint32
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
    if ls.remote == nil {
	return
    }
    ls.s_accept++
    // prepare stream
    st := NewStream(ls.streamid)
    ls.streamid++
    // prepare message
    msg := []byte("openSSSSDDDDXXXXXXXX")
    binary.LittleEndian.PutUint32(msg[12:], st.streamid)
    // try to send
    ls.remote.q_sendmsg <- msg
}

func (ls *LocalServer)Run() {
    ls.running = true
    ls.serv.Run()
}

func (ls *LocalServer)Stop() {
    ls.running = false
    ls.serv.Stop()
}

type Peer struct {
    lsocks []*LocalSocket
    checkers []string
    conns []*Connection
    lservs []*LocalServer
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

func (p *Peer)LookupConnection(name string) *Connection {
    p.m.Lock()
    defer p.m.Unlock()
    for _, c := range p.conns {
	if c.hostname == name {
	    return c
	}
    }
    // name is peerid
    if name[0:2] == "0x" {
	name = name[2:]
    }
    peerid64, err := strconv.ParseUint(name, 16, 32)
    if err != nil {
	return nil
    }
    peerid := uint32(peerid64)
    for _, c := range p.conns {
	if c.peerid == peerid {
	    return c
	}
    }
    logrus.Infof("Lookup: no name %s peerid 0x%x", name, peerid)
    return nil
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
	logrus.Infof("self communication")
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
    // create stream and replay
}

// Open Ack/Nack Message
// |oack|spid|dpid|stream id|result|
func (p *Peer)UDP_handler_OpenAck(s *LocalSocket, addr *net.UDPAddr, spid, dpid uint32, data []byte) {
    p.s_openack++
    // create stream and replay
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
    default:
	logrus.Infof("msg code [%s] 0x%x 0x%x", code, spid, dpid)
    }
}

func (p *Peer)API_handler(conn net.Conn) {
    defer conn.Close()
    buf := make([]byte, 256)
    n, err := conn.Read(buf)
    if n <= 0 {
	logrus.Infof("API: Read: %v", err)
	return
    }
    firstline := strings.Split(string(buf[:n]), "\n")[0]
    req := strings.TrimSpace(firstline)
    words := strings.Fields(req)
    logrus.Infof("API: %v", words)
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
	    logrus.Info("ResolveUDPAddr: %v", err)
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
	    logrus.Infof("unknown remote %s", rname)
	    resp := fmt.Sprintf("Unknown: %s", rname)
	    conn.Write([]byte(resp))
	    return
	}
	ls, err := NewLocalServer(laddr, raddr, remote)
	if err != nil {
	    logrus.Infof("NewLocalServer: %v", err)
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
	    logrus.Infof("new checker: %s", words[1])
	    p.checkers = append(p.checkers, words[1])
	}
	p.m.Unlock()
    }
}

func (p *Peer)Housekeeper_Connection(c *Connection) {
    remotes := c.Freshers()
    now := time.Now()
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
	time.Sleep(time.Second * 5)
    }
}

func (p *Peer)Run() {
    p.running = true
    logrus.Infof("peer running")
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
    logrus.Infof("peer stopped")
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
