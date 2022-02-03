// MIT License Copyright(c) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "net"
    "os"
    "os/exec"
    "strings"
    "time"

    "github.com/sirupsen/logrus"
    "github.com/hshimamoto/go-session"
)

type Peer struct {
    addr string
    cmd *exec.Cmd
    peerid string
    uaddr string
}

func NewPeer(addr string) (*Peer, error) {
    cmd := exec.Command("./uuconn2", "peer", addr)
    // start test process
    err := cmd.Start()
    if err != nil {
	return nil, err
    }
    time.Sleep(time.Millisecond * 10)
    os.Rename("uuconn2.log", "uuconn2-1.log")
    p := &Peer{
	addr: addr,
	cmd: cmd,
    }
    return p, nil
}

func (p *Peer)Stop() {
    if p.cmd.Process != nil {
	logrus.Infof("SIGINT to %s", p.peerid)
	p.cmd.Process.Signal(os.Interrupt)
    }
    p.cmd.Wait()
}

func (p *Peer)Do(cmd string) string {
    conn, err := session.Dial(p.addr)
    if err != nil {
	logrus.Infof("Dial %v", err)
	return ""
    }
    defer conn.Close()
    conn.Write([]byte(cmd))
    buf := make([]byte, 4096)
    n, _ := conn.Read(buf)
    resp := string(buf[:n])
    logrus.Infof("Command: %s", cmd)
    logrus.Infof("Resp:\n%s", resp)
    return resp
}

func (p *Peer)Info() {
    info := p.Do("INFO")
    p.peerid = strings.Split(strings.Split(info, "\n")[0], " ")[1]
    p.uaddr = get_addr(info)
}

func get_addr(info string) string {
    for _, l := range strings.Split(info, "\n") {
	if len(l) < 12 {
	    continue
	}
	if l[0:11] == "localsocket" {
	    w := strings.Split(l, " ")
	    if w[1] != "" {
		return w[1]
	    }
	}
    }
    return ""
}

func dumpinfo(peers []*Peer, n int) {
    for i := 0; i < n; i++ {
	logrus.Infof("dumpinfo %d/%d", i+1, n)
	for _, p := range peers {
	    p.Do("INFO")
	}
	time.Sleep(time.Second)
    }
}

type TestServer struct {
    serv *session.Server
    handler func(net.Conn)
}

func NewTestServer(addr string) *TestServer {
    ts := &TestServer{}
    serv, _ := session.NewServer(addr, func(conn net.Conn) {
	ts.handler(conn)
    })
    ts.serv = serv
    ts.handler = ts.Handler
    return ts
}

func (ts *TestServer)Handler(conn net.Conn) {
    defer conn.Close()
    // 1MiB transfer
    buf := make([]byte, 256)
    for i := 0; i < 256; i++ {
	buf[i] = byte(i)
    }
    for i := 0; i < 4 * 1024; i++ {
	conn.Write(buf)
    }
    logrus.Infof("Transfer DONE")
}

func (ts *TestServer)Run() {
    ts.serv.Run()
}

func (ts *TestServer)Stop() {
    ts.serv.Stop()
}

func (ts *TestServer)Test(addr string) {
    // connect to addr and recv data...
    conn, _ := session.Dial(addr)
    defer conn.Close()

    buf := make([]byte, 256)
    bad := false
    for i := 0; i < 4 * 1024; i++ {
	for n := 0; n < 256; n++ {
	    buf[n] = byte(255 - n)
	}
	conn.Read(buf)
	for n := 0; n < 256; n++ {
	    if buf[n] != byte(n) {
		bad = true
	    }
	}
    }
    if bad {
	logrus.Infof("bad result")
    }
    logrus.Infof("complete")
}

func HelloWorldHandler(conn net.Conn) {
    buf := make([]byte, 256)
    n, _ := conn.Read(buf)
    logrus.Infof("Test Server recv %d bytes %s", n, string(buf[:n]))
    if string(buf[:n]) == "HELLO" {
	conn.Write([]byte("WORLD"))
    }
    time.Sleep(time.Second)
    conn.Close()
}

func HelloWorldTest(addr string) {
    conn, _ := session.Dial(addr)

    // write HELLO
    logrus.Infof("write HELLO to conn")
    conn.Write([]byte("HELLO"))

    // read WORLD
    ticker := time.NewTicker(time.Second)
    ch := make(chan bool)

    go func() {
	buf := make([]byte, 256)
	n, _ := conn.Read(buf)
	logrus.Infof("read %s from conn1", buf[:n])
	ch <- true
    }()

    select {
    case <-ch:
    case <-ticker.C:
	logrus.Infof("Timeout")
    }

    conn.Close()
    ticker.Stop()
}

func Scenario() {
    logrus.Infof("start scenario")
    // local uuconn2 instance 1
    peer1, err := NewPeer("localhost:8888")
    if err != nil {
	logrus.Infof("NewPeer: %v", err)
	return
    }
    // local uuconn2 instance 2
    peer2, err := NewPeer("localhost:8889")
    if err != nil {
	logrus.Infof("NewPeer: %v", err)
	// kill peer1
	peer1.Stop()
	return
    }

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    logrus.Infof("uuconn2 instances started")

    peers := []*Peer{
	peer1,
	peer2,
    }

    for _, p := range peers {
	p.Info()
    }

    logrus.Infof("addr1 = %s", peer1.uaddr)
    logrus.Infof("addr2 = %s", peer2.uaddr)

    time.Sleep(time.Millisecond * 100)

    // start test server
    ts_HelloWorld := NewTestServer(":18889")
    ts_HelloWorld.handler = HelloWorldHandler
    go ts_HelloWorld.Run()

    ts := NewTestServer(":28889")
    go ts.Run()

    // ask to connect
    peer1.Do("CONNECT " + peer2.uaddr)

    time.Sleep(time.Millisecond * 100)

    // show connection
    peer1.Do("SHOW " + peer2.peerid)
    peer2.Do("SHOW " + peer1.peerid)

    dumpinfo(peers, 3)

    logrus.Infof("adding localserv")

    peer1.Do("ADD 127.0.0.1:18888 " + peer2.peerid + ":127.0.0.1:18889")
    peer1.Do("ADD 127.0.0.1:28888 " + peer2.peerid + ":127.0.0.1:28889")

    dumpinfo(peers, 3)

    HelloWorldTest("127.0.0.1:18888")

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    ts.Test("localhost:28888")

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    ts.Stop()

    // show connection
    peer1.Do("SHOW " + peer2.peerid)
    peer2.Do("SHOW " + peer1.peerid)

    dumpinfo(peers, 5)

    // show connection
    peer1.Do("SHOW " + peer2.peerid)
    peer2.Do("SHOW " + peer1.peerid)

    logrus.Infof("ending test")

    // 5sec...
    time.Sleep(time.Second * 5)

    for _, p := range peers {
	p.Stop()
    }

    logrus.Infof("end scenario")
}

func main() {
    Scenario()
    os.Exit(0)
}
