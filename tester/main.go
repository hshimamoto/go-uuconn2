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

func api(addr, cmd string) string {
    conn, err := session.Dial(addr)
    if err != nil {
	logrus.Infof("Dial %v", err)
	return ""
    }
    defer conn.Close()
    conn.Write([]byte(cmd))
    buf := make([]byte, 4096)
    n, _ := conn.Read(buf)
    logrus.Infof("Command: %s", cmd)
    resp := string(buf[:n])
    logrus.Infof("Resp:\n%s", resp)
    return resp
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

func dumpinfo(n int) {
    for i := 0; i < n; i++ {
	logrus.Infof("dumpinfo %d/%d", i+1, n)
	api("localhost:8888", "INFO") // show inst1 INFO
	api("localhost:8889", "INFO") // show inst2 INFO
	time.Sleep(time.Second)
    }
}

type TestServer struct {
    serv *session.Server
}

func NewTestServer(addr string) *TestServer {
    ts := &TestServer{}
    serv, _ := session.NewServer(addr, func(conn net.Conn) {
	ts.Handler(conn)
    })
    ts.serv = serv
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

func Scenario() {
    logrus.Infof("start scenario")
    // local uuconn2 instance 1
    inst1 := exec.Command("./uuconn2", "peer", ":8888")
    // local uuconn2 instance 2
    inst2 := exec.Command("./uuconn2", "peer", ":8889")
    // start inst1
    err1 := inst1.Start()
    if err1 != nil {
	logrus.Infof("Start: %v", err1)
    }
    time.Sleep(time.Millisecond * 10)
    os.Rename("uuconn2.log", "uuconn2-1.log")
    time.Sleep(time.Millisecond * 10)
    // start inst2
    err2 := inst2.Start()
    if err2 != nil {
	logrus.Infof("Start: %v", err2)
    }
    time.Sleep(time.Millisecond * 10)
    os.Rename("uuconn2.log", "uuconn2-2.log")
    time.Sleep(time.Millisecond * 10)

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    logrus.Infof("uuconn2 instances started")

    // show inst1 INFO
    info1 := api("localhost:8888", "INFO")
    // show inst2 INFO
    info2 := api("localhost:8889", "INFO")

    // find inst1 addr
    addr1 := get_addr(info1)
    addr2 := get_addr(info2)
    //peerid1 := strings.Split(strings.Split(info1, "\n")[0], " ")[1]
    peerid2 := strings.Split(strings.Split(info2, "\n")[0], " ")[1]

    logrus.Infof("addr1 = %s", addr1)
    logrus.Infof("addr2 = %s", addr2)

    time.Sleep(time.Millisecond * 100)

    // start test server
    testserv, _ := session.NewServer(":18889", func(conn net.Conn) {
	buf := make([]byte, 256)
	n, _ := conn.Read(buf)
	logrus.Infof("Test Server recv %d bytes %s", n, string(buf[:n]))
	if string(buf[:n]) == "HELLO" {
	    conn.Write([]byte("WORLD"))
	}
	time.Sleep(time.Second)
	conn.Close()
    })
    go testserv.Run()

    ts := NewTestServer(":28889")
    go ts.Run()

    // ask to connect
    api("localhost:8888", "CONNECT " + addr2)
    //api("localhost:8889", "CONNECT " + addr1)

    dumpinfo(3)

    logrus.Infof("adding localserv")

    api("localhost:8888", "ADD 127.0.0.1:18888 " + peerid2 + ":127.0.0.1:18889")
    api("localhost:8888", "ADD 127.0.0.1:28888 " + peerid2 + ":127.0.0.1:28889")

    dumpinfo(3)

    // try to connect
    conn1, _ := session.Dial("127.0.0.1:18888")

    dumpinfo(2)

    // try to send data in conn1
    logrus.Infof("write HELLO to conn1")
    conn1.Write([]byte("HELLO"))

    dumpinfo(2)

    // try to read data
    ticker := time.NewTicker(time.Second)
    ch := make(chan bool)

    go func() {
	buf := make([]byte, 256)
	n, _ := conn1.Read(buf)
	logrus.Infof("read %s from conn1", buf[:n])
	ch <- true
    }()

    select {
    case <-ch:
    case <-ticker.C:
	logrus.Infof("Timeout")
    }

    conn1.Close()
    ticker.Stop()

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    ts.Test("localhost:28888")

    // wait a bit
    time.Sleep(time.Millisecond * 100)

    ts.Stop()

    dumpinfo(3)

    logrus.Infof("ending test")

    // 3sec...
    time.Sleep(time.Second * 3)

    if inst1.Process != nil {
	logrus.Infof("SIGINT to inst1")
	inst1.Process.Signal(os.Interrupt)
    }
    if inst2.Process != nil {
	logrus.Infof("SIGINT to inst2")
	inst2.Process.Signal(os.Interrupt)
    }

    // wait...
    inst1.Wait()
    inst2.Wait()
    logrus.Infof("end scenario")
}

func main() {
    Scenario()
    os.Exit(0)
}
