// MIT License Copyright(c) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
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
    // start inst2
    err2 := inst2.Start()
    if err2 != nil {
	logrus.Infof("Start: %v", err2)
    }

    // wait a bit
    time.Sleep(time.Second)

    logrus.Infof("uuconn2 instances started")

    // show inst1 INFO
    info1 := api("localhost:8888", "INFO")
    // show inst2 INFO
    info2 := api("localhost:8889", "INFO")

    time.Sleep(time.Second)

    // find inst1 addr
    addr1 := get_addr(info1)
    addr2 := get_addr(info2)

    logrus.Infof("addr1 = %s", addr1)
    logrus.Infof("addr2 = %s", addr2)

    // ask to connect
    api("localhost:8888", "CONNECT " + addr2)
    //api("localhost:8889", "CONNECT " + addr1)

    // 1sec...
    time.Sleep(time.Second)

    for i := 0; i < 10; i++ {
	logrus.Infof("check %d/10", i+1)
	// show inst1 INFO
	api("localhost:8888", "INFO")
	// show inst2 INFO
	api("localhost:8889", "INFO")
	// 1sec...
	time.Sleep(time.Second)
    }

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
