// MIT License Copyright(c) 2022 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "fmt"
    "net"
    "os"

    "github.com/sirupsen/logrus"
)

func checker(laddr string) {
    if laddr == "" {
	logrus.Infof("No local addr")
	return
    }
    addr, err := net.ResolveUDPAddr("udp", laddr)
    if err != nil {
	logrus.Infof("ResolveUDPAddr: %v", err)
	return
    }
    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
	logrus.Infof("ListenUDP: %v", err)
	return
    }
    logrus.Infof("Start checker on %v", laddr)
    buf := make([]byte, 1500)
    for {
	_, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
	    logrus.Infof("ReadFromUDP: %v", err)
	    continue
	}
	resp := fmt.Sprintf("Probe %v", addr)
	conn.WriteToUDP([]byte(resp), addr)
	conn.WriteToUDP([]byte(resp), addr)
	conn.WriteToUDP([]byte(resp), addr)
    }
}

func main() {
    args := os.Args
    if len(args) == 1 {
	fmt.Println("uuconn2 command options...")
	os.Exit(1)
	return
    }

    f, _ := os.Create("uuconn2.log")
    logrus.SetOutput(f)
    logrus.SetLevel(logrus.InfoLevel)

    logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat:"2006-01-02 15:04:05.000000"})

    cmd := args[1]
    opts := []string{""}
    if len(args) > 2 {
	opts = args[2:]
    }

    switch (cmd) {
    case "checker":
	checker(opts[0])
	return
    case "peer":
	peer(opts[0])
	return
    }
}
