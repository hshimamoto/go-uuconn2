all: uuconn2 tester

uuconn2: main.go peer.go
	go build

tester:
	$(MAKE) -C tester

.PHONY: tester
