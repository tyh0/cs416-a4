// Necessary RPC Functions

// MServer.MeasureWebsite

// MServer.GetWorkers

// website-stats ← MeasureWebsite(URI, samplesPerWorker)
// Instructs the system's workers to perform distributed measurements to URI. Each worker should perform samplesPerWorker number of measurements. Returns a data structure containing the IP of each worker and the min/median/max latency to retrieve URI by the worker. See client code below for more details.
// worker-stats ← GetWorkers(samplesPerWorker)
// Instructs the system to perform distributed measurements from each of the workers to the server. Each worker should perform samplesPerWorker number of measurements. Returns a data structure containing the IP of each worker and the min/median/max round-trip latency between worker and the server. See client code below for more details.

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	logger                *log.Logger
	workerIPPort          string
	clientIPPort          string
	err                   error
	clientRPC             *rpc.Client
	workerIPs             []string
	workerRPCClients      map[string]*rpc.Client
	WORKER_DELEGATE_PORT  string
	WORKER_PING_PONG_PORT string
	SERVER_UDP_SEND_PORT  string
	clientConnected       bool
	rpcsConfigured        bool
	ServerIP              string
)

// Server type for RPC
type MServer int

// Worker type for RPC
type Worker int

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

// Request that client sends in RPC call to MServer.GetWorkers
type MWorkersReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

// Response to:
// MServer.MeasureWebsite:
//   - latency stats per worker to a *URI*
//   - (optional) Diff map
// MServer.GetWorkers
//   - latency stats per worker to the *server*
type MRes struct {
	Stats map[string]LatencyStats    // map: workerIP -> LatencyStats
	Diff  map[string]map[string]bool // map: [workerIP x workerIP] -> True/False
}

// main workhorse function
func main() {
	logger = log.New(os.Stdout, "[Server] ", log.Lshortfile)

	WORKER_DELEGATE_PORT = ":9000"
	WORKER_PING_PONG_PORT = ":9001"
	SERVER_UDP_SEND_PORT = ":9002"
	workerIPPort, clientIPPort, err = ParseServerArguments()
	if err != nil {
		panic(err)
	}
	x := strings.LastIndex(workerIPPort, ":")
	ServerIP = workerIPPort[0:x]
	// Listen for worker connections
	go func() {
		wServer := rpc.NewServer()
		ws := new(MServer)
		wServer.Register(ws)
		wl, err := net.Listen("tcp", workerIPPort)
		checkError("", err, true)
		for {
			conn, err := wl.Accept()
			checkError("", err, false)
			go wServer.ServeConn(conn)
		}
	}()

	// Listen for client connections
	go func() {
		clientServer := rpc.NewServer()
		cs := new(MServer)
		clientServer.Register(cs)
		cl, err := net.Listen("tcp", clientIPPort)
		checkError("", err, true)
		for {
			conn, err := cl.Accept()
			clientConnected = true
			checkError("", err, false)
			go clientServer.ServeConn(conn)
		}
	}()

	for {
		if clientConnected {
			if !rpcsConfigured {
				configureWorkerRPC()
			}
		}

	}

	// Create RPC Clients to communicate with workers

	// On receiving a connection from a client, add it to the client pool
	// On receiving a request from a client to MeasureWebsite,
	//		- start a goroutine so the request is handled separately from all other clients
	// 			- send the same command to all workers
	// 			- pool their responses and then respond to client
	// On receiving a response from the worker, pool it
	// 		- once all workers have responded, respond to client with the necessary information
}

// RPC Calls

func (s *MServer) MeasureWebsite(req MWebsiteReq, res *MRes) error {
	var Response MRes
	var Diff = make(map[string]map[string]bool)
	Response.Diff = Diff
	var Stats = make(map[string]LatencyStats)
	statc, errc := make(chan LatencyStats), make(chan error)
	logger.Printf("Currently there are %v workers\n", len(workerIPs))
	for _, ip := range workerIPs {
		go func(ip string) {
			logger.Printf("about to delegate to worker %v\n", ip)
			worker_stats, err := delegateToWorker(ip, req)
			if err != nil {
				errc <- err
				return
			}
			statc <- worker_stats
		}(ip)
	}

	for i := 0; i < len(workerIPs); i++ {
		select {
		case res := <-statc:
			Stats[workerIPs[i]] = res
		case e := <-errc:
			checkError("Error with processing DelegateToWorker", e, false)
		}
	}
	Response.Stats = Stats
	*res = Response
	return nil
}

// TODO
// This should be done via UDP.
// Start a timer, then create a UDP connection between the server
// and the client, send a ping, and stop the timer when the client responds
func (s *MServer) GetWorkers(req MWorkersReq, reply *MRes) error {
	// instructs workers to perform distributed measurements between workers and server
	// each worker performs SPW measurements
	// returns similar struct as MeasureWebsite
	var Response MRes

	var Diff = make(map[string]map[string]bool)
	Response.Diff = Diff
	var Stats = make(map[string]LatencyStats)

	latc, errc := make(chan LatencyStats), make(chan error)
	logger.Print("About to call SendPingPong on all workers")
	for i, ip := range workerIPs {
		go func(ip string) {
			logger.Printf("Calling SendPingPong on worker: %v", ip)
			LS, err := SendPingPong(ip, req, i)
			if err != nil {
				errc <- err
				return
			}
			latc <- LS
		}(ip)
	}

	for i := 0; i < len(workerIPs); i++ {
		select {
		case res := <-latc:
			Stats[workerIPs[i]] = res
		case e := <-errc:
			checkError("Error with processing SendPingPong", e, false)
		}
	}
	Response.Stats = Stats
	*reply = Response
	return nil
}

func SendPingPong(ip string, req MWorkersReq, index int) (LatencyStats, error) {
	numSamples := req.SamplesPerWorker

	var LS LatencyStats
	times := make([]int, numSamples)
	PongChan := make(chan bool)
	SendPort := 7000 + index
	ListenPort := 10000 + index
	SendPortString := strconv.Itoa(SendPort)
	ListenPortString := strconv.Itoa(ListenPort)
	udpListenIPPort := ServerIP + ":" + SendPortString
	udpSendIPPort := ServerIP + ":" + ListenPortString
	logger.Printf("Will be listening in SendPingPong on %v", udpListenIPPort)
	ClientAddr, err := net.ResolveUDPAddr("udp", ip+WORKER_PING_PONG_PORT)
	ServerListenPortAddr, err := net.ResolveUDPAddr("udp", udpListenIPPort)
	ServerSendPortAddr, err := net.ResolveUDPAddr("udp", udpSendIPPort)
	checkError("Creating SendPingPong udp addrs", err, false)
	ListenConn, err := net.ListenUDP("udp", ServerListenPortAddr)
	if err != nil {
		logger.Printf("error setting up ListenConn: %v", err)
	}
	checkError("Creating Server Listen UDP Conn", err, false)
	logger.Printf("About to call ListenForPong")
	go ListenForPong(ListenConn, PongChan)
	msg := "PING-" + udpListenIPPort
	buf := []byte(msg)
	logger.Printf("Ping msg: %v", msg)
	logger.Printf("Ping msg in buffer form: %v", buf)
	logger.Print("Dialing Worker via UDP")
	SendConn, err := net.DialUDP("udp", ServerSendPortAddr, ClientAddr)
	if err != nil {
		logger.Printf("error making UDP SendConn: %v", err)
	}
	// defer ListenConn.Close()
	// defer SendConn.Close()
	for i := 0; i < numSamples; i++ {
		now := time.Now()
		logger.Printf("Sending ping %v to Worker @: %v", strconv.Itoa(i), SendConn)
		_, err = SendConn.Write(buf)
		checkError("Sending PING", err, false)
		logger.Print("Waiting on something in PongChan")
		<-PongChan
		diff := int(time.Since(now))
		diff = diff / 1000000
		times[i] = diff
		logger.Printf("Finished Sending Ping %v to Worker", strconv.Itoa(i))

	}

	// sort times
	sort.Ints(times)
	// get max
	Max := findMax(times)
	// get min
	Min := findMin(times)
	// get median
	Median := findMedian(times)
	// save min, max and median in stats
	LS.Max = Max
	LS.Min = Min
	LS.Median = Median

	// diff = diff / 1000000
	return LS, nil
}

func ListenForPong(conn *net.UDPConn, c chan bool) {
	buf := make([]byte, 1024)
	logger.Print("About to start reading from UDP in ListenForPong")
	for {
		logger.Printf("Listening on UDPConn %v", conn)
		if err != nil {
			logger.Printf("Error Listening for Pong on UDP Connection: ", err)
		}
		n, _, err := conn.ReadFromUDP(buf)
		checkError("ListenForPong", err, false)
		logger.Printf("Read value from pong: %v", buf[0:n])
		if string(buf[0:n]) == "PONG" {
			logger.Print("Putting true into channel")
			c <- true
		}
	}
}

func configureWorkerRPC() {
	workerRPCClients = make(map[string]*rpc.Client)
	for _, ip := range workerIPs {
		workerRPCClients[ip] = getServerWorkerRPCClient(ip)
	}
	rpcsConfigured = true
}

func (s *MServer) JoinWorker(ip string, reply *bool) error {
	// save worker ip:port in list of workers
	logger.Println("New server joined!")
	workerIPs = append(workerIPs, ip)
	fmt.Printf("Fmt worker ip: %v", ip)
	logger.Printf("New worker ip: ", ip)
	fmt.Printf("WorkerIPs: %v", workerIPs)
	*reply = true
	return nil
}

func delegateToWorker(workerIP string, req MWebsiteReq) (LatencyStats, error) {
	var LS LatencyStats
	logger.Printf("There are %v worker rpc clients", len(workerRPCClients))
	logger.Printf("Calling worker.FetchTest on ip: %v", workerIP)
	client := workerRPCClients[workerIP]
	logger.Println("About to call Worker.FetchTest")
	client.Call("Worker.FetchTest", req, &LS)
	return LS, nil
}

// Parses the command line args.
func ParseServerArguments() (workerInfo, clientInfo string, err error) {
	args := os.Args[1:]
	if len(args) != 2 {
		err = fmt.Errorf("Usage: {go run server.go [worker-incoming ip:port] [client-incoming ip:port]}")
	} else {
		// parse args
		workerInfo = args[0]
		clientInfo = args[1]
		logger.Printf("Listening for workers on: %v\n", workerInfo)
		logger.Printf("Listening for clients on: %v\n", clientInfo)
	}
	return workerInfo, clientInfo, err
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

// function to create an RPC Client that connects to a worker
func getServerWorkerRPCClient(workerIP string) *rpc.Client {
	raddr, err := net.ResolveTCPAddr("tcp", workerIP+WORKER_DELEGATE_PORT)
	if err != nil {
		logger.Fatal(err)
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		logger.Fatal(err)
	}
	client := rpc.NewClient(conn)

	return client
}

// function to find max int value
func findMax(times []int) int {
	if len(times) == 0 {
		return 0
	}
	Max := times[0]
	for _, val := range times {
		if val > Max {
			Max = val
		}
	}
	return Max
}

// function to find min int value
func findMin(times []int) int {
	if len(times) == 0 {
		return 0
	}
	Max := times[0]
	for _, val := range times {
		if val < Max {
			Max = val
		}
	}
	return Max
}

// function to find median int value
func findMedian(times []int) int {
	medValue := len(times) / 2
	return times[medValue]
}
