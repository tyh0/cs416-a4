package main

// needs to access websites and return the round-trip-time
// needs to calculate mean, median and max of rtt's

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	logger               *log.Logger
	mserverIpPort        string
	err                  error
	rpcClient            *rpc.Client
	DELEGATE_LISTEN_PORT string
	PINGPONG_LISTEN_PORT string
	PINGPONG_SEND_PORT   string
	WorkerIP             string
)

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

// worker type for RPC
type Worker int

// server type for RPC
type MServer int

func main() {
	logger = log.New(os.Stdout, "[Worker] ", log.Lshortfile)

	// set port to listen to server on
	DELEGATE_LISTEN_PORT = ":9000"
	PINGPONG_LISTEN_PORT = ":9001"
	PINGPONG_SEND_PORT = ":9003"
	// parse arguments
	mserverIpPort, err = ParseWorkerArguments()
	checkError("", err, true)
	// Listen for workers RPC calls
	logger.Println("Attempting to connect to server")
	ConnectToServer()
	// diffTest()
	go ListenForServerRPC()
	go ListenForPingPong()
	logger.Println("Finished connecting to server")
	for {
		// keep the Worker running to listen for server communication
	}
}

func ListenForPingPong() {
	UDPListenAddrStr := WorkerIP + PINGPONG_LISTEN_PORT
	UDPSendAddrStr := WorkerIP + PINGPONG_SEND_PORT
	logger.Printf("Listening for Server UDP Connections on: %v", UDPListenAddrStr)
	UDPListenAddr, err := net.ResolveUDPAddr("udp", UDPListenAddrStr)
	UDPSendAddr, err := net.ResolveUDPAddr("udp", UDPSendAddrStr)

	checkError("ListenForServerUDP: ", err, true)

	WListenConn, err := net.ListenUDP("udp", UDPListenAddr)
	checkError("Creating WListenConn: ", err, false)
	// defer WListenConn.Close()
	buf := make([]byte, 1024)
	for {
		logger.Print("Reading data from UDP connection")
		n, _, err := WListenConn.ReadFromUDP(buf)
		checkError("ListenForPingPong", err, false)
		servermsg := string(buf[0:n])
		logger.Printf("Received message from server: %v", servermsg)
		logger.Printf("servermsg[0:4]: %v", servermsg[0:4])
		if servermsg[0:5] == "PING-" {
			respIpPort := servermsg[5:]
			logger.Printf("Responding to server on: %v", servermsg[5:])
			ServerUDPAddr, err := net.ResolveUDPAddr("udp", respIpPort)

			WSendConn, err := net.DialUDP("udp", UDPSendAddr, ServerUDPAddr)
			checkError("Creating WSendConn: ", err, false)

			workermsg := "PONG"
			logger.Print("Sending PONG to server")
			wbuf := []byte(workermsg)
			_, err = WSendConn.Write(wbuf)
			checkError("Writing PONG", err, false)
			WSendConn.Close()
		}
	}
}

func ListenForServerRPC() {
	// Listen for client connections
	workerServer := rpc.NewServer()
	w := new(Worker)
	workerServer.Register(w)
	workerConnectionIPPort := WorkerIP + DELEGATE_LISTEN_PORT
	l, err := net.Listen("tcp", workerConnectionIPPort)
	logger.Println("Listening for server delegate calls")
	checkError("", err, true)
	for {
		conn, err := l.Accept()
		logger.Println("Server delegate connection accepted")
		checkError("Accepting server delegation request", err, false)
		go workerServer.ServeConn(conn)
	}
}
func ConnectToServer() {
	// open RPC client to server
	rpcClient = getRPCClient()
	// join to server
	var reply bool
	// call RPC to join to server
	// Creating connection to server so we can get IP
	logger.Printf("About to dial server at %v: \n", mserverIpPort)
	conn, err := net.Dial("tcp", mserverIpPort)
	// Pulling out IP
	logger.Println("Finished dialing server")

	localAddr := conn.LocalAddr().String()
	index := strings.LastIndex(localAddr, ":")
	WorkerIP = localAddr[0:index]
	logger.Printf("Current IP: %v\n", WorkerIP)
	logger.Println("About to call MServer.JoinWorker")
	err = rpcClient.Call("MServer.JoinWorker", WorkerIP, &reply)
	// check for errors
	logger.Print("Hopefully got IP")
	checkError("Connecting Worker to Server", err, true)
}

// Create RPC client for contacting the wserver.
func getRPCClient() *rpc.Client {
	raddr, err := net.ResolveTCPAddr("tcp", mserverIpPort)
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

// Parses the command line args.
func ParseWorkerArguments() (serverInfo string, err error) {
	args := os.Args[1:]
	if len(args) != 1 {
		err = fmt.Errorf("Usage: {go run worker.go [server ip:port]}")
	} else {
		// parse args
		serverInfo = args[0]
		fmt.Println("before using logger")
		logger.Printf("Connecting to server on: %v\n", serverInfo)
	}
	return serverInfo, err
}

// Function to check for errors
func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func fetch(uri string) int {
	// start timer
	start := time.Now()
	// Perform Get
	resp, err := http.Get(uri)
	// defer closing body
	defer resp.Body.Close()
	// get time diff between start and end of Get (in ns)
	diff := int(time.Since(start))
	// convert nanoseconds to milliseconds
	diff = diff / 1000000
	// check error
	checkError("FetchTest: ", err, false)
	// save time in times array
	return diff
}

func (w *Worker) FetchTest(req MWebsiteReq, reply *LatencyStats) error {
	logger.Println("FetchTest started")
	// create LatencyStats to return
	var Stats LatencyStats
	// peel off SamplesPerWorker
	numSamples := req.SamplesPerWorker
	logger.Printf("Number of samples to run: %v", numSamples)
	// create array of ints to hold response times
	times := make([]int, numSamples)
	logger.Println("About to make Get calls")
	// perform fetch and measure response times, SamplesPerWorker times
	for i := 0; i < numSamples; i++ {
		index := strconv.Itoa(i)
		logger.Printf("Current index: %v\n", index)
		logger.Printf("Making Get call %v\n", index)
		times[i] = fetch(req.URI)
		logger.Printf("Finished Get call %v%n", index)
	}
	logger.Println("Calculating stats")
	// sort times
	sort.Ints(times)
	// get max
	Max := findMax(times)
	// get min
	Min := findMin(times)
	// get median
	Median := findMedian(times)
	// save min, max and median in stats
	Stats.Max = Max
	Stats.Min = Min
	Stats.Median = Median
	logger.Println("Finished calculating stats, about to send result back")
	// send stats back to server callee
	*reply = Stats
	logger.Println("FetchTest Finished")
	return nil
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
