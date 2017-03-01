/*
Implements the client in assignment 4 for UBC CS 416 2016 W2.

Usage:

Measure a website:
go run client.go -m [server ip:port] [URI] [samples]

Retrieve worker status:
go run client.go -w [server ip:port] [samples]

Example:
go run client.go -m 127.0.0.1:19001 http://www.facebook.com 33
go run client.go -w 127.0.0.1:19001 33

*/

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

var (
	logger       *log.Logger // Global logger.
	client       *rpc.Client // RPC client.
	serverIpPort string      // RPC server
	URI          string      // URI to measure
	samplesPerW  int         // Number of samples per worker, >= 1

)

//Modes of operation
const (
	MEASUREWEBSITE = iota
	GETWORKERS
)

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

/////////////// RPC structs

// Resource server type.
type MServer int

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
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

// Request that client sends in RPC call to MServer.GetWorkers
type MWorkersReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

/////////////// /RPC structs

// Main workpuppy method.
func main() {
	// Parse the command line args, panic if error
	mode, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	// Create RPC client for contacting the server.
	client = getRPCClient()

	switch mode {
	case MEASUREWEBSITE:
		req := MWebsiteReq{
			URI:              URI,
			SamplesPerWorker: samplesPerW,
		}
		var res MRes
		// Make RPC call.
		err := client.Call("MServer.MeasureWebsite", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		res.Print()

	case GETWORKERS:
		req := MWorkersReq{
			SamplesPerWorker: samplesPerW,
		}
		var res MRes
		// Make RPC call.
		err := client.Call("MServer.GetWorkers", req, &res)
		if err != nil {
			fmt.Println(err)
		}
		res.Print()

	default:
		err = fmt.Errorf("Invalid mode")
	}
	client.Close()
}

// Parses the command line args.
func ParseArguments() (mode int, err error) {
	args := os.Args[1:]

	if len(args) == 4 && args[0] == "-m" {
		// MeasureWebsite RPC call case
		mode = MEASUREWEBSITE
		serverIpPort = args[1]
		URI = args[2]

		samplesPerW, err = strconv.Atoi(args[3])
		if err != nil {
			logger.Fatal(err)
		}
		fmt.Printf("Invoking MeasureWebsite with URI: %s, numSamples: %d ...\n", URI, samplesPerW)

	} else if len(args) == 3 && args[0] == "-w" {
		// GetWorkers RPC call case
		mode = GETWORKERS
		serverIpPort = args[1]

		samplesPerW, err = strconv.Atoi(args[2])
		if err != nil {
			logger.Fatal(err)
		}
		fmt.Printf("Invoking GetWorkers with numSamples: %d ...\n", samplesPerW)

	} else {
		err = fmt.Errorf("Usage: {go run client.go -m [server ip:port] [URI] [samples]} | {go run client.go -w [server ip:port] [samples]}")
		return
	}
	return
}

// Create RPC client for contacting the server.
func getRPCClient() *rpc.Client {
	raddr, err := net.ResolveTCPAddr("tcp", serverIpPort)
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

/////////////////// Pretty printing fns.

func (r MRes) Print() {
	for workerIP, latencyStats := range r.Stats {
		fmt.Printf("Worker: %s, ", workerIP)
		fmt.Printf("Latency: ")
		latencyStats.Print()
	}

	// Optional: print the diff map, if one is set.
	if r.Diff != nil {
		fmt.Printf("Diff:\n")
		for wIP1, wMap := range r.Diff {
			fmt.Printf("%s: ", wIP1)
			for wIP2, diffVal := range wMap {
				fmt.Printf("[%s : %t] ", wIP2, diffVal)
			}
			fmt.Printf("\n")
		}
	}
}

// Print latency stats
func (l LatencyStats) Print() {
	fmt.Printf("[min:%d, median:%d max:%d]\n", l.Min, l.Median, l.Max)
}
