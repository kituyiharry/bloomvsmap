package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"runtime/trace"
)

// Our data stream
// File fetched from https://github.com/json-iterator/test-data
const LARGE_JSON_FILE = "https://raw.githubusercontent.com/json-iterator/test-data/master/large-file.json"
const TRACE_FILE = "bloomtrace.trace.out"
const PPROF_CPU_FILE = "bloomtrace-cpu.pprof.out"
const PPROF_MEM_FILE = "bloomtrace-mem.pprof.out"

type DataSource struct {
	Uri string
}

type Config struct {
	TracingEnabled bool
	TraceFile      string
	PprofCpuFile   string
	PprofMemFile   string
	DataSourcs     int
}

type Metrics struct {
	Id         string
	TotalBytes int
	Iterations int
}

func newEmptyMetrics(id string) *Metrics {
	metrics := Metrics{
		Id:         id,
		TotalBytes: 0,
		Iterations: 0,
	}
	return &metrics
}

func (m *Metrics) Dump() {
	log.Println(fmt.Sprintf("\n\tTotalBytes:\t%d\n\tIterations:\t%d\n", m.TotalBytes, m.Iterations))
}

// call and defer after
func setupTracing(cfg *Config) func() {
	if !cfg.TracingEnabled {
		return func() {}
	}

	f, err := os.Create(cfg.TraceFile)
	if err != nil {
		log.Fatalf("failed to create trace output file: %v", err)
	}

	if err := trace.Start(f); err != nil {
		log.Fatalf("failed to start trace: %v", err)
	}

	// defer this
	return func() {
		trace.Stop()
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			log.Fatalf("failed to close trace file: %v", err)
		}
	}
}

func readAllInMemory(ctx context.Context, cfg *Config) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllInMemory", func() {
			req, err := http.Get(LARGE_JSON_FILE)
			if err != nil {
				log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
			}
			metrics := newEmptyMetrics("readAllScanner")
			defer metrics.Dump()
			var dataModel []Model
			jsonBytes, err := ioutil.ReadAll(req.Body)
			metrics.TotalBytes += len(jsonBytes)
			metrics.Iterations += 1
			if err != nil {
				log.Fatalf("Error reading all data into memory ", err.Error())
			}
			if err := json.Unmarshal(jsonBytes, &dataModel); err != nil {
				log.Fatalf("Error Unmarshalling data into memory ", err.Error())
			}
		})
	}
}

func readAllScanner(ctx context.Context, cfg *Config) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllScanner", func() {
			req, err := http.Get(LARGE_JSON_FILE)
			if err != nil {
				log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
			}
			metrics := newEmptyMetrics("readAllScanner")
			defer metrics.Dump()
			reader := bufio.NewReader(req.Body)
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				totalBytes := scanner.Bytes()
				metrics.TotalBytes += len(totalBytes)
				metrics.Iterations += 1
			}
		})
	}
}

func main() {

	enableTracing := flag.Bool("e", true, "Enable Tracing files for profiling with runtime/trace")
	ctx := context.TODO()

	cfg := &Config{
		TracingEnabled: *enableTracing,
		TraceFile:      TRACE_FILE,
		PprofCpuFile:   PPROF_CPU_FILE,
		PprofMemFile:   PPROF_MEM_FILE,
	}

	closer := setupTracing(cfg)
	defer closer()

	readAllScanner(ctx, cfg)
	readAllInMemory(ctx, cfg)
}
