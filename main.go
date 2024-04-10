package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// Our data stream
// File fetched from https://github.com/json-iterator/test-data
const LARGE_JSON_FILE = "https://raw.githubusercontent.com/json-iterator/test-data/master/large-file.json"
const TRACE_FILE = "bloomtrace.trace.out"

type Model struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Public    bool   `json:"public"`
	CreatedAt string `json:"created_at"`
	Actor     struct {
		Id     int    `json:"id"`
		Login  string `json:"login"`
		Grav   string `json:"gravatar_id"`
		Url    string `json:"url"`
		Avatar string `json:"avatar_url"`
	} `json:"actor"`
	Repo struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
		Url  string `json:"url"`
	} `json:"repo"`
	Payload struct {
		Action       string `json:"action"`
		Ref          string `json:"ref"`
		RefType      string `json:"ref_type"`
		MasterBranch string `json:"master_branch"`
		Description  string `json:"description"`
		PusherType   string `json:"pusher_type"`
		Head         string `json:"head"`
		Before       string `json:"before"`
		Commits      []struct {
			Sha    string `json:"sha"`
			Author struct {
				Email string `json:"email"`
				Name  string `json:"name"`
			} `json:"author"`
			Message  string `json:"message"`
			Distinct bool   `json:"distinct"`
			Url      string `json:"url"`
		} `json:"commits"`
	} `json:"payload"`
}

type Config struct {
	TracingEnabled bool
	TraceFile      string
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

	type Model struct {
		Id        string `json:"id"`
		Type      string `json:"type"`
		Public    bool   `json:"public"`
		CreatedAt string `json:"created_at"`
		Actor     struct {
			Id     int    `json:"id"`
			Login  string `json:"login"`
			Grav   string `json:"gravatar_id"`
			Url    string `json:"url"`
			Avatar string `json:"avatar_url"`
		} `json:"actor"`
		Repo struct {
			Id   int    `json:"id"`
			Name string `json:"name"`
			Url  string `json:"url"`
		} `json:"repo"`
		Payload struct {
			Action       string `json:"action"`
			Ref          string `json:"ref"`
			RefType      string `json:"ref_type"`
			MasterBranch string `json:"master_branch"`
			Description  string `json:"description"`
			PusherType   string `json:"pusher_type"`
			Head         string `json:"head"`
			Before       string `json:"before"`
			Commits      []struct {
				Sha    string `json:"sha"`
				Author struct {
					Email string `json:"email"`
					Name  string `json:"name"`
				} `json:"author"`
				Message  string `json:"message"`
				Distinct bool   `json:"distinct"`
				Url      string `json:"url"`
			} `json:"commits"`
		} `json:"payload"`
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

var (
	pushEventMap map[string]bool = map[string]bool{}
	blomfil                      = bloom.NewWithEstimates(12000, 0.1)
	halfblomfil                  = bloom.NewWithEstimates(6000, 0.1)
)

func memUsage(mOld, mNew *runtime.MemStats) {
	fmt.Println(
		fmt.Sprintf(
			"[Alloc]: %d, [Heap]: %d, [Total]: %d  MBs BloomFil: (%d , %d) ",
			((mNew.Alloc - mOld.Alloc) / 1000000),
			((mNew.HeapAlloc - mOld.HeapAlloc) / 1000000),
			((mNew.TotalAlloc - mOld.TotalAlloc) / 1000000),
			blomfil.ApproximatedSize(),
			blomfil.BitSet().BinaryStorageSize(),
		))
}

func ProcessChunkUsingMap(md *Model) {
	if md.Type == "PushEvent" {
		pushEventMap[md.Id] = true
	}
}

func ProcessChunkUsingBloom(md *Model) {
	if md.Type == "PushEvent" {
		blomfil.AddString(md.Id)
		halfblomfil.AddString(md.Id)
	}
}

func readAllInMemoryInternal(ctx context.Context, cfg *Config, proc func(*Model)) {
	client := http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := client.Get(LARGE_JSON_FILE)
	if err != nil {
		log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
	}
	var dataModel []Model
	jsonBytes, err := io.ReadAll(req.Body)
	if err != nil {
		log.Fatalf("Error reading all data into memory: %v", err)
	}
	if err := json.Unmarshal(jsonBytes, &dataModel); err != nil {
		log.Fatalf("Error Unmarshalling data into memory: %v", err)
	}
	for _, m := range dataModel {
		proc(&m)
	}
	log.Println("entries: %d", len(dataModel))
}

func readAllInMemoryInternalBuffered(ctx context.Context, cfg *Config, proc func(*Model)) {
	client := http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := client.Get(LARGE_JSON_FILE)
	if err != nil {
		log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
	}
	var dataModel []Model
	jsonBytes, err := io.ReadAll(bufio.NewReader(req.Body))
	if err != nil {
		log.Fatalf("Error reading all data into memory: %v", err)
	}
	if err := json.Unmarshal(jsonBytes, &dataModel); err != nil {
		log.Fatalf("Error Unmarshalling data into memory: %v", err)
	}
	for _, m := range dataModel {
		proc(&m)
	}
	log.Println("entries: %d", len(dataModel))
}

func ReadAllInMemory(ctx context.Context, cfg *Config, proc func(*Model)) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllInMemory", func() {
			readAllInMemoryInternal(ctx, cfg, proc)
		})
	} else {
		readAllInMemoryInternal(ctx, cfg, proc)
	}
}

func ReadAllInMemoryBuffered(ctx context.Context, cfg *Config, proc func(*Model)) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllInMemory", func() {
			readAllInMemoryInternalBuffered(ctx, cfg, proc)
		})
	} else {
		readAllInMemoryInternalBuffered(ctx, cfg, proc)
	}
}

func readAllStreamingBufferedInternal(ctx context.Context, cfg *Config, proc func(*Model)) {
	client := http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := client.Get(LARGE_JSON_FILE)
	if err != nil {
		log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
	}
	dec := json.NewDecoder(bufio.NewReader(req.Body))
	var dataModel []Model
	if toke, err := dec.Token(); err != nil {
		log.Fatalf("Token decoding error: %v %v", toke, err)
	} else {
		for dec.More() {
			m := Model{}
			if err := dec.Decode(&m); err != nil {
				log.Println("decoding err => ", err.Error())
			} else {
				proc(&m)
				dataModel = append(dataModel, m)
			}
		}
	}
	log.Println("entries: %d", len(dataModel))
}

func readAllStreamingInternal(ctx context.Context, cfg *Config, proc func(*Model)) {
	client := http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := client.Get(LARGE_JSON_FILE)
	if err != nil {
		log.Fatal("ERROR FETCHING TEST DATA: ", err.Error())
	}
	dec := json.NewDecoder(req.Body)
	var dataModel []Model
	if toke, err := dec.Token(); err != nil {
		log.Fatalf("Token decoding error: %v %v", toke, err)
	} else {
		for dec.More() {
			m := Model{}
			if err := dec.Decode(&m); err != nil {
				log.Println("decoding err => ", err.Error())
			} else {
				dataModel = append(dataModel, m)
				proc(&m)
			}
		}
	}
	log.Println("entries: %d", len(dataModel))
}

func ReadAllStreaming(ctx context.Context, cfg *Config, proc func(*Model)) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllStreaming", func() {
			readAllStreamingInternal(ctx, cfg, proc)
		})
	} else {
		readAllStreamingInternal(ctx, cfg, proc)
	}
}

func ReadAllStreamingBuffered(ctx context.Context, cfg *Config, proc func(*Model)) {
	if trace.IsEnabled() {
		trace.WithRegion(ctx, "readAllStreaming", func() {
			readAllStreamingBufferedInternal(ctx, cfg, proc)
		})
	} else {
		readAllStreamingBufferedInternal(ctx, cfg, proc)
	}
}

func Save(filename string, data []byte) error {

	fi, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer fi.Close()

	fz := gzip.NewWriter(fi)
	defer fz.Close()

	fz.Write(data)

	return err
}

func Confirm() {
	hitCount := 0
	missCount := 0
	halfCoount := 0
	mhalfCount := 0

	for k, _ := range pushEventMap {
		if blomfil.TestString(k) {
			hitCount += 1
		} else {
			missCount += 1
		}

		if halfblomfil.TestString(k) {
			halfCoount += 1
		} else {
			mhalfCount += 1
		}
	}

	log.Println(fmt.Sprintf("Hits in bloom: %d, Miss in bloom: %d, Half in: %d, Half miss: %d", hitCount, missCount, halfCoount, mhalfCount))
}

func main() {

	enableTracing := flag.Bool("e", true, "Enable Tracing files for profiling with runtime/trace")
	ctx := context.TODO()

	var (
		m1, m2, m3 runtime.MemStats
	)

	cfg := &Config{
		TracingEnabled: *enableTracing,
		TraceFile:      TRACE_FILE,
	}

	closer := setupTracing(cfg)
	defer closer()

	runtime.ReadMemStats(&m1)
	ReadAllStreaming(ctx, cfg, ProcessChunkUsingMap)
	runtime.ReadMemStats(&m2)
	memUsage(&m1, &m2)
	ReadAllStreaming(ctx, cfg, ProcessChunkUsingBloom)
	// memory consumption can actually reduce causing an overflow
	runtime.ReadMemStats(&m3)
	memUsage(&m2, &m3)

	blomBytes, err := blomfil.GobEncode()
	if err != nil {
		log.Fatalf("Error on gob Marshal: %v", err)
	}

	halfblomBytes, err := halfblomfil.GobEncode()
	if err != nil {
		log.Fatalf("Error on gob Marshal: %v", err)
	}

	var buf bytes.Buffer
	gobenc := gob.NewEncoder(&buf)
	err = gobenc.Encode(pushEventMap)
	if err != nil {
		log.Fatalf("Error on json Marshal: %v", err)
	}

	Save("mapBytes.gob", buf.Bytes())
	Save("bloomBytes.gob", blomBytes)
	Save("halfbloomBytes.gob", halfblomBytes)
	Confirm()
}
