package main

import (
	"math"
	"testing"
	"time"
)

func TestParseSqueueMinMemoryMiB(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"16384", 16384},
		{"16G", 16 * 1024},
		{"2T", 2 * 1024 * 1024},
		{"512M", 512},
		{`"8192"`, 8192},
		{"", 0},
		{"N/A", 0},
	}
	for _, tt := range tests {
		got := parseSqueueMinMemoryMiB(tt.in)
		if math.Abs(got-tt.want) > 1e-6 {
			t.Errorf("parseSqueueMinMemoryMiB(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseAllocTRESGPUs(t *testing.T) {
	if g := parseAllocTRESGPUs(`cpu=4,mem=16G,gres/gpu=2`); g != 2 {
		t.Errorf("got %v", g)
	}
	if g := parseAllocTRESGPUs(`gres/gpu=8`); g != 8 {
		t.Errorf("got %v", g)
	}
}

func TestExtractGPUBindSegments(t *testing.T) {
	segs := extractGPUBindSegments(`gpu:0,1`)
	if len(segs) != 1 || segs[0] != "0,1" {
		t.Errorf("got %#v", segs)
	}
	segs = extractGPUBindSegments(`GPU:2|gpu:3,4`)
	if len(segs) != 2 || segs[0] != "2" || segs[1] != "3,4" {
		t.Errorf("got %#v", segs)
	}
}

func TestNormalizeSqueueJobID(t *testing.T) {
	if normalizeSqueueJobID("12345.batch") != "12345" {
		t.Fail()
	}
	if normalizeSqueueJobID("12345.0") != "12345" {
		t.Fail()
	}
	if normalizeSqueueJobID("12345_2") != "12345_2" {
		t.Fail()
	}
}

func TestExtractGresDetailGPUIdxAnywhere(t *testing.T) {
	if s := extractGresDetailGPUIdxAnywhere(`gpu:4(IDX:0-3)`); s != "0,1,2,3" {
		t.Errorf("got %q", s)
	}
	if s := extractGresDetailGPUIdxAnywhere(`gpu:4(S:0-3)`); s != "0,1,2,3" {
		t.Errorf("S: got %q", s)
	}
	if s := extractGresDetailGPUIdxAnywhere(`gpu:2(IDX:N/A)`); s != "" {
		t.Errorf("got %q", s)
	}
	if s := extractAllGPUIndicesFromSlurmGres(`gpu:2(S:0-1) gpu:2(S:2-3)`); s != "0,1,2,3" {
		t.Errorf("merge got %q", s)
	}
}

func TestParseGresDetailPerNode_S(t *testing.T) {
	m := parseGresDetailPerNode(`gpu:h100:4(S:0-3)@hk01dgx001`)
	if m["hk01dgx001"] != "0,1,2,3" {
		t.Errorf("got %#v", m)
	}
}

func TestGPUIndicesFromTresBindSegment(t *testing.T) {
	if s := gpuIndicesFromTresBindSegment(`gres/gpu:0-1`); s != "0,1" {
		t.Errorf("got %q", s)
	}
	if s := gpuIndicesFromTresBindSegment(`gres:gpu:8`); s != "" {
		t.Errorf("count form got %q", s)
	}
}

func TestParseScontrolJobGRESLineExpandedNodes(t *testing.T) {
	line := `Nodes=hk01dgx[011,046] CPU_IDs=0-31,112-143 Mem=1048576 GRES=gpu:8(IDX:0-7)`
	got, ok := parseScontrolJobGRESLine(line)
	if !ok {
		t.Fatal("expected parsed GPU allocation")
	}
	if len(got) != 2 {
		t.Fatalf("got %#v", got)
	}
	if got[0].Node != "hk01dgx011" || gpuIdxIntsToCSV(got[0].GPUs) != "0,1,2,3,4,5,6,7" {
		t.Errorf("first node got %#v", got[0])
	}
	if got[1].Node != "hk01dgx046" || gpuIdxIntsToCSV(got[1].GPUs) != "0,1,2,3,4,5,6,7" {
		t.Errorf("second node got %#v", got[1])
	}
}

func TestJobGPUUtilAndMemoryMultiNodeAverages(t *testing.T) {
	snapshot := &gpuScrapeSnapshot{
		dcgm: &dcgmScrapeCache{nodes: map[string]dcgmNodeMetrics{
			"node-a": {
				util:   map[string]float64{"0": 10, "1": 20},
				memory: map[string]float64{"0": 100, "1": 200},
			},
			"node-b": {
				util:   map[string]float64{"0": 30, "1": 40},
				memory: map[string]float64{"0": 300, "1": 400},
			},
		}},
		jobAllocs: []JobGPUAlloc{{
			JobID:     "123",
			JobName:   "test-job",
			NodeList:  "node-a,node-b",
			Partition: "gpu",
			GPUs:      4,
		}},
		jobGPUs: map[string][]NodeGPUs{
			"123": {
				{Node: "node-a", GPUs: []int{0, 1}},
				{Node: "node-b", GPUs: []int{0, 1}},
			},
		},
	}

	utils := getJobGPUUtilFromSnapshot(snapshot)
	if len(utils) != 1 {
		t.Fatalf("utils got %#v", utils)
	}
	if math.Abs(utils[0].UtilPct-25) > 1e-6 {
		t.Errorf("util got %v", utils[0].UtilPct)
	}
	if utils[0].NodeList != "node-a,node-b" {
		t.Errorf("util nodelist got %q", utils[0].NodeList)
	}

	memories := getJobGPUMemoryUtilFromSnapshot(snapshot)
	if len(memories) != 1 {
		t.Fatalf("memories got %#v", memories)
	}
	if math.Abs(memories[0].MemoryMB-250) > 1e-6 {
		t.Errorf("memory got %v", memories[0].MemoryMB)
	}
	if memories[0].NodeList != "node-a,node-b" {
		t.Errorf("memory nodelist got %q", memories[0].NodeList)
	}
}

func TestParseScontrolUserId(t *testing.T) {
	if s := parseScontrolUserId("alice(1001)"); s != "alice" {
		t.Errorf("got %q", s)
	}
	if s := parseScontrolUserId("bob"); s != "bob" {
		t.Errorf("got %q", s)
	}
}

func TestParseSlurmStartTimeUnix(t *testing.T) {
	if v := parseSlurmStartTimeUnix(""); v != 0 {
		t.Errorf("empty got %v", v)
	}
	if v := parseSlurmStartTimeUnix("Unknown"); v != 0 {
		t.Errorf("unknown got %v", v)
	}
	ref := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	want := float64(ref.Unix())
	if v := parseSlurmStartTimeUnix("2024-03-15T10:30:00Z"); math.Abs(v-want) > 1 {
		t.Errorf("RFC3339 got %v want %v", v, want)
	}
	if v := parseSlurmStartTimeUnix("1739020800"); v != 1739020800 {
		t.Errorf("unix string got %v", v)
	}
	// scontrol StartTime (no timezone, local interpretation)
	refLocal := time.Date(2026, 3, 5, 17, 40, 19, 0, time.Local)
	if v := parseSlurmStartTimeUnix("2026-03-05T17:40:19"); math.Abs(v-float64(refLocal.Unix())) > 1 {
		t.Errorf("scontrol-style StartTime got %v want ~%v", v, refLocal.Unix())
	}
}

func TestJobLabelOrUnknown(t *testing.T) {
	if jobLabelOrUnknown("") != "unknown" {
		t.Fail()
	}
	if jobLabelOrUnknown("main") != "main" {
		t.Fail()
	}
}
