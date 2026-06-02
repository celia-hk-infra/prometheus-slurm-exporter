package main

import (
	"math"
	"testing"
)

func TestParsePendingJobLine(t *testing.T) {
	line := "12345|gpu*|train-a100|alice|Resources|32|64G|normal|gpu001|2026-03-23T01:00:00|2026-03-23T04:30:00"
	j, ok := parsePendingJobLine(line)
	if !ok {
		t.Fatalf("expected valid pending line")
	}
	if j.JobID != "12345" {
		t.Fatalf("job id = %q", j.JobID)
	}
	if j.Partition != "gpu" {
		t.Fatalf("partition = %q", j.Partition)
	}
	if j.JobName != "train-a100" {
		t.Fatalf("job name = %q", j.JobName)
	}
	if j.User != "alice" {
		t.Fatalf("user = %q", j.User)
	}
	if j.Reason != "Resources" {
		t.Fatalf("reason = %q", j.Reason)
	}
	if j.ReqCPUs != 0 {
		t.Fatalf("cpus-per-gpu = %v", j.ReqCPUs)
	}
	if math.Abs(j.ReqMemMiB-0) > 1e-6 {
		t.Fatalf("mem-per-cpu mib = %v", j.ReqMemMiB)
	}
	if j.ReqGPUs != 0 {
		t.Fatalf("gpus = %v", j.ReqGPUs)
	}
	if j.QOS != "normal" {
		t.Fatalf("qos = %q", j.QOS)
	}
	if j.RequestedNode != "gpu001" {
		t.Fatalf("requested node = %q", j.RequestedNode)
	}
	if j.SubmitTimeUnix <= 0 {
		t.Fatalf("submit time = %v", j.SubmitTimeUnix)
	}
	if j.ExpectedStartUnix <= 0 {
		t.Fatalf("start time = %v", j.ExpectedStartUnix)
	}
}

func TestParsePendingScontrolOutput(t *testing.T) {
	raw := "JobId=12345 JobName=train-a100 UserId=alice(1001) JobState=PENDING Reason=Priority Partition=gpu QOS=normal ReqNodeList=gpu[001-002] NumCPUs=128 CpusPerTres=gres:gpu:16 MinMemoryCPU=8G ReqTRES=cpu=128,mem=1024G,node=1,billing=128,gres/gpu=8 SubmitTime=2026-03-23T02:00:00 StartTime=2026-03-23T03:00:00\n" +
		"JobId=12346 JobName=test UserId=bob(1002) JobState=RUNNING Reason=None Partition=cpu QOS=normal ReqNodeList=(null) NumCPUs=1 ReqTRES=cpu=1,mem=1G,node=1 SubmitTime=2026-03-23T02:00:00 StartTime=2026-03-23T03:00:00\n"
	m := parsePendingScontrolOutput(raw)
	if len(m) != 1 {
		t.Fatalf("len = %d", len(m))
	}
	j := m["12345"]
	if j.partition != "gpu" {
		t.Fatalf("partition = %q", j.partition)
	}
	if j.user != "alice" {
		t.Fatalf("user = %q", j.user)
	}
	if j.reason != "Priority" {
		t.Fatalf("reason = %q", j.reason)
	}
	if j.reqCPUs != 16 {
		t.Fatalf("cpus-per-gpu = %v", j.reqCPUs)
	}
	if math.Abs(j.reqMemMiB-8192) > 1e-6 {
		t.Fatalf("mem-per-cpu mib = %v", j.reqMemMiB)
	}
	if j.reqGPUs != 8 {
		t.Fatalf("gpus = %v", j.reqGPUs)
	}
	if j.submitTimeUnix <= 0 || j.expectedStartUnix <= 0 {
		t.Fatalf("times submit=%v start=%v", j.submitTimeUnix, j.expectedStartUnix)
	}
}

func TestMergePendingWithScontrol(t *testing.T) {
	rows := []PendingJobSnapshot{
		{
			JobID:     "12345",
			Partition: "gpu",
			JobName:   "train-a100",
			User:      "alice",
			Reason:    "Resources",
			ReqCPUs:   32,
			ReqMemMiB: 65536,
		},
	}
	meta := map[string]pendingScontrolMeta{
		"12345": {
			qos:           "normal",
			requestedNode: "gpu[001-002]",
			reqGPUs:       4,
		},
		"12346": {
			partition: "cpu",
			jobName:   "other",
			user:      "bob",
			reason:    "Priority",
			qos:       "normal",
			reqCPUs:   8,
		},
	}
	out := mergePendingWithScontrol(rows, meta)
	if len(out) != 2 {
		t.Fatalf("len = %d", len(out))
	}
	var j1, j2 *PendingJobSnapshot
	for i := range out {
		if out[i].JobID == "12345" {
			j1 = &out[i]
		}
		if out[i].JobID == "12346" {
			j2 = &out[i]
		}
	}
	if j1 == nil || j2 == nil {
		t.Fatalf("missing expected jobs: %#v", out)
	}
	if j1.QOS != "normal" {
		t.Fatalf("qos = %q", j1.QOS)
	}
	if j1.RequestedNode != "gpu[001-002]" {
		t.Fatalf("requested node = %q", j1.RequestedNode)
	}
	if j1.ReqGPUs != 4 {
		t.Fatalf("gpus = %v", j1.ReqGPUs)
	}
	if j2.Partition != "cpu" || j2.User != "bob" {
		t.Fatalf("merged scontrol-only row mismatch: %#v", *j2)
	}
}

func TestParsePendingReqGPUsTresPerNodeStyle(t *testing.T) {
	kv := map[string]string{
		"TresPerNode": "gres:gpu:8",
	}
	if g := parsePendingReqGPUs(kv); g != 8 {
		t.Fatalf("gpus = %v", g)
	}
}

func TestParsePendingReqMemPerCPUMiB_FromReqMemSuffixC(t *testing.T) {
	kv := map[string]string{
		"ReqMem":  "4Gc",
		"NumCPUs": "32",
	}
	if v := parsePendingReqMemPerCPUMiB(kv); math.Abs(v-4096) > 1e-6 {
		t.Fatalf("mem-per-cpu mib = %v", v)
	}
}

func TestParsePendingCPUsPerGPU_FromCpusPerTres(t *testing.T) {
	kv := map[string]string{
		"CpusPerTres": "gres:gpu:16",
		"NumCPUs":     "128",
		"ReqTRES":     "cpu=128,gres/gpu=8",
	}
	if v := parsePendingCPUsPerGPU(kv); v != 16 {
		t.Fatalf("cpus-per-gpu = %v", v)
	}
}

