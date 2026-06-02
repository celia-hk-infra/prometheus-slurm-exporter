package main

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/log"
)

func TestNormalizeTerminalState(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"completed", "COMPLETED"},
		{"CANCELLED by 1001", "CANCELLED"},
		{"FAILED+", "FAILED"},
		{" TIMEOUT ", "TIMEOUT"},
	}
	for _, tt := range tests {
		got := normalizeTerminalState(tt.in)
		if got != tt.want {
			t.Fatalf("normalizeTerminalState(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseCompletedJobLine(t *testing.T) {
	states := parseTerminalStates("COMPLETED,FAILED")
	line := "12345|test-job|gpu|alice|normal|COMPLETED|gpu001|2026-03-23T01:00:00|2026-03-23T02:30:00|5400|8|cpu=8,mem=64G,gres/gpu=2|64Gn"
	job, ok := parseCompletedJobLine(line, states)
	if !ok {
		t.Fatalf("expected parsed completed job")
	}
	if job.JobID != "12345" {
		t.Fatalf("job id = %q", job.JobID)
	}
	if job.State != "COMPLETED" {
		t.Fatalf("state = %q", job.State)
	}
	if job.TotalCPUs != 8 {
		t.Fatalf("cpus = %v", job.TotalCPUs)
	}
	if job.TotalGPUs != 2 {
		t.Fatalf("gpus = %v", job.TotalGPUs)
	}
	if math.Abs(job.TotalMemMiB-(64*1024)) > 1e-6 {
		t.Fatalf("mem = %v", job.TotalMemMiB)
	}
	if math.Abs(job.RuntimeSec-5400) > 1e-6 {
		t.Fatalf("runtime = %v", job.RuntimeSec)
	}
}

func TestParseCompletedJobsOutputFiltersState(t *testing.T) {
	states := parseTerminalStates("COMPLETED")
	raw := "" +
		"12345|ok|gpu|alice|normal|COMPLETED|gpu001|2026-03-23T01:00:00|2026-03-23T01:10:00|600|2|cpu=2,mem=4G,gres/gpu=1|4Gn\n" +
		"12346|bad|gpu|alice|normal|FAILED|gpu001|2026-03-23T01:00:00|2026-03-23T01:10:00|600|2|cpu=2,mem=4G,gres/gpu=1|4Gn\n"
	jobs := parseCompletedJobsOutput(raw, states)
	if len(jobs) != 1 {
		t.Fatalf("len = %d", len(jobs))
	}
	if jobs[0].JobID != "12345" {
		t.Fatalf("job id = %q", jobs[0].JobID)
	}
}

func TestParseDurationOrDefault(t *testing.T) {
	fallback := 24 * time.Hour
	if got := parseDurationOrDefault("2h", fallback); got != 2*time.Hour {
		t.Fatalf("got %s", got)
	}
	if err := log.Base().SetLevel("error"); err != nil {
		t.Fatalf("set log level: %v", err)
	}
	defer log.Base().SetLevel("info")
	if got := parseDurationOrDefault("bad-value", fallback); got != fallback {
		t.Fatalf("got %s", got)
	}
}
