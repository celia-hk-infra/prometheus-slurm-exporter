package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type CompletedJobSnapshot struct {
	JobID       string
	JobName     string
	Partition   string
	User        string
	QOS         string
	State       string
	Nodes       string
	StartTime   float64
	EndTime     float64
	RuntimeSec  float64
	TotalCPUs   float64
	TotalMemMiB float64
	TotalGPUs   float64
}

type runningJobGPUState struct {
	lastSampleSec      float64
	lastUtilPct        float64
	lastMemMB          float64
	validUtil          bool
	validMem           bool
	utilIntegral       float64
	memIntegral        float64
	observedUtilSec    float64
	observedMemSec     float64
	firstUtilSampleSec float64
	firstMemSampleSec  float64
	firstUtilPct       float64
	firstMemMB         float64
	lastSeenSec        float64
}

type finalizedJobGPUState struct {
	endTimeSec float64
	utilPct    float64
	memoryMB   float64
}

type CompletedJobsCollector struct {
	info       *prometheus.Desc
	start      *prometheus.Desc
	end        *prometheus.Desc
	runtime    *prometheus.Desc
	cpus       *prometheus.Desc
	mem        *prometheus.Desc
	gpus       *prometheus.Desc
	gpuUtilAvg *prometheus.Desc
	gpuMemAvg  *prometheus.Desc

	lookback       time.Duration
	cacheTTL       time.Duration
	sampleInterval time.Duration
	states         map[string]struct{}

	mu               sync.Mutex
	running          map[string]*runningJobGPUState
	finalized        map[string]finalizedJobGPUState
	lastSampleRunSec float64
}

func NewCompletedJobsCollector(lookback, cacheTTL, sampleInterval time.Duration, terminalStates string) *CompletedJobsCollector {
	if sampleInterval <= 0 {
		sampleInterval = 30 * time.Second
	}
	c := &CompletedJobsCollector{
		info: prometheus.NewDesc("slurm_job_completed_info",
			"Metadata for terminal jobs from sacct (value 1)",
			[]string{"job_id", "job_name", "partition", "user", "qos", "state", "nodes"}, nil),
		start: prometheus.NewDesc("slurm_job_completed_start_time_seconds",
			"Unix start time for terminal jobs from sacct",
			[]string{"job_id", "partition"}, nil),
		end: prometheus.NewDesc("slurm_job_completed_end_time_seconds",
			"Unix end time for terminal jobs from sacct",
			[]string{"job_id", "partition"}, nil),
		runtime: prometheus.NewDesc("slurm_job_completed_runtime_seconds",
			"Runtime in seconds for terminal jobs from sacct",
			[]string{"job_id", "partition"}, nil),
		cpus: prometheus.NewDesc("slurm_job_completed_allocated_cpus",
			"Allocated CPU cores for terminal jobs",
			[]string{"job_id", "partition"}, nil),
		mem: prometheus.NewDesc("slurm_job_completed_allocated_memory_mib",
			"Allocated memory for terminal jobs in MiB",
			[]string{"job_id", "partition"}, nil),
		gpus: prometheus.NewDesc("slurm_job_completed_allocated_gpus",
			"Allocated GPUs for terminal jobs",
			[]string{"job_id", "partition"}, nil),
		gpuUtilAvg: prometheus.NewDesc("slurm_job_completed_gpu_utilization_avg_pct",
			"Average GPU utilization (0-100) for terminal jobs; -1 when unavailable",
			[]string{"job_id", "partition"}, nil),
		gpuMemAvg: prometheus.NewDesc("slurm_job_completed_gpu_memory_used_avg_mb",
			"Average GPU memory used (MB) for terminal jobs; -1 when unavailable",
			[]string{"job_id", "partition"}, nil),
		lookback:       lookback,
		cacheTTL:       cacheTTL,
		sampleInterval: sampleInterval,
		states:         parseTerminalStates(terminalStates),
		running:        make(map[string]*runningJobGPUState),
		finalized:      make(map[string]finalizedJobGPUState),
	}
	c.startBackgroundSampler()
	return c
}

func (c *CompletedJobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	ch <- c.start
	ch <- c.end
	ch <- c.runtime
	ch <- c.cpus
	ch <- c.mem
	ch <- c.gpus
	ch <- c.gpuUtilAvg
	ch <- c.gpuMemAvg
}

func (c *CompletedJobsCollector) Collect(ch chan<- prometheus.Metric) {
	now := float64(time.Now().Unix())
	c.sampleIfStale(now)

	jobs := ParseCompletedJobs(c.lookback, c.states)
	c.prune(now)
	for _, j := range jobs {
		partition := jobLabelOrUnknown(j.Partition)
		ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1,
			j.JobID, j.JobName, partition, j.User, j.QOS, j.State, j.Nodes)
		ch <- prometheus.MustNewConstMetric(c.start, prometheus.GaugeValue, j.StartTime, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.end, prometheus.GaugeValue, j.EndTime, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.runtime, prometheus.GaugeValue, j.RuntimeSec, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.cpus, prometheus.GaugeValue, j.TotalCPUs, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.mem, prometheus.GaugeValue, j.TotalMemMiB, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.gpus, prometheus.GaugeValue, j.TotalGPUs, j.JobID, partition)

		utilAvg, memAvg := c.lookupOrFinalizeGPUAverages(j, now)
		ch <- prometheus.MustNewConstMetric(c.gpuUtilAvg, prometheus.GaugeValue, utilAvg, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.gpuMemAvg, prometheus.GaugeValue, memAvg, j.JobID, partition)
	}
}

func parseTerminalStates(v string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, s := range strings.Split(v, ",") {
		s = normalizeTerminalState(s)
		if s != "" {
			out[s] = struct{}{}
		}
	}
	return out
}

func normalizeTerminalState(s string) string {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return ""
	}
	if i := strings.IndexAny(s, " +("); i > 0 {
		s = s[:i]
	}
	return s
}

func ParseCompletedJobs(lookback time.Duration, terminalStates map[string]struct{}) []CompletedJobSnapshot {
	var jobs []CompletedJobSnapshot
	start := time.Now().Add(-lookback).Format("2006-01-02T15:04:05")
	args := []string{
		"-a", "-X", "--noheader", "--parsable2",
		"--format=JobIDRaw,JobName,Partition,User,QOS,State,NodeList,Start,End,ElapsedRaw,AllocCPUS,AllocTRES,ReqMem",
		"-S", start,
	}
	// Do not pass --state here. Some Slurm setups accept only short state codes for --state,
	// while we accept full names in config (COMPLETED, FAILED, ...). Filter in parser instead.
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	out, err := execute("sacct", args)
	if err != nil {
		log.Warnf("sacct failed (completed jobs): %v", err)
		return jobs
	}
	return parseCompletedJobsOutput(string(out), terminalStates)
}

func parseCompletedJobsOutput(raw string, terminalStates map[string]struct{}) []CompletedJobSnapshot {
	byKey := make(map[string]CompletedJobSnapshot)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if j, ok := parseCompletedJobLine(line, terminalStates); ok {
			key := j.JobID + "|" + j.Partition
			if prev, exists := byKey[key]; !exists || j.EndTime >= prev.EndTime {
				byKey[key] = j
			}
		}
	}
	jobs := make([]CompletedJobSnapshot, 0, len(byKey))
	for _, j := range byKey {
		jobs = append(jobs, j)
	}
	return jobs
}

func parseCompletedJobLine(line string, terminalStates map[string]struct{}) (CompletedJobSnapshot, bool) {
	parts := strings.Split(line, "|")
	if len(parts) < 13 {
		return CompletedJobSnapshot{}, false
	}
	jobID := normalizeSqueueJobID(sanitizeLabel(strings.Trim(parts[0], `"`)))
	if jobID == "" {
		return CompletedJobSnapshot{}, false
	}
	state := normalizeTerminalState(strings.Trim(parts[5], `"`))
	if state == "" {
		return CompletedJobSnapshot{}, false
	}
	if len(terminalStates) > 0 {
		if _, ok := terminalStates[state]; !ok {
			return CompletedJobSnapshot{}, false
		}
	}
	startRaw := strings.Trim(parts[7], `"`)
	endRaw := strings.Trim(parts[8], `"`)
	endSec := parseSlurmStartTimeUnix(endRaw)
	if endSec <= 0 {
		return CompletedJobSnapshot{}, false
	}
	startSec := parseSlurmStartTimeUnix(startRaw)
	elapsedRaw := strings.Trim(parts[9], `"`)
	runtimeSec, _ := strconv.ParseFloat(elapsedRaw, 64)
	if runtimeSec <= 0 && startSec > 0 && endSec >= startSec {
		runtimeSec = endSec - startSec
	}
	allocCPUS, _ := strconv.ParseFloat(strings.Trim(parts[10], `"`), 64)
	allocTRES := strings.Trim(parts[11], `"`)
	reqMem := strings.Trim(parts[12], `"`)
	_, memMiB, gpus := parseTRESAllocFields(allocTRES)
	if memMiB <= 0 {
		memMiB = parseReqMemPerNodeMiB(reqMem, 1)
	}
	nodes := sanitizeLabel(strings.Trim(parts[6], `"`))
	if nodes == "" {
		nodes = "unknown"
	}
	if len(nodes) > 2000 {
		nodes = nodes[:2000]
	}
	return CompletedJobSnapshot{
		JobID:       jobLabelOrUnknown(jobID),
		JobName:     jobLabelOrUnknown(sanitizeLabel(strings.Trim(parts[1], `"`))),
		Partition:   jobLabelOrUnknown(sanitizeLabel(strings.Trim(parts[2], `"`))),
		User:        jobLabelOrUnknown(sanitizeLabel(strings.Trim(parts[3], `"`))),
		QOS:         jobLabelOrUnknown(sanitizeLabel(strings.Trim(parts[4], `"`))),
		State:       state,
		Nodes:       nodes,
		StartTime:   startSec,
		EndTime:     endSec,
		RuntimeSec:  runtimeSec,
		TotalCPUs:   allocCPUS,
		TotalMemMiB: memMiB,
		TotalGPUs:   gpus,
	}, true
}

func (c *CompletedJobsCollector) captureRunningGPUState(now float64) {
	snapshot := newGPUScrapeSnapshot()
	utilByJob := make(map[string]float64)
	memByJob := make(map[string]float64)
	for _, v := range getJobGPUUtilFromSnapshot(snapshot) {
		utilByJob[jobLabelOrUnknown(normalizeSqueueJobID(v.JobID))] = v.UtilPct
	}
	for _, v := range getJobGPUMemoryUtilFromSnapshot(snapshot) {
		memByJob[jobLabelOrUnknown(normalizeSqueueJobID(v.JobID))] = v.MemoryMB
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	active := make(map[string]struct{})
	for jobID, util := range utilByJob {
		s := c.running[jobID]
		if s == nil {
			s = &runningJobGPUState{}
			c.running[jobID] = s
		}
		c.advanceRunningState(s, now)
		s.lastUtilPct = util
		s.validUtil = util >= 0
		if s.validUtil && s.firstUtilSampleSec <= 0 {
			s.firstUtilSampleSec = now
			s.firstUtilPct = util
		}
		if mem, ok := memByJob[jobID]; ok {
			s.lastMemMB = mem
			s.validMem = mem >= 0
			if s.validMem && s.firstMemSampleSec <= 0 {
				s.firstMemSampleSec = now
				s.firstMemMB = mem
			}
		} else {
			s.validMem = false
		}
		s.lastSampleSec = now
		s.lastSeenSec = now
		active[jobID] = struct{}{}
	}
	for jobID, mem := range memByJob {
		if _, ok := active[jobID]; ok {
			continue
		}
		s := c.running[jobID]
		if s == nil {
			s = &runningJobGPUState{}
			c.running[jobID] = s
		}
		c.advanceRunningState(s, now)
		s.lastMemMB = mem
		s.validMem = mem >= 0
		if s.validMem && s.firstMemSampleSec <= 0 {
			s.firstMemSampleSec = now
			s.firstMemMB = mem
		}
		s.validUtil = false
		s.lastSampleSec = now
		s.lastSeenSec = now
	}
	c.lastSampleRunSec = now
}

func (c *CompletedJobsCollector) advanceRunningState(s *runningJobGPUState, now float64) {
	if s.lastSampleSec <= 0 {
		return
	}
	dt := now - s.lastSampleSec
	if dt <= 0 {
		return
	}
	if s.validUtil {
		s.utilIntegral += s.lastUtilPct * dt
		s.observedUtilSec += dt
	}
	if s.validMem {
		s.memIntegral += s.lastMemMB * dt
		s.observedMemSec += dt
	}
}

func (c *CompletedJobsCollector) lookupOrFinalizeGPUAverages(j CompletedJobSnapshot, now float64) (float64, float64) {
	if j.TotalGPUs <= 0 {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if f, ok := c.finalized[j.JobID]; ok && math.Abs(f.endTimeSec-j.EndTime) < 1 {
		return f.utilPct, f.memoryMB
	}

	s := c.running[j.JobID]
	if s == nil {
		return -1, -1
	}
	utilInt := s.utilIntegral
	memInt := s.memIntegral
	utilObs := s.observedUtilSec
	memObs := s.observedMemSec
	if s.lastSampleSec > 0 && j.EndTime > s.lastSampleSec {
		dt := j.EndTime - s.lastSampleSec
		if dt > 0 {
			if s.validUtil {
				utilInt += s.lastUtilPct * dt
				utilObs += dt
			}
			if s.validMem {
				memInt += s.lastMemMB * dt
				memObs += dt
			}
		}
	}
	// Backfill from known job start to first observed sample to better approximate
	// whole-execution average when the first DCGM sample arrives after start.
	if j.StartTime > 0 {
		if s.firstUtilSampleSec > j.StartTime {
			dt := s.firstUtilSampleSec - j.StartTime
			utilInt += s.firstUtilPct * dt
			utilObs += dt
		}
		if s.firstMemSampleSec > j.StartTime {
			dt := s.firstMemSampleSec - j.StartTime
			memInt += s.firstMemMB * dt
			memObs += dt
		}
	}
	utilAvg := -1.0
	if utilObs > 0 {
		utilAvg = utilInt / utilObs
	}
	memAvg := -1.0
	if memObs > 0 {
		memAvg = memInt / memObs
	}
	c.finalized[j.JobID] = finalizedJobGPUState{
		endTimeSec: j.EndTime,
		utilPct:    utilAvg,
		memoryMB:   memAvg,
	}
	return utilAvg, memAvg
}

func (c *CompletedJobsCollector) prune(now float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cacheTTL <= 0 {
		return
	}
	cutoff := now - c.cacheTTL.Seconds()
	for k, v := range c.running {
		if v.lastSeenSec > 0 && v.lastSeenSec < cutoff {
			delete(c.running, k)
		}
	}
	for k, v := range c.finalized {
		if v.endTimeSec > 0 && v.endTimeSec < cutoff {
			delete(c.finalized, k)
		}
	}
}

func parseDurationOrDefault(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Warnf("invalid duration %q: %v; using %s", raw, err, fallback)
		return fallback
	}
	return d
}

func validateCompletedJobsConfig(lookback, cacheTTL, sampleInterval time.Duration) error {
	if lookback <= 0 {
		return fmt.Errorf("lookback must be > 0")
	}
	if cacheTTL <= 0 {
		return fmt.Errorf("cache ttl must be > 0")
	}
	if sampleInterval <= 0 {
		return fmt.Errorf("sample interval must be > 0")
	}
	return nil
}

func (c *CompletedJobsCollector) startBackgroundSampler() {
	go func() {
		c.captureRunningGPUState(float64(time.Now().Unix()))
		ticker := time.NewTicker(c.sampleInterval)
		defer ticker.Stop()
		for range ticker.C {
			c.captureRunningGPUState(float64(time.Now().Unix()))
		}
	}()
}

func (c *CompletedJobsCollector) sampleIfStale(now float64) {
	c.mu.Lock()
	last := c.lastSampleRunSec
	c.mu.Unlock()
	if last <= 0 || now-last >= (2*c.sampleInterval.Seconds()) {
		c.captureRunningGPUState(now)
	}
}
