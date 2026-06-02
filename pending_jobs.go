package main

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type PendingJobSnapshot struct {
	JobID             string
	Partition         string
	JobName           string
	User              string
	Reason            string
	ReqCPUs           float64
	ReqMemMiB         float64
	ReqGPUs           float64
	QOS               string
	RequestedNode     string
	SubmitTimeUnix    float64
	ExpectedStartUnix float64
}

type pendingScontrolMeta struct {
	partition         string
	jobName           string
	user              string
	reason            string
	qos               string
	requestedNode     string
	reqCPUs           float64 // CPUs per GPU
	reqMemMiB         float64 // memory per CPU in MiB
	reqGPUs           float64
	submitTimeUnix    float64
	expectedStartUnix float64
}

type PendingJobsCollector struct {
	info          *prometheus.Desc
	reqCPUs       *prometheus.Desc
	reqMemMiB     *prometheus.Desc
	reqGPUs       *prometheus.Desc
	submitTime    *prometheus.Desc
	expectedStart *prometheus.Desc
}

func NewPendingJobsCollector() *PendingJobsCollector {
	return &PendingJobsCollector{
		info: prometheus.NewDesc("slurm_pending_job_info",
			"Metadata for currently pending jobs (value 1)",
			[]string{"job_id", "job_name", "partition", "user", "qos", "reason", "requested_node", "state"}, nil),
		reqCPUs: prometheus.NewDesc("slurm_pending_job_req_cpus",
			"Requested CPU cores per GPU for pending jobs",
			[]string{"job_id", "partition"}, nil),
		reqMemMiB: prometheus.NewDesc("slurm_pending_job_req_memory_mib",
			"Requested memory per CPU for pending jobs in MiB",
			[]string{"job_id", "partition"}, nil),
		reqGPUs: prometheus.NewDesc("slurm_pending_job_req_gpus",
			"Requested GPUs for pending jobs",
			[]string{"job_id", "partition"}, nil),
		submitTime: prometheus.NewDesc("slurm_pending_job_submit_time_seconds",
			"Pending job submit time as Unix seconds",
			[]string{"job_id", "partition"}, nil),
		expectedStart: prometheus.NewDesc("slurm_pending_job_expected_start_time_seconds",
			"Pending job expected start time as Unix seconds; 0 when unknown",
			[]string{"job_id", "partition"}, nil),
	}
}

func (c *PendingJobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	ch <- c.reqCPUs
	ch <- c.reqMemMiB
	ch <- c.reqGPUs
	ch <- c.submitTime
	ch <- c.expectedStart
}

func (c *PendingJobsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, j := range ParsePendingJobs() {
		partition := jobLabelOrUnknown(j.Partition)
		ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1,
			j.JobID, jobLabelOrUnknown(j.JobName), partition, jobLabelOrUnknown(j.User),
			jobLabelOrUnknown(j.QOS), jobLabelOrUnknown(j.Reason), pendingRequestedNodeLabel(j.RequestedNode), "pending")
		ch <- prometheus.MustNewConstMetric(c.reqCPUs, prometheus.GaugeValue, j.ReqCPUs, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.reqMemMiB, prometheus.GaugeValue, j.ReqMemMiB, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.reqGPUs, prometheus.GaugeValue, j.ReqGPUs, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.submitTime, prometheus.GaugeValue, j.SubmitTimeUnix, j.JobID, partition)
		ch <- prometheus.MustNewConstMetric(c.expectedStart, prometheus.GaugeValue, j.ExpectedStartUnix, j.JobID, partition)
	}
}

func ParsePendingJobs() []PendingJobSnapshot {
	args := []string{"-h", "-t", "PD", "--noheader", "-o", "%A|%P|%j|%u|%r|%C|%m|%q|%N|%V|%S"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	out, err := execute("squeue", args)
	if err != nil {
		log.Warnf("squeue failed (pending jobs): %v", err)
		return nil
	}
	jobs := parsePendingJobsOutput(string(out))
	meta := parsePendingScontrolOutput(fetchPendingJobsScontrolRaw())
	return mergePendingWithScontrol(jobs, meta)
}

func fetchPendingJobsScontrolRaw() string {
	out, err := execute("scontrol", []string{"show", "job", "-o"})
	if err != nil {
		log.Warnf("scontrol failed (pending jobs metadata): %v", err)
		return ""
	}
	return string(out)
}

func parsePendingJobsOutput(raw string) []PendingJobSnapshot {
	byKey := make(map[string]PendingJobSnapshot)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		j, ok := parsePendingJobLine(line)
		if !ok {
			continue
		}
		key := j.JobID + "|" + j.Partition
		if prev, exists := byKey[key]; !exists || j.SubmitTimeUnix >= prev.SubmitTimeUnix {
			byKey[key] = j
		}
	}
	out := make([]PendingJobSnapshot, 0, len(byKey))
	for _, j := range byKey {
		out = append(out, j)
	}
	return out
}

func parsePendingJobLine(line string) (PendingJobSnapshot, bool) {
	parts := strings.Split(line, "|")
	if len(parts) < 11 {
		return PendingJobSnapshot{}, false
	}
	jobID := strings.TrimSpace(strings.Trim(parts[0], `"`))
	if jobID == "" || !shouldIncludeSqueueLine(jobID) {
		return PendingJobSnapshot{}, false
	}
	baseID := normalizeSqueueJobID(jobID)
	part := normalizePartitionLabel(parts[1])
	if !jobPartitionMatchesFilter(part, gpuPartitionsFilter) {
		return PendingJobSnapshot{}, false
	}
	// squeue does not reliably expose cpus-per-gpu; keep 0 and let scontrol override.
	memPerCPU := parseReqMemPerCPUMiB(strings.TrimSpace(strings.Trim(parts[6], `"`)))
	return PendingJobSnapshot{
		JobID:             baseID,
		Partition:         part,
		JobName:           cleanSlurmField(parts[2]),
		User:              cleanSlurmField(parts[3]),
		Reason:            cleanSlurmField(parts[4]),
		ReqCPUs:           0,
		ReqMemMiB:         memPerCPU,
		ReqGPUs:           0,
		QOS:               cleanSlurmField(parts[7]),
		RequestedNode:     cleanSlurmField(parts[8]),
		SubmitTimeUnix:    parseSlurmStartTimeUnix(strings.TrimSpace(strings.Trim(parts[9], `"`))),
		ExpectedStartUnix: parseSlurmStartTimeUnix(strings.TrimSpace(strings.Trim(parts[10], `"`))),
	}, true
}

func normalizePartitionLabel(raw string) string {
	p := cleanSlurmField(raw)
	p = strings.TrimSuffix(p, "*")
	return p
}

func cleanSlurmField(raw string) string {
	s := strings.TrimSpace(strings.Trim(raw, `"`))
	if s == "" || strings.EqualFold(s, "N/A") || strings.EqualFold(s, "(null)") || strings.EqualFold(s, "None") || strings.EqualFold(s, "Unknown") {
		return ""
	}
	return sanitizeLabel(s)
}

func pendingRequestedNodeLabel(s string) string {
	s = strings.TrimSpace(sanitizeLabel(s))
	if s == "" {
		return "none"
	}
	return s
}

func parsePendingScontrolOutput(raw string) map[string]pendingScontrolMeta {
	out := make(map[string]pendingScontrolMeta)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		kv := parseScontrolKeyValues(line)
		state := normalizeTerminalState(kv["JobState"])
		if state != "PENDING" {
			continue
		}
		jobID := baseJobIDKey(strings.TrimSpace(kv["JobId"]))
		if jobID == "" {
			continue
		}
		meta := pendingScontrolMeta{
			partition:         normalizePartitionLabel(kv["Partition"]),
			jobName:           cleanSlurmField(kv["JobName"]),
			user:              parseScontrolUserId(kv["UserId"]),
			reason:            cleanSlurmField(kv["Reason"]),
			qos:               cleanSlurmField(kv["QOS"]),
			requestedNode:     cleanSlurmField(kv["ReqNodeList"]),
			reqGPUs:           parsePendingReqGPUs(kv),
			reqCPUs:           parsePendingCPUsPerGPU(kv),
			reqMemMiB:         parsePendingReqMemPerCPUMiB(kv),
			submitTimeUnix:    parseSlurmStartTimeUnix(kv["SubmitTime"]),
			expectedStartUnix: parseSlurmStartTimeUnix(kv["StartTime"]),
		}
		if !jobPartitionMatchesFilter(meta.partition, gpuPartitionsFilter) {
			continue
		}
		if prev, ok := out[jobID]; !ok || preferPendingMeta(meta, prev) {
			out[jobID] = meta
		}
	}
	return out
}

func parseFloatOrZero(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(strings.Trim(s, `"`)), 64)
	if err != nil {
		return 0
	}
	return v
}

func parsePendingReqMemPerCPUMiB(kv map[string]string) float64 {
	if v := parseSlurmMemToMiB(strings.TrimSpace(kv["MinMemoryCPU"])); v > 0 {
		return v
	}
	// ReqMem suffix 'c' means per CPU.
	if v := parseReqMemPerCPUMiB(strings.TrimSpace(kv["ReqMem"])); v > 0 {
		return v
	}
	// Fallback: derive per-CPU from total requested memory and NumCPUs.
	totalCPUs := parseFloatOrZero(kv["NumCPUs"])
	if _, totalMemMiB, _ := parseTRESAllocFields(strings.TrimSpace(kv["ReqTRES"])); totalMemMiB > 0 && totalCPUs > 0 {
		return totalMemMiB / totalCPUs
	}
	return 0
}

func parseReqMemPerCPUMiB(req string) float64 {
	req = strings.TrimSpace(req)
	if req == "" || strings.EqualFold(req, "N/A") {
		return 0
	}
	low := strings.ToLower(req)
	if strings.HasSuffix(low, "c") && len(req) > 1 {
		return parseSlurmMemToMiB(strings.TrimSpace(req[:len(req)-1]))
	}
	return 0
}

var reGPUCount = regexp.MustCompile(`(?:gres/gpu[:=]|gres:gpu:)([0-9]+(?:\.[0-9]+)?)`)
var reCPUsPerTresGPU = regexp.MustCompile(`(?:^|,)gres/gpu[:=]([0-9]+(?:\.[0-9]+)?)`)

func parsePendingReqGPUs(kv map[string]string) float64 {
	candidates := []string{
		kv["ReqTRES"],
		kv["TresPerJob"],
		kv["TRESPerJob"],
		kv["TresPerNode"],
		kv["TRESPerNode"],
	}
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		normalized := strings.ReplaceAll(c, "gres/gpu:", "gres/gpu=")
		normalized = strings.ReplaceAll(normalized, "gres:gpu:", "gres/gpu=")
		if _, _, g := parseTRESAllocFields(normalized); g > 0 {
			return g
		}
		if m := reGPUCount.FindStringSubmatch(c); len(m) >= 2 {
			if v, err := strconv.ParseFloat(m[1], 64); err == nil {
				return v
			}
		}
	}
	return 0
}

func parsePendingCPUsPerGPU(kv map[string]string) float64 {
	for _, key := range []string{"CpusPerTres", "CPUsPerTres", "CpusPerTRES", "CPUsPerTRES"} {
		raw := strings.TrimSpace(kv[key])
		if raw == "" {
			continue
		}
		normalized := strings.ReplaceAll(raw, "gres:gpu:", "gres/gpu=")
		if m := reCPUsPerTresGPU.FindStringSubmatch(normalized); len(m) >= 2 {
			if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
				return v
			}
		}
	}
	for _, key := range []string{"CpusPerGPU", "CPUsPerGPU"} {
		if v := parseFloatOrZero(kv[key]); v > 0 {
			return v
		}
	}
	reqGPU := parsePendingReqGPUs(kv)
	totalCPUs := parseFloatOrZero(kv["NumCPUs"])
	if reqGPU > 0 && totalCPUs > 0 {
		return totalCPUs / reqGPU
	}
	return 0
}

func preferPendingMeta(cur, prev pendingScontrolMeta) bool {
	if cur.submitTimeUnix > prev.submitTimeUnix {
		return true
	}
	if cur.expectedStartUnix > prev.expectedStartUnix {
		return true
	}
	return false
}

func mergePendingWithScontrol(rows []PendingJobSnapshot, meta map[string]pendingScontrolMeta) []PendingJobSnapshot {
	out := make([]PendingJobSnapshot, 0, len(rows))
	seen := make(map[string]struct{})
	for _, j := range rows {
		if m, ok := meta[j.JobID]; ok {
			j = applyPendingMeta(j, m)
		}
		out = append(out, j)
		seen[j.JobID] = struct{}{}
	}
	// Include jobs that appeared only in scontrol output.
	for jobID, m := range meta {
		if _, ok := seen[jobID]; ok {
			continue
		}
		out = append(out, applyPendingMeta(PendingJobSnapshot{JobID: jobID}, m))
	}
	return out
}

func applyPendingMeta(j PendingJobSnapshot, m pendingScontrolMeta) PendingJobSnapshot {
	if j.Partition == "" {
		j.Partition = m.partition
	}
	if j.JobName == "" {
		j.JobName = m.jobName
	}
	if j.User == "" {
		j.User = m.user
	}
	if j.Reason == "" {
		j.Reason = m.reason
	}
	if j.QOS == "" {
		j.QOS = m.qos
	}
	if j.RequestedNode == "" {
		j.RequestedNode = m.requestedNode
	}
	if m.reqCPUs > 0 {
		j.ReqCPUs = m.reqCPUs
	}
	if m.reqMemMiB > 0 {
		j.ReqMemMiB = m.reqMemMiB
	}
	if m.reqGPUs > 0 {
		j.ReqGPUs = m.reqGPUs
	}
	if j.SubmitTimeUnix <= 0 {
		j.SubmitTimeUnix = m.submitTimeUnix
	}
	if j.ExpectedStartUnix <= 0 {
		j.ExpectedStartUnix = m.expectedStartUnix
	}
	return j
}
