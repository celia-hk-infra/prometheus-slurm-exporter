/* Copyright 2025

Per-node running job allocation metrics (job id, name, CPUs, memory, GPUs, GPU indices).
Data from squeue (running) merged with sacct AllocTRES for GPU totals.

When a job spans multiple nodes, CPUs and GPUs are split evenly across expanded
nodelist unless squeue TRES bind uses | to separate per-node GPU bindings.
*/

package main

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

// NodeJobRow is one running job's resource attribution on a single node.
type NodeJobRow struct {
	Node      string
	JobID     string
	JobName   string
	CPUs      float64
	MemoryMiB float64
	GPUs      float64
	GPUIdx    string // comma-separated GPU indices on this node, or empty
}

// parseSqueueMinMemoryMiB parses squeue %m (min memory per node). Plain number = MiB; supports G/M/T suffix.
func parseSqueueMinMemoryMiB(s string) float64 {
	s = strings.TrimSpace(strings.Trim(s, `"`))
	if s == "" || strings.EqualFold(s, "N/A") {
		return 0
	}
	upper := strings.ToUpper(s)
	var mul float64 = 1
	if strings.HasSuffix(upper, "T") {
		mul = 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-1])
	} else if strings.HasSuffix(upper, "G") {
		mul = 1024
		s = strings.TrimSpace(s[:len(s)-1])
	} else if strings.HasSuffix(upper, "M") {
		mul = 1
		s = strings.TrimSpace(s[:len(s)-1])
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v * mul
}

// parseTRESAllocFields extracts cpu, mem (MiB), and gres/gpu from AllocTRES / TRES style strings.
func parseTRESAllocFields(tres string) (cpu float64, memMiB float64, gpu float64) {
	tres = strings.TrimSpace(tres)
	for _, part := range strings.Split(tres, ",") {
		part = strings.TrimSpace(strings.Trim(part, `"`))
		if strings.HasPrefix(part, "cpu=") {
			s := strings.TrimPrefix(part, "cpu=")
			if n, err := strconv.ParseFloat(s, 64); err == nil {
				cpu = n
			}
			continue
		}
		if strings.HasPrefix(part, "mem=") {
			memMiB = parseSlurmMemToMiB(strings.TrimPrefix(part, "mem="))
			continue
		}
		if strings.HasPrefix(part, "gres/gpu=") {
			s := strings.TrimPrefix(part, "gres/gpu=")
			if n, err := strconv.ParseFloat(s, 64); err == nil {
				gpu = n
			}
		}
	}
	return cpu, memMiB, gpu
}

func parseAllocTRESGPUs(tres string) float64 {
	_, _, g := parseTRESAllocFields(tres)
	return g
}

// parseSlurmMemToMiB parses Slurm memory like "409600M", "400G", "16T".
func parseSlurmMemToMiB(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	upper := strings.ToUpper(s)
	var mul float64 = 1
	if strings.HasSuffix(upper, "N") && len(s) > 1 {
		// per-node suffix sometimes appears; strip and treat number as per-node
		s = strings.TrimSpace(s[:len(s)-1])
		upper = strings.ToUpper(s)
	}
	if strings.HasSuffix(upper, "T") {
		mul = 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-1])
	} else if strings.HasSuffix(upper, "G") {
		mul = 1024
		s = strings.TrimSpace(s[:len(s)-1])
	} else if strings.HasSuffix(upper, "M") {
		mul = 1
		s = strings.TrimSpace(s[:len(s)-1])
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v * mul
}

// sacctRunningJobAlloc is accounting data for a running job (when scontrol steps show 0 CPUs).
type sacctRunningJobAlloc struct {
	allocCPUS float64
	allocTRES string
	reqMem    string
}

type runningJobSacctSnapshot struct {
	allocs map[string]sacctRunningJobAlloc
	gpus   map[string]*sacctGPUJob
}

func newRunningJobSacctSnapshot() runningJobSacctSnapshot {
	snapshot := runningJobSacctSnapshot{
		allocs: make(map[string]sacctRunningJobAlloc),
		gpus:   make(map[string]*sacctGPUJob),
	}
	args := []string{"-a", "-X", "--format=JobId,JobName,NodeList,Partition,AllocCPUS,AllocTRES,ReqMem", "--state=RUNNING", "--noheader", "--parsable2"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	b, err := execute("sacct", args)
	if err != nil {
		log.Warnf("sacct failed (node job allocation lookup): %v", err)
		return snapshot
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 7)
		if len(parts) < 7 {
			continue
		}
		jid := strings.TrimSpace(strings.Trim(parts[0], `"`))
		if jid == "" {
			continue
		}
		nodeList := sanitizeLabel(strings.Trim(parts[2], `"`))
		cpus, _ := strconv.ParseFloat(strings.TrimSpace(strings.Trim(parts[4], `"`)), 64)
		tres := strings.TrimSpace(strings.Trim(parts[5], `"`))
		reqM := strings.TrimSpace(strings.Trim(parts[6], `"`))

		rec := sacctRunningJobAlloc{allocCPUS: cpus, allocTRES: tres, reqMem: reqM}
		snapshot.allocs[jid] = rec
		base := normalizeSlurmJobID(jid)
		if prev, ok := snapshot.allocs[base]; !ok || rec.allocCPUS > prev.allocCPUS {
			snapshot.allocs[base] = rec
		}

		gpus := parseAllocTRESGPUs(tres)
		if gpus <= 0 {
			continue
		}
		var hosts []string
		for _, n := range expandSlurmNodelist(nodeList) {
			n = strings.TrimSpace(n)
			if n != "" {
				hosts = append(hosts, n)
			}
		}
		if len(hosts) == 0 {
			continue
		}
		gpuRec := &sacctGPUJob{
			nodes:   hosts,
			perNode: gpus / float64(len(hosts)),
			total:   gpus,
		}
		snapshot.gpus[jid] = gpuRec
		snapshot.gpus[base] = gpuRec
	}
	return snapshot
}

// sacctRunningJobAllocs returns JobId -> allocation; base id (before '.') prefers max AllocCPUS.
func sacctRunningJobAllocs() map[string]sacctRunningJobAlloc {
	return newRunningJobSacctSnapshot().allocs
}

func jobPartitionMatchesFilter(jobPart, filter string) bool {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return true
	}
	allowed := make(map[string]struct{})
	for _, p := range strings.Split(filter, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			allowed[p] = struct{}{}
		}
	}
	for _, q := range strings.Split(jobPart, ",") {
		q = strings.TrimSpace(q)
		if q != "" {
			if _, ok := allowed[q]; ok {
				return true
			}
		}
	}
	return false
}

func baseJobIDKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if i := strings.Index(raw, "."); i > 0 {
		return raw[:i]
	}
	return raw
}

// parseReqMemPerNodeMiB returns MiB per node from ReqMem (e.g. 16Gn, 100G total).
func parseReqMemPerNodeMiB(req string, numNodes int) float64 {
	req = strings.TrimSpace(req)
	if req == "" || strings.EqualFold(req, "N/A") {
		return 0
	}
	low := strings.ToLower(req)
	perNode := strings.HasSuffix(low, "n") && len(req) > 1
	s := req
	if perNode {
		s = strings.TrimSpace(req[:len(req)-1])
	}
	mi := parseSlurmMemToMiB(s)
	if perNode {
		return mi
	}
	if numNodes > 0 {
		return mi / float64(numNodes)
	}
	return mi
}

// extractGPUBindSegments returns GPU index strings per bind segment (pipe-separated in squeue %b).
func extractGPUBindSegments(tresBind string) []string {
	tresBind = strings.TrimSpace(strings.Trim(tresBind, `"`))
	if tresBind == "" || strings.EqualFold(tresBind, "N/A") || tresBind == "(null)" {
		return nil
	}
	var segs []string
	for _, seg := range strings.Split(tresBind, "|") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		low := strings.ToLower(seg)
		idx := strings.Index(low, "gpu:")
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(seg[idx+4:])
		end := len(rest)
		for i, c := range rest {
			if c == ' ' || c == '(' {
				end = i
				break
			}
		}
		indices := strings.TrimSpace(rest[:end])
		if indices != "" {
			segs = append(segs, indices)
		}
	}
	return segs
}

func normalizeSqueueJobID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if i := strings.Index(raw, "."); i > 0 {
		suf := raw[i+1:]
		if suf == "batch" || suf == "extern" {
			return raw[:i]
		}
		if _, err := strconv.Atoi(suf); err == nil {
			// step id .0, .1 — use parent job id for sacct merge
			return raw[:i]
		}
	}
	return raw
}

func shouldIncludeSqueueLine(jobID string) bool {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return false
	}
	// Skip explicit job steps (e.g. 12345.interactive) except batch/extern (handled by normalize)
	if strings.Contains(jobID, ".") {
		suf := jobID[strings.LastIndex(jobID, ".")+1:]
		if suf != "batch" && suf != "extern" {
			if _, err := strconv.Atoi(suf); err == nil {
				return false
			}
		}
	}
	return true
}

// parseScontrolKeyValues parses a single-line "key=value" sequence from `scontrol show job -o`.
func parseScontrolKeyValues(line string) map[string]string {
	out := make(map[string]string)
	fields := strings.Fields(line)
	for _, f := range fields {
		if i := strings.Index(f, "="); i > 0 {
			k := f[:i]
			v := f[i+1:]
			out[k] = v
		}
	}
	return out
}

// expandIdxRanges expands "0-3,5" => "0,1,2,3,5"
func expandIdxRanges(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			ab := strings.SplitN(part, "-", 2)
			if len(ab) != 2 {
				continue
			}
			a, err1 := strconv.Atoi(strings.TrimSpace(ab[0]))
			b, err2 := strconv.Atoi(strings.TrimSpace(ab[1]))
			if err1 != nil || err2 != nil || a > b {
				continue
			}
			for i := a; i <= b; i++ {
				out = append(out, strconv.Itoa(i))
			}
		} else {
			out = append(out, part)
		}
	}
	return strings.Join(out, ",")
}

// reGresGPUIdxParen matches Slurm (IDX:…), (S:…), (Index:…) in Gres/GresDetail.
var reGresGPUIdxParen = regexp.MustCompile(`(?i)\((?:IDX|S|INDEX):([^)]+)\)`)

// reDevNvidia matches /dev/nvidiaN device paths in Gres strings.
var reDevNvidia = regexp.MustCompile(`(?i)/dev/nvidia(\d+)`)

// extractGPUIndicesFromSlurmGresString returns the longest single-parenthetical index list.
func extractGPUIndicesFromSlurmGresString(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var best string
	bestN := 0
	for _, m := range reGresGPUIdxParen.FindAllStringSubmatch(s, -1) {
		inner := strings.TrimSpace(m[1])
		if inner == "" || strings.EqualFold(inner, "N/A") {
			continue
		}
		exp := expandIdxRanges(inner)
		if exp == "" {
			continue
		}
		n := 1 + strings.Count(exp, ",")
		if n > bestN {
			bestN = n
			best = exp
		}
	}
	return best
}

// extractAllGPUIndicesFromSlurmGres merges every (IDX:)/(S:)/(Index:) segment into sorted unique indices.
func extractAllGPUIndicesFromSlurmGres(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	seen := make(map[string]struct{})
	for _, m := range reGresGPUIdxParen.FindAllStringSubmatch(s, -1) {
		inner := strings.TrimSpace(m[1])
		if inner == "" || strings.EqualFold(inner, "N/A") {
			continue
		}
		exp := expandIdxRanges(inner)
		for _, p := range strings.Split(exp, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				seen[p] = struct{}{}
			}
		}
	}
	if len(seen) == 0 {
		return ""
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, _ := strconv.Atoi(ids[i])
		aj, _ := strconv.Atoi(ids[j])
		return ai < aj
	})
	return strings.Join(ids, ",")
}

// extractGPUIndicesFromDevNvidia returns sorted unique indices from /dev/nvidiaN occurrences.
func extractGPUIndicesFromDevNvidia(s string) string {
	seen := make(map[string]struct{})
	for _, m := range reDevNvidia.FindAllStringSubmatch(s, -1) {
		if len(m) >= 2 {
			seen[m[1]] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return ""
	}
	var ids []string
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, _ := strconv.Atoi(ids[i])
		aj, _ := strconv.Atoi(ids[j])
		return ai < aj
	})
	return strings.Join(ids, ",")
}

// parseGresDetailPerNode maps node -> indices from tokens like gpu:…(S:0-3)@hostname.
func parseGresDetailPerNode(gresDetail string) map[string]string {
	m := make(map[string]string)
	s := strings.TrimSpace(gresDetail)
	if s == "" || strings.EqualFold(s, "(null)") {
		return m
	}
	for _, tok := range strings.Fields(s) {
		at := strings.LastIndex(tok, "@")
		if at <= 0 || at >= len(tok)-1 {
			continue
		}
		host := strings.TrimSpace(tok[at+1:])
		pre := tok[:at]
		if !strings.Contains(strings.ToLower(pre), "gpu") {
			continue
		}
		ix := extractGPUIndicesFromSlurmGresString(pre)
		if ix == "" {
			ix = extractGPUIndicesFromDevNvidia(pre)
		}
		if host != "" && ix != "" {
			m[host] = ix
		}
	}
	return m
}

func extractGresDetailGPUIdxAnywhere(gresDetail string) string {
	s := strings.TrimSpace(gresDetail)
	if s == "" {
		return ""
	}
	if ix := extractAllGPUIndicesFromSlurmGres(s); ix != "" {
		return ix
	}
	if ix := extractGPUIndicesFromSlurmGresString(s); ix != "" {
		return ix
	}
	return extractGPUIndicesFromDevNvidia(s)
}

// squeueRunningTresBindByJob returns base job id -> TRES bind field (%b) for running jobs.
func squeueRunningTresBindByJob() map[string]string {
	out := make(map[string]string)
	args := []string{"-h", "-t", "R", "-o", "%A|%b", "--noheader"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	b, err := execute("squeue", args)
	if err != nil {
		log.Warnf("squeue failed (GPU tres bind for indices): %v", err)
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 2)
		if len(parts) < 2 {
			continue
		}
		raw := strings.TrimSpace(parts[0])
		if !shouldIncludeSqueueLine(raw) {
			continue
		}
		base := baseJobIDKey(raw)
		bind := strings.TrimSpace(parts[1])
		if bind == "" || strings.EqualFold(bind, "N/A") {
			continue
		}
		if len(bind) > len(out[base]) {
			out[base] = bind
		}
	}
	return out
}

// gpuIndicesFromTresBindSegment parses one squeue %b segment (e.g. gres/gpu:0-3, gpu:0,1).
func gpuIndicesFromTresBindSegment(seg string) string {
	seg = strings.TrimSpace(seg)
	if seg == "" {
		return ""
	}
	low := strings.ToLower(seg)
	if strings.HasPrefix(low, "gres:gpu:") {
		// Slurm often prints a GPU count here (e.g. gres:gpu:8), not device indices.
		return ""
	}
	for _, key := range []string{"gres/gpu:", "gpu:"} {
		i := strings.Index(low, key)
		if i < 0 {
			continue
		}
		rest := seg[i+len(key):]
		end := len(rest)
		for j, c := range rest {
			if c == ' ' || c == '(' || c == '|' {
				end = j
				break
			}
		}
		s := strings.TrimSpace(rest[:end])
		if s == "" {
			continue
		}
		return expandIdxRanges(s)
	}
	return ""
}

func gpuIndicesFromTresBind(bind string, nodeIdx, numNodes int) string {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		return ""
	}
	segs := strings.Split(bind, "|")
	if numNodes <= 1 {
		if len(segs) > 0 {
			return gpuIndicesFromTresBindSegment(segs[0])
		}
		return ""
	}
	if nodeIdx < len(segs) {
		return gpuIndicesFromTresBindSegment(strings.TrimSpace(segs[nodeIdx]))
	}
	if len(segs) == 1 {
		return gpuIndicesFromTresBindSegment(segs[0])
	}
	return ""
}

// distributeGPUIndicesAcrossNodes splits "0,1,2,3,4,5,6,7" across nodes when each node has perNode GPUs.
func distributeGPUIndicesAcrossNodes(indices string, nodes []string, perNode int) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(indices, ",")
	if perNode <= 0 || len(nodes) == 0 {
		return out
	}
	need := len(nodes) * perNode
	if len(parts) != need {
		return out
	}
	for i, node := range nodes {
		chunk := parts[i*perNode : (i+1)*perNode]
		out[strings.TrimSpace(node)] = strings.Join(chunk, ",")
	}
	return out
}

func gpuIdxIntsToCSV(idxs []int) string {
	if len(idxs) == 0 {
		return ""
	}
	// Ensure stable order (scontrol usually returns ordered, but don't rely on it)
	cp := make([]int, len(idxs))
	copy(cp, idxs)
	sort.Ints(cp)
	ss := make([]string, len(cp))
	for i, v := range cp {
		ss[i] = strconv.Itoa(v)
	}
	return strings.Join(ss, ",")
}

type scontrolJobGroup struct {
	jobName    string
	partition  string
	maxCPUs    float64
	minMemNode string
	allocTRES  string
	gresPieces []string
	nodeList   string
	user       string // from UserId= (username only)
	qos        string
	startTime  string // raw StartTime= from scontrol (e.g. 2026-03-05T17:40:19)
	runTime    string // raw RunTime= from scontrol (e.g. 14-23:21:23 or HH:MM:SS)
}

// RunningJobSummary is one RUNNING job's cluster-wide allocation and metadata (for slurm_job_* metrics).
type RunningJobSummary struct {
	JobID       string
	JobName     string
	Partition   string
	User        string
	QOS         string
	Nodes       string // comma-separated expanded hostnames
	StartTime   string // raw scontrol StartTime
	RunTime     string // raw scontrol RunTime
	TotalCPUs   float64
	TotalMemMiB float64
	TotalGPUs   float64
}

type runningJobData struct {
	rows      []NodeJobRow
	summaries []RunningJobSummary
}

func parseScontrolUserId(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if i := strings.Index(s, "("); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// parseSlurmStartTimeUnix parses scontrol StartTime string into Unix seconds; 0 if unknown.
// Slurm prints StartTime without a zone (e.g. 2026-03-05T17:40:19); that is interpreted in local time.
func parseSlurmStartTimeUnix(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "Unknown") || strings.EqualFold(s, "None") || strings.EqualFold(s, "Invalid") {
		return 0
	}
	if u, err := strconv.ParseInt(s, 10, 64); err == nil && u > 1000000000 {
		return float64(u)
	}
	// scontrol-style, no timezone → local wall time
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05.000000000",
	} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return float64(t.Unix())
		}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return float64(t.Unix())
	}
	return 0
}

func jobLabelOrUnknown(s string) string {
	s = strings.TrimSpace(sanitizeLabel(s))
	if s == "" {
		return "unknown"
	}
	return s
}

// sacctGPUJob matches ParseGPUsAllocationByJob (sacct JobId, NodeList, AllocTRES gres/gpu).
type sacctGPUJob struct {
	nodes   []string // expanded hostnames, order preserved for multi-node GPU index chunks
	perNode float64
	total   float64
}

// sacctGPUAllocByJob builds map base job id -> GPU allocation from the same sacct query as slurm_gpus_allocated_to_job.
func sacctGPUAllocByJob() map[string]*sacctGPUJob {
	return newRunningJobSacctSnapshot().gpus
}

func (g *sacctGPUJob) gpusOnNode(node string) float64 {
	node = strings.TrimSpace(node)
	for _, h := range g.nodes {
		if strings.TrimSpace(h) == node {
			return g.perNode
		}
	}
	return 0
}

func (g *sacctGPUJob) nodeBindIndex(node string) (idx int, nNodes int) {
	node = strings.TrimSpace(node)
	for i, h := range g.nodes {
		if strings.TrimSpace(h) == node {
			return i, len(g.nodes)
		}
	}
	return 0, len(g.nodes)
}

// ParseNodeRunningJobs returns per-(node,job) rows: merges all RUNNING scontrol lines per base job
// (73444 + 73444.batch, …) and supplements CPUs/memory/GPUs from sacct when steps report 0.
func ParseNodeRunningJobs() []NodeJobRow {
	return loadRunningJobData().rows
}

// loadRunningJobData runs one scontrol/sacct/squeue pass and returns per-node rows plus job-level summaries.
func loadRunningJobData() runningJobData {
	data, err := execute("scontrol", []string{"show", "job", "-o"})
	if err != nil {
		log.Warnf("scontrol show job failed (node job metrics): %v", err)
		return runningJobData{}
	}
	sacctSnapshot := newRunningJobSacctSnapshot()
	sacctJobs := sacctSnapshot.allocs
	sacctGPU := sacctSnapshot.gpus
	jobGPUs := fetchAllocatedGPUsByJob()
	tresBindByJob := squeueRunningTresBindByJob()

	groups := make(map[string]*scontrolJobGroup)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		kv := parseScontrolKeyValues(line)
		if strings.TrimSpace(kv["JobState"]) != "RUNNING" {
			continue
		}
		base := baseJobIDKey(kv["JobId"])
		if base == "" {
			continue
		}
		g, ok := groups[base]
		if !ok {
			g = &scontrolJobGroup{}
			groups[base] = g
		}
		if g.jobName == "" {
			g.jobName = strings.TrimSpace(kv["JobName"])
		}
		if g.partition == "" {
			g.partition = strings.TrimSpace(kv["Partition"])
		}
		cpus, _ := strconv.ParseFloat(strings.TrimSpace(kv["NumCPUs"]), 64)
		if cpus > g.maxCPUs {
			g.maxCPUs = cpus
			g.minMemNode = strings.TrimSpace(kv["MinMemoryNode"])
		}
		at := strings.TrimSpace(kv["AllocTRES"])
		if at == "" {
			at = strings.TrimSpace(kv["TRES"])
		}
		if len(at) > len(g.allocTRES) {
			g.allocTRES = at
		}
		if gd := strings.TrimSpace(kv["GresDetail"]); gd != "" && !strings.EqualFold(gd, "(null)") {
			g.gresPieces = append(g.gresPieces, gd)
		}
		if gr := strings.TrimSpace(kv["Gres"]); gr != "" && !strings.EqualFold(gr, "(null)") {
			g.gresPieces = append(g.gresPieces, gr)
		}
		nl := strings.TrimSpace(kv["NodeList"])
		if nl != "" && (g.nodeList == "" || len(nl) > len(g.nodeList)) {
			g.nodeList = nl
		}
		if g.user == "" {
			if u := strings.TrimSpace(kv["UserId"]); u != "" {
				g.user = parseScontrolUserId(u)
			}
		}
		if g.qos == "" {
			if q := strings.TrimSpace(kv["QOS"]); q != "" {
				g.qos = q
			}
		}
		if st := strings.TrimSpace(kv["StartTime"]); st != "" && !strings.EqualFold(st, "Unknown") {
			if g.startTime == "" || len(st) > len(g.startTime) {
				g.startTime = st
			}
		}
		if rt := strings.TrimSpace(kv["RunTime"]); rt != "" && !strings.EqualFold(rt, "N/A") {
			if g.runTime == "" || len(rt) > len(g.runTime) {
				g.runTime = rt
			}
		}
	}

	var rows []NodeJobRow
	var summaries []RunningJobSummary
	for base, g := range groups {
		if !jobPartitionMatchesFilter(g.partition, gpuPartitionsFilter) {
			continue
		}
		nodes := expandSlurmNodelist(g.nodeList)
		if len(nodes) == 0 {
			continue
		}
		nn := float64(len(nodes))

		sa := sacctJobs[base]
		if sa.allocCPUS == 0 && strings.Contains(base, "_") {
			if i := strings.Index(base, "_"); i > 0 {
				sa = sacctJobs[base[:i]]
			}
		}

		totalCPUs := g.maxCPUs
		if totalCPUs <= 0 {
			totalCPUs = sa.allocCPUS
		}
		allocT := g.allocTRES
		if strings.TrimSpace(allocT) == "" {
			allocT = sa.allocTRES
		}
		if c, _, _ := parseTRESAllocFields(allocT); totalCPUs <= 0 && c > 0 {
			totalCPUs = c
		}
		cpusPerNode := totalCPUs / nn

		memMiB := parseSqueueMinMemoryMiB(g.minMemNode)
		if memMiB <= 0 {
			_, memTot, _ := parseTRESAllocFields(allocT)
			if memTot > 0 {
				memMiB = memTot / nn
			}
		}
		if memMiB <= 0 {
			memMiB = parseReqMemPerNodeMiB(sa.reqMem, len(nodes))
		}

		sg := sacctGPU[base]
		if sg == nil && strings.Contains(base, "_") {
			if i := strings.Index(base, "_"); i > 0 {
				sg = sacctGPU[base[:i]]
			}
		}
		var gpuTotal float64
		if sg != nil {
			gpuTotal = sg.total
		} else {
			_, _, gpuTotal = parseTRESAllocFields(allocT)
			if gpuTotal <= 0 {
				gpuTotal = parseAllocTRESGPUs(sa.allocTRES)
			}
		}
		gpusPerNode := gpuTotal / nn

		// GPU indices: use same authoritative source as gpus.go (scontrol show job -d)
		// This is the only reliably correct per-node GPU index mapping across Slurm configs.
		idxPerNode := make(map[string]string)
		if gpuTotal > 0 {
			for _, ng := range jobGPUs[normalizeSlurmJobID(base)] {
				if ng.Node == "" {
					continue
				}
				if s := gpuIdxIntsToCSV(ng.GPUs); s != "" {
					idxPerNode[strings.TrimSpace(ng.Node)] = s
				}
			}
		}

		// If scontrol -d didn't yield indices, attempt best-effort inference from Gres/GresDetail and/or squeue %b.
		if len(idxPerNode) == 0 {
			for _, piece := range g.gresPieces {
				for n, ix := range parseGresDetailPerNode(piece) {
					idxPerNode[n] = ix
				}
			}
			if len(nodes) == 1 {
				only := strings.TrimSpace(nodes[0])
				if idxPerNode[only] == "" {
					for _, piece := range g.gresPieces {
						if ix := extractGresDetailGPUIdxAnywhere(piece); ix != "" {
							idxPerNode[only] = ix
							break
						}
					}
				}
			}
		}

		// Job-level Gres/GresDetail: use sacct GPU total/count for index validation; node order from sacct GPU nodelist
		joinedGres := strings.Join(g.gresPieces, " ")
		jobLevelIdx := extractAllGPUIndicesFromSlurmGres(joinedGres)
		if jobLevelIdx == "" {
			jobLevelIdx = extractGPUIndicesFromSlurmGresString(joinedGres)
		}
		if jobLevelIdx == "" {
			jobLevelIdx = extractGPUIndicesFromDevNvidia(joinedGres)
		}
		if jobLevelIdx != "" && gpuTotal > 0 {
			nIdx := 1 + strings.Count(jobLevelIdx, ",")
			want := int(gpuTotal + 0.5)
			if nIdx == want {
				if sg != nil && len(sg.nodes) >= 1 {
					if len(sg.nodes) == 1 {
						only := strings.TrimSpace(sg.nodes[0])
						if idxPerNode[only] == "" {
							idxPerNode[only] = jobLevelIdx
						}
					} else {
						perN := int(sg.perNode + 0.5)
						if perN > 0 && float64(perN) == sg.perNode {
							for k, v := range distributeGPUIndicesAcrossNodes(jobLevelIdx, sg.nodes, perN) {
								if idxPerNode[k] == "" {
									idxPerNode[k] = v
								}
							}
						}
					}
				} else if len(nodes) == 1 {
					only := strings.TrimSpace(nodes[0])
					if idxPerNode[only] == "" {
						idxPerNode[only] = jobLevelIdx
					}
				} else {
					perN := int(gpusPerNode + 0.5)
					if perN > 0 && float64(perN) == gpusPerNode {
						for k, v := range distributeGPUIndicesAcrossNodes(jobLevelIdx, nodes, perN) {
							if idxPerNode[k] == "" {
								idxPerNode[k] = v
							}
						}
					}
				}
			}
		}

		jobName := sanitizeLabel(g.jobName)
		jobName = strings.ReplaceAll(jobName, "|", "_")
		jobID := sanitizeLabel(base)

		nodeListLabel := strings.Join(nodes, ",")
		if len(nodeListLabel) > 2000 {
			nodeListLabel = nodeListLabel[:2000]
		}
		summaries = append(summaries, RunningJobSummary{
			JobID:       jobID,
			JobName:     jobName,
			Partition:   jobLabelOrUnknown(g.partition),
			User:        jobLabelOrUnknown(g.user),
			QOS:         jobLabelOrUnknown(g.qos),
			Nodes:       jobLabelOrUnknown(nodeListLabel),
			StartTime:   g.startTime,
			RunTime:     g.runTime,
			TotalCPUs:   totalCPUs,
			TotalMemMiB: memMiB * nn,
			TotalGPUs:   gpuTotal,
		})

		for _, node := range nodes {
			node = strings.TrimSpace(node)
			if node == "" {
				continue
			}
			idx := idxPerNode[node]
			if idx != "" && (strings.EqualFold(idx, "N/A") || idx == "none") {
				idx = ""
			}
			var gn float64
			if sg != nil {
				gn = sg.gpusOnNode(node)
			} else {
				gn = gpusPerNode
			}
			if idx == "" && gn > 0 {
				if tb := tresBindByJob[base]; tb != "" {
					bi, bn := 0, len(nodes)
					if sg != nil {
						bi, bn = sg.nodeBindIndex(node)
					} else {
						for i, x := range nodes {
							if strings.TrimSpace(x) == node {
								bi, bn = i, len(nodes)
								break
							}
						}
					}
					idx = gpuIndicesFromTresBind(tb, bi, bn)
				}
			}
			if idx != "" && gn > 0 {
				idxN := float64(1 + strings.Count(idx, ","))
				if idxN == gn {
					// keep gn from sacct
				} else if idxN > 0 {
					gn = idxN
				}
			}
			rows = append(rows, NodeJobRow{
				Node:      sanitizeLabel(node),
				JobID:     jobID,
				JobName:   jobName,
				CPUs:      cpusPerNode,
				MemoryMiB: memMiB,
				GPUs:      gn,
				GPUIdx:    idx,
			})
		}
	}
	return runningJobData{rows: rows, summaries: summaries}
}

// NodeJobsCollector exposes running job allocation per node.
type NodeJobsCollector struct {
	cpus      *prometheus.Desc
	mem       *prometheus.Desc
	gpus      *prometheus.Desc
	gpuIdx    *prometheus.Desc
	jobInfo   *prometheus.Desc
	jobStart  *prometheus.Desc
	jobTotCPU *prometheus.Desc
	jobTotMem *prometheus.Desc
	jobTotGPU *prometheus.Desc
}

func NewNodeJobsCollector() *NodeJobsCollector {
	return &NodeJobsCollector{
		cpus: prometheus.NewDesc("slurm_node_job_allocated_cpus",
			"Allocated CPU cores per node (scontrol+sacct merged; split evenly across nodelist if multi-node)",
			[]string{"node", "job_id", "job_name"}, nil),
		mem: prometheus.NewDesc("slurm_node_job_allocated_memory_mib",
			"Allocated memory (MiB) per node (scontrol MinMemoryNode, else AllocTRES/ReqMem via sacct)",
			[]string{"node", "job_id", "job_name"}, nil),
		gpus: prometheus.NewDesc("slurm_node_job_allocated_gpus",
			"Allocated GPUs on this node (same sacct basis as slurm_gpus_allocated_to_job / ParseGPUsAllocationByJob)",
			[]string{"node", "job_id", "job_name"}, nil),
		gpuIdx: prometheus.NewDesc("slurm_node_job_gpu_indices",
			"GPU indices (GresDetail IDX/S:/dev/nvidia, else squeue %b); value 1; none if unknown",
			[]string{"node", "job_id", "job_name", "gpu_indices"}, nil),
		jobInfo: prometheus.NewDesc("slurm_job_info",
			"Metadata for RUNNING jobs (value 1); start_time and run_time are raw scontrol strings; use slurm_job_start_time_seconds for Unix start",
			[]string{"job_id", "job_name", "partition", "user", "qos", "state", "nodes", "start_time", "run_time"}, nil),
		jobStart: prometheus.NewDesc("slurm_job_start_time_seconds",
			"Unix start time of the job from scontrol StartTime; 0 if unknown",
			[]string{"job_id", "job_name", "partition"}, nil),
		jobTotCPU: prometheus.NewDesc("slurm_job_allocated_cpus",
			"Total allocated CPU cores for the job (cluster-wide)",
			[]string{"job_id", "job_name", "partition"}, nil),
		jobTotMem: prometheus.NewDesc("slurm_job_allocated_memory_mib",
			"Total allocated memory for the job in MiB (sum over nodes)",
			[]string{"job_id", "job_name", "partition"}, nil),
		jobTotGPU: prometheus.NewDesc("slurm_job_allocated_gpus",
			"Total allocated GPUs for the job",
			[]string{"job_id", "job_name", "partition"}, nil),
	}
}

func (c *NodeJobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.cpus
	ch <- c.mem
	ch <- c.gpus
	ch <- c.gpuIdx
	ch <- c.jobInfo
	ch <- c.jobStart
	ch <- c.jobTotCPU
	ch <- c.jobTotMem
	ch <- c.jobTotGPU
}

func (c *NodeJobsCollector) Collect(ch chan<- prometheus.Metric) {
	data := loadRunningJobData()
	for _, r := range data.rows {
		ch <- prometheus.MustNewConstMetric(c.cpus, prometheus.GaugeValue, r.CPUs, r.Node, r.JobID, r.JobName)
		ch <- prometheus.MustNewConstMetric(c.mem, prometheus.GaugeValue, r.MemoryMiB, r.Node, r.JobID, r.JobName)
		ch <- prometheus.MustNewConstMetric(c.gpus, prometheus.GaugeValue, r.GPUs, r.Node, r.JobID, r.JobName)
		idx := r.GPUIdx
		if idx == "" {
			idx = "none"
		}
		ch <- prometheus.MustNewConstMetric(c.gpuIdx, prometheus.GaugeValue, 1, r.Node, r.JobID, r.JobName, idx)
	}
	const stateRunning = "running"
	for _, s := range data.summaries {
		ch <- prometheus.MustNewConstMetric(c.jobInfo, prometheus.GaugeValue, 1,
			s.JobID, s.JobName, s.Partition, s.User, s.QOS, stateRunning, s.Nodes,
			jobLabelOrUnknown(s.StartTime), jobLabelOrUnknown(s.RunTime))
		ch <- prometheus.MustNewConstMetric(c.jobStart, prometheus.GaugeValue, parseSlurmStartTimeUnix(s.StartTime),
			s.JobID, s.JobName, s.Partition)
		ch <- prometheus.MustNewConstMetric(c.jobTotCPU, prometheus.GaugeValue, s.TotalCPUs,
			s.JobID, s.JobName, s.Partition)
		ch <- prometheus.MustNewConstMetric(c.jobTotMem, prometheus.GaugeValue, s.TotalMemMiB,
			s.JobID, s.JobName, s.Partition)
		ch <- prometheus.MustNewConstMetric(c.jobTotGPU, prometheus.GaugeValue, s.TotalGPUs,
			s.JobID, s.JobName, s.Partition)
	}
}
