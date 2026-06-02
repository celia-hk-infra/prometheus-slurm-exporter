/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
	unavailable float64
}

// gpuPartitionsFilter is set by main when -gpu-partitions is used; restricts sinfo/sacct to those partitions.
var gpuPartitionsFilter string

// dcgmExporterPort is the port on GPU nodes where DCGM exporter is exposed (e.g. 9400).
var dcgmExporterPort = "9400"

// default job groups
var defaultJobGroupName = []string{"VGEN", "BOOGU", "IVTR", "STRM", "AGNT", "DLM", "OTH"}

// SetGPUPartitions sets the partition filter for GPU metrics (e.g. "gpu,gpu-long"). Call before registering GPUsCollector.
func SetGPUPartitions(partitions string) {
	gpuPartitionsFilter = strings.TrimSpace(partitions)
}

// SetDCGMExporterPort sets the port used to scrape DCGM exporter on GPU nodes (default 9400).
func SetDCGMExporterPort(port string) {
	p := strings.TrimSpace(port)
	if p != "" {
		dcgmExporterPort = p
	}
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter) // sacct: restrict to partition(s)
	}
	outputBytes, err := execute("sacct", args)
	if err != nil {
		log.Warnf("sacct failed (allocated GPUs): %v", err)
		return num_gpus
	}
	output := string(outputBytes)
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			line = strings.TrimSpace(strings.Trim(line, "\""))
			if line == "" {
				continue
			}
			// AllocTRES format: e.g. "cpu=2,mem=4096,gres/gpu=4" or "gres/gpu=1"
			for _, part := range strings.Split(line, ",") {
				part = strings.TrimSpace(part)
				if strings.HasPrefix(part, "gres/gpu=") {
					s := strings.TrimPrefix(part, "gres/gpu=")
					if n, err := strconv.ParseFloat(s, 64); err == nil {
						num_gpus += n
					}
					break // one gres/gpu= per job line
				}
			}
		}
	}

	return num_gpus
}

func extractJobGroup(jobName string) string {
	jobName = strings.TrimSpace(strings.Trim(jobName, "\""))
	if jobName == "" {
		return ""
	}
	return strings.ToUpper(strings.Split(jobName, "-")[0])
}

// ParseGPUsByJobName returns total GPU count per job name (sum over all running jobs with that name).
func ParseGPUsByJobGroup() map[string]float64 {
	// float64 map lookups default to 0 for missing keys.
	out := make(map[string]float64)
	// add default keys in out
	for _, jobGroupName := range defaultJobGroupName {
		out[jobGroupName] = 0
	}

	args := []string{"-a", "-X", "--format=JobName,AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	outputBytes, err := execute("sacct", args)
	if err != nil {
		log.Warnf("sacct failed (GPUs by job group): %v", err)
		return out
	}
	output := string(outputBytes)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// parsable2: JobName|AllocTRES (e.g. "VGEN-zq-|gres/gpu=1" or "BOOGU-zi|cpu=2,mem=4096,gres/gpu=1")
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		// jobGroup := strings.TrimSpace(strings.Trim(parts[0], "\""))
		jobName := strings.TrimSpace(strings.Trim(parts[0], "\""))
		jobGroup := extractJobGroup(jobName)
		tres := parts[1]
		var gpus float64
		for _, part := range strings.Split(tres, ",") {
			part = strings.TrimSpace(strings.Trim(part, "\""))
			if strings.HasPrefix(part, "gres/gpu=") {
				s := strings.TrimPrefix(part, "gres/gpu=")
				if n, err := strconv.ParseFloat(s, 64); err == nil {
					gpus = n
				}
				break
			}
		}
		if jobGroup != "" {
			out[jobGroup] += gpus
		}
	}
	return out
}

// JobGPUAlloc is one job's GPU allocation (which GPUs / nodes are allocated to which job).
type JobGPUAlloc struct {
	JobID     string
	JobName   string
	NodeList  string
	Partition string
	GPUs      float64
}

// ParseGPUsAllocationByJob returns per-job GPU allocation (job id, name, nodelist, partition, gpu count).
func ParseGPUsAllocationByJob() []JobGPUAlloc {
	var out []JobGPUAlloc
	args := []string{"-a", "-X", "--format=JobId,JobName,NodeList,AllocTRES,Partition", "--state=RUNNING", "--noheader", "--parsable2"}
	if gpuPartitionsFilter != "" {
		args = append(args, "--partition="+gpuPartitionsFilter)
	}
	outputBytes, err := execute("sacct", args)
	if err != nil {
		log.Warnf("sacct failed (GPUs allocation by job): %v", err)
		return out
	}
	for _, line := range strings.Split(string(outputBytes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// parsable2: JobId|JobName|NodeList|AllocTRES|Partition
		parts := strings.Split(line, "|")
		if len(parts) < 5 {
			continue
		}
		jobID := sanitizeLabel(strings.Trim(parts[0], "\""))
		jobName := sanitizeLabel(strings.Trim(parts[1], "\""))
		nodeList := sanitizeLabel(strings.Trim(parts[2], "\""))
		tres := parts[3]
		partition := sanitizeLabel(strings.Trim(parts[4], "\""))
		var gpus float64
		for _, part := range strings.Split(tres, ",") {
			part = strings.TrimSpace(strings.Trim(part, "\""))
			if strings.HasPrefix(part, "gres/gpu=") {
				s := strings.TrimPrefix(part, "gres/gpu=")
				if n, err := strconv.ParseFloat(s, 64); err == nil {
					gpus = n
				}
				break
			}
		}
		if jobID != "" && gpus > 0 {
			// Expand compressed nodelist (e.g. hk01dgx[038,054] -> hk01dgx038,hk01dgx054) for consistent parsing and metrics.
			expanded := expandSlurmNodelist(nodeList)
			nodeListForMetric := nodeList
			if len(expanded) > 0 {
				nodeListForMetric = strings.Join(expanded, ",")
			}
			out = append(out, JobGPUAlloc{JobID: jobID, JobName: jobName, NodeList: nodeListForMetric, Partition: partition, GPUs: gpus})
		}
	}
	return out
}

// sanitizeLabel replaces characters that are problematic in Prometheus label values.
func sanitizeLabel(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

// expandSlurmNodelist expands Slurm's compressed nodelist (e.g. hk01dgx[038,054] -> hk01dgx038, hk01dgx054).
// Uses "scontrol show hostnames" when possible; otherwise parses bracket format prefix[n1,n2-n3,...].
// If nodelist contains no '[', returns comma-separated entries as a slice.
func expandSlurmNodelist(nodelist string) []string {
	nodelist = strings.TrimSpace(nodelist)
	if nodelist == "" {
		return nil
	}
	// Already comma-separated list (e.g. after prior expansion): return as slice.
	if !strings.Contains(nodelist, "[") {
		var nodes []string
		for _, s := range strings.Split(nodelist, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				nodes = append(nodes, s)
			}
		}
		if len(nodes) > 0 {
			return nodes
		}
		return []string{nodelist}
	}
	// Prefer scontrol so expansion always matches Slurm.
	out, err := execute("scontrol", []string{"show", "hostnames", nodelist})
	if err == nil {
		var nodes []string
		for _, line := range strings.Split(string(out), "\n") {
			node := strings.TrimSpace(line)
			if node != "" {
				nodes = append(nodes, node)
			}
		}
		if len(nodes) > 0 {
			return nodes
		}
	}
	// Fallback: parse bracket format prefix[suffix1,suffix2,...] or prefix[start-end]
	idx := strings.Index(nodelist, "[")
	if idx < 0 {
		return []string{nodelist}
	}
	prefix := nodelist[:idx]
	rest := nodelist[idx+1:]
	endIdx := strings.Index(rest, "]")
	if endIdx < 0 {
		return []string{nodelist}
	}
	rangeStr := strings.TrimSpace(rest[:endIdx])
	if rangeStr == "" {
		return []string{nodelist}
	}
	var suffixes []string
	for _, part := range strings.Split(rangeStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			// Range: e.g. 038-040
			rangeParts := strings.SplitN(part, "-", 2)
			if len(rangeParts) != 2 {
				suffixes = append(suffixes, part)
				continue
			}
			startStr := strings.TrimSpace(rangeParts[0])
			endStr := strings.TrimSpace(rangeParts[1])
			start, err1 := strconv.Atoi(startStr)
			end, err2 := strconv.Atoi(endStr)
			if err1 != nil || err2 != nil || start > end {
				suffixes = append(suffixes, part)
				continue
			}
			width := len(startStr)
			if len(endStr) > width {
				width = len(endStr)
			}
			for i := start; i <= end; i++ {
				suffixes = append(suffixes, fmt.Sprintf("%0*d", width, i))
			}
		} else {
			suffixes = append(suffixes, part)
		}
	}
	if len(suffixes) == 0 {
		return []string{nodelist}
	}
	nodes := make([]string, 0, len(suffixes))
	for _, s := range suffixes {
		nodes = append(nodes, prefix+s)
	}
	return nodes
}

// DCGM metric names.
const (
	dcgmGPUUtilMetric   = "DCGM_FI_DEV_GPU_UTIL" // GPU utilization 0-100
	dcgmGPUMemoryMetric = "DCGM_FI_DEV_FB_USED"  // GPU memory used in MB (framebuffer)
)

// parseDCGMMetricPerGPU extracts a DCGM metric per GPU index from Prometheus exposition format.
// Returns map from gpu index (e.g. "0", "4") to metric value. Metric line format: METRIC{gpu="0",...} 42.0
func parseDCGMMetricPerGPU(body []byte, metricName string) map[string]float64 {
	out := make(map[string]float64)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || !strings.Contains(line, metricName) {
			continue
		}
		gpuIdx := ""
		if i := strings.Index(line, `gpu="`); i >= 0 {
			start := i + len(`gpu="`)
			end := strings.Index(line[start:], `"`)
			if end >= 0 {
				gpuIdx = line[start : start+end]
			}
		}
		if gpuIdx == "" {
			continue
		}
		idx := strings.Index(line, "} ")
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(line[idx+2:])
		fields := strings.Fields(rest)
		if len(fields) < 1 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		out[gpuIdx] = v
	}
	return out
}

// parseDCGMUtilPerGPU extracts DCGM_FI_DEV_GPU_UTIL per GPU index (utilization 0-100).
func parseDCGMUtilPerGPU(body []byte) map[string]float64 {
	return parseDCGMMetricPerGPU(body, dcgmGPUUtilMetric)
}

// parseDCGMMemoryPerGPU extracts DCGM_FI_DEV_FB_USED per GPU index (memory used in MB).
func parseDCGMMemoryPerGPU(body []byte) map[string]float64 {
	return parseDCGMMetricPerGPU(body, dcgmGPUMemoryMetric)
}

var dcgmHTTPClient = &http.Client{
	Timeout: 3 * time.Second,
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: 2 * time.Second}
			return d.DialContext(ctx, network, addr)
		},
	},
}

// fetchDCGMMetrics returns raw /metrics body from the DCGM exporter on the given node.
func fetchDCGMMetrics(node string) ([]byte, error) {
	node = strings.TrimSpace(node)
	if node == "" {
		return nil, fmt.Errorf("empty node")
	}
	url := fmt.Sprintf("http://%s:%s/metrics", node, dcgmExporterPort)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := dcgmHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// fetchDCGMGPUUtilPerGPU returns per-GPU utilization (0-100) from the DCGM exporter on the given node. Key = gpu index as string ("0", "1", ...).
func fetchDCGMGPUUtilPerGPU(node string) (map[string]float64, error) {
	body, err := fetchDCGMMetrics(node)
	if err != nil {
		return nil, err
	}
	out := parseDCGMUtilPerGPU(body)
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s metrics found", dcgmGPUUtilMetric)
	}
	return out, nil
}

// fetchDCGMMemoryPerGPU returns per-GPU memory used (MB, DCGM_FI_DEV_FB_USED) from the DCGM exporter on the given node.
func fetchDCGMMemoryPerGPU(node string) (map[string]float64, error) {
	body, err := fetchDCGMMetrics(node)
	if err != nil {
		return nil, err
	}
	out := parseDCGMMemoryPerGPU(body)
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s metrics found", dcgmGPUMemoryMetric)
	}
	return out, nil
}

type dcgmNodeMetrics struct {
	util   map[string]float64
	memory map[string]float64
	err    error
}

type dcgmScrapeCache struct {
	nodes map[string]dcgmNodeMetrics
}

func newDCGMScrapeCache() *dcgmScrapeCache {
	return &dcgmScrapeCache{nodes: make(map[string]dcgmNodeMetrics)}
}

func (c *dcgmScrapeCache) nodeMetrics(node string) dcgmNodeMetrics {
	if c == nil {
		c = newDCGMScrapeCache()
	}
	node = strings.TrimSpace(node)
	if node == "" {
		return dcgmNodeMetrics{err: fmt.Errorf("empty node")}
	}
	if metrics, ok := c.nodes[node]; ok {
		return metrics
	}

	body, err := fetchDCGMMetrics(node)
	if err != nil {
		metrics := dcgmNodeMetrics{err: err}
		c.nodes[node] = metrics
		return metrics
	}

	metrics := dcgmNodeMetrics{
		util:   parseDCGMUtilPerGPU(body),
		memory: parseDCGMMemoryPerGPU(body),
	}
	c.nodes[node] = metrics
	return metrics
}

func (c *dcgmScrapeCache) gpuUtilPerGPU(node string) (map[string]float64, error) {
	metrics := c.nodeMetrics(node)
	if metrics.err != nil {
		return nil, metrics.err
	}
	if len(metrics.util) == 0 {
		return nil, fmt.Errorf("no %s metrics found", dcgmGPUUtilMetric)
	}
	return metrics.util, nil
}

func (c *dcgmScrapeCache) gpuMemoryPerGPU(node string) (map[string]float64, error) {
	metrics := c.nodeMetrics(node)
	if metrics.err != nil {
		return nil, metrics.err
	}
	if len(metrics.memory) == 0 {
		return nil, fmt.Errorf("no %s metrics found", dcgmGPUMemoryMetric)
	}
	return metrics.memory, nil
}

type gpuScrapeSnapshot struct {
	dcgm      *dcgmScrapeCache
	jobAllocs []JobGPUAlloc
	jobGPUs   map[string][]NodeGPUs
	gpuNodes  []nodePartitionGPUs
}

func newGPUScrapeSnapshot() *gpuScrapeSnapshot {
	return &gpuScrapeSnapshot{
		dcgm:      newDCGMScrapeCache(),
		jobAllocs: ParseGPUsAllocationByJob(),
		jobGPUs:   fetchAllocatedGPUsByJob(),
	}
}

func (s *gpuScrapeSnapshot) allocatedGPUs(jobID string) []NodeGPUs {
	if s == nil {
		return nil
	}
	return s.jobGPUs[normalizeSlurmJobID(jobID)]
}

func (s *gpuScrapeSnapshot) nodes() []nodePartitionGPUs {
	if s == nil {
		return nil
	}
	if s.gpuNodes == nil {
		s.gpuNodes = fetchGPUNodeSinfo()
	}
	return s.gpuNodes
}

func fetchAllocatedGPUsByJob() map[string][]NodeGPUs {
	raw, err := execute("scontrol", []string{"show", "job", "-d"})
	if err != nil {
		log.Warnf("scontrol show job -d failed: %v", err)
		return make(map[string][]NodeGPUs)
	}
	return parseScontrolJobGRESByJob(string(raw))
}

func normalizeSlurmJobID(jobID string) string {
	jobID = strings.TrimSpace(jobID)
	if i := strings.Index(jobID, "."); i > 0 {
		jobID = jobID[:i]
	}
	return jobID
}

// NodeGPUs is one node's GPU indices allocated to a job (from scontrol show job -d).
type NodeGPUs struct {
	Node string
	GPUs []int // GPU device indices (e.g. 0, 1 or 6, 7)
}

// getJobAllocatedGPUs runs "scontrol show job -d <jobid>" and parses Nodes=... GRES=... IDX:... to return which GPU indices on which node are allocated to the job.
func getJobAllocatedGPUs(jobID string) []NodeGPUs {
	jobID = normalizeSlurmJobID(jobID)
	if jobID == "" {
		return nil
	}
	out, err := execute("scontrol", []string{"show", "job", "-d", jobID})
	if err != nil {
		log.Warnf("scontrol show job -d %s failed: %v", jobID, err)
		return nil
	}
	return parseScontrolJobGRES(string(out))
}

func parseScontrolJobGRESByJob(text string) map[string][]NodeGPUs {
	result := make(map[string][]NodeGPUs)
	currentJobID := ""
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if jobID := parseScontrolJobID(line); jobID != "" {
			currentJobID = jobID
		}
		if currentJobID == "" {
			continue
		}
		if ngs, ok := parseScontrolJobGRESLine(line); ok {
			result[currentJobID] = append(result[currentJobID], ngs...)
		}
	}
	return result
}

func parseScontrolJobID(line string) string {
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "JobId=") {
			return normalizeSlurmJobID(strings.TrimPrefix(f, "JobId="))
		}
	}
	return ""
}

// parseScontrolJobGRES parses "scontrol show job -d" output for lines like "Nodes=hk01dgx038 ... GRES=gpu:2(IDX:0-1)" and returns (node, gpu indices) per line.
func parseScontrolJobGRES(text string) []NodeGPUs {
	var result []NodeGPUs
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if ngs, ok := parseScontrolJobGRESLine(line); ok {
			result = append(result, ngs...)
		}
	}
	return result
}

func parseScontrolJobGRESLine(line string) ([]NodeGPUs, bool) {
	if !strings.Contains(line, "Nodes=") || !strings.Contains(line, "GRES=") || !strings.Contains(line, "IDX:") {
		return nil, false
	}
	// Nodes=hk01dgx038 or Nodes=hk01dgx[011,046] ...
	nodeName := ""
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "Nodes=") {
			nodeName = strings.TrimPrefix(f, "Nodes=")
			break
		}
	}
	if nodeName == "" {
		return nil, false
	}
	// GRES=gpu:2(IDX:0-1) or GRES=gpu:2(IDX:6-7) or IDX:0,2,4
	idxStart := strings.Index(line, "IDX:")
	if idxStart < 0 {
		return nil, false
	}
	idxStr := line[idxStart+4:]
	if end := strings.Index(idxStr, ")"); end >= 0 {
		idxStr = idxStr[:end]
	}
	idxStr = strings.TrimSpace(idxStr)
	indices := parseIDXRange(idxStr)
	if len(indices) == 0 {
		return nil, false
	}
	nodes := expandSlurmNodelist(nodeName)
	if len(nodes) == 0 {
		nodes = []string{nodeName}
	}
	result := make([]NodeGPUs, 0, len(nodes))
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		result = append(result, NodeGPUs{Node: node, GPUs: indices})
	}
	return result, len(result) > 0
}

// parseIDXRange parses Slurm IDX spec like "0-1", "6-7", "0,1,2", "0,2-4,6" into a slice of integers.
func parseIDXRange(s string) []int {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			seg := strings.SplitN(part, "-", 2)
			if len(seg) != 2 {
				continue
			}
			start, err1 := strconv.Atoi(strings.TrimSpace(seg[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(seg[1]))
			if err1 != nil || err2 != nil || start > end {
				continue
			}
			for i := start; i <= end; i++ {
				out = append(out, i)
			}
		} else {
			if n, err := strconv.Atoi(part); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

// JobGPUUtil is a job's GPU utilization from DCGM (-1 if DCGM exporter unreachable).
type JobGPUUtil struct {
	JobID     string
	JobName   string
	NodeList  string
	Partition string
	UtilPct   float64 // 0-100 or -1 if unavailable
}

// JobGPUMemory is a job's GPU memory usage from DCGM (-1 if DCGM exporter unreachable).
type JobGPUMemory struct {
	JobID     string
	JobName   string
	NodeList  string
	Partition string
	MemoryMB  float64 // Sum of memory used (MB) on allocated GPUs, or -1 if unavailable
}

// nodePartitionGPUs represents one node, the partitions it belongs to, and how many GPUs it has.
type nodePartitionGPUs struct {
	Node        string
	Partitions  []string
	GPUCount    int
	Unavailable bool
}

func isUnavailableNodeState(state string) bool {
	state = strings.ToLower(strings.TrimSpace(state))
	return strings.Contains(state, "drain") || strings.Contains(state, "down") || strings.Contains(state, "inval") || strings.Contains(state, "maint")
}

func allocatedGPUsFromJobAllocs(jobs []JobGPUAlloc) float64 {
	var total float64
	for _, a := range jobs {
		total += a.GPUs
	}
	return total
}

func jobGroupsFromJobAllocs(jobs []JobGPUAlloc) map[string]float64 {
	out := make(map[string]float64)
	for _, jobGroupName := range defaultJobGroupName {
		out[jobGroupName] = 0
	}
	for _, a := range jobs {
		jobGroup := extractJobGroup(a.JobName)
		if jobGroup != "" {
			out[jobGroup] += a.GPUs
		}
	}
	return out
}

func totalGPUsFromNodes(nodes []nodePartitionGPUs) float64 {
	var total float64
	for _, n := range nodes {
		total += float64(n.GPUCount)
	}
	return total
}

func unavailableGPUsFromNodes(nodes []nodePartitionGPUs) float64 {
	var total float64
	for _, n := range nodes {
		if n.Unavailable {
			total += float64(n.GPUCount)
		}
	}
	return total
}

func gpuMetricsFromSnapshot(snapshot *gpuScrapeSnapshot) *GPUsMetrics {
	var gm GPUsMetrics
	if snapshot == nil {
		return &gm
	}
	totalGPUs := totalGPUsFromNodes(snapshot.nodes())
	allocatedGPUs := allocatedGPUsFromJobAllocs(snapshot.jobAllocs)
	unavailableGPUs := unavailableGPUsFromNodes(snapshot.nodes())
	gm.alloc = allocatedGPUs
	gm.idle = totalGPUs - allocatedGPUs - unavailableGPUs
	gm.total = totalGPUs
	gm.unavailable = unavailableGPUs
	if totalGPUs > 0 {
		gm.utilization = allocatedGPUs / totalGPUs
	}
	return &gm
}

// JobGroupGPUUtil is a job group's GPU utilization from DCGM (-1 if DCGM exporter unreachable).
// A "job group" is derived from the job name prefix before the first '-' (same as ParseGPUsByJobGroup).
type JobGroupGPUUtil struct {
	JobGroup string
	UtilPct  float64 // 0-100 or -1 if unavailable
}

// JobGroupGPUMemory is a job group's average GPU memory used (MB) from DCGM_FI_DEV_FB_USED.
type JobGroupGPUMemory struct {
	JobGroup string
	MemoryMB float64 // average MB across GPUs used by the job group
}

// GetJobGPUUtilFromDCGM returns per-job GPU utilization by scraping DCGM exporter on each job's nodes, averaging only the GPUs allocated to the job (from scontrol show job -d). -1 if any node's exporter is unreachable.
func GetJobGPUUtilFromDCGM() []JobGPUUtil {
	return getJobGPUUtilFromSnapshot(newGPUScrapeSnapshot())
}

func getJobGPUUtilFromSnapshot(snapshot *gpuScrapeSnapshot) []JobGPUUtil {
	jobs := snapshot.jobAllocs
	out := make([]JobGPUUtil, 0, len(jobs))
	for _, a := range jobs {
		nodeGPUs := snapshot.allocatedGPUs(a.JobID)
		if len(nodeGPUs) == 0 {
			out = append(out, JobGPUUtil{JobID: a.JobID, JobName: a.JobName, NodeList: a.NodeList, Partition: a.Partition, UtilPct: -1})
			continue
		}
		// INFO[0011] job 69778 nodeGPUs [{Node:hk01dgx038 GPUs:[0 1]} {Node:hk01dgx054 GPUs:[6 7]}]  source="gpus.go:535"
		// log.Infof("job %s nodeGPUs %+v", a.JobID, nodeGPUs)
		var sum float64
		var count int
		allOk := true
		var expandedNodes []string
		for _, ng := range nodeGPUs {
			nodeName := strings.TrimSpace(ng.Node)
			if nodeName == "" || len(ng.GPUs) == 0 {
				continue
			}
			expandedNodes = append(expandedNodes, nodeName)
			perGPU, err := snapshot.dcgm.gpuUtilPerGPU(nodeName)
			if err != nil {
				allOk = false
				break
			}
			for _, gpuIdx := range ng.GPUs {
				gpuKey := strconv.Itoa(gpuIdx)
				if u, ok := perGPU[gpuKey]; ok {
					sum += u
					count++
					// log.Infof("job %s node %s gpu %s util %f", a.JobID, nodeName, gpuKey, u)
				}
			}
		}
		utilVal := -1.0
		if allOk && count > 0 {
			utilVal = sum / float64(count)
		}
		expandedList := a.NodeList
		if len(expandedNodes) > 0 {
			expandedList = strings.Join(expandedNodes, ",")
		}
		out = append(out, JobGPUUtil{
			JobID:     a.JobID,
			JobName:   a.JobName,
			NodeList:  expandedList,
			Partition: a.Partition,
			UtilPct:   utilVal,
		})
	}
	return out
}

// GetJobGroupGPUUtilFromDCGM returns per-job-group GPU utilization by scraping DCGM exporter on each job's nodes,
// averaging only the GPUs allocated to the jobs in the group. If any job in the group has unknown DCGM data,
// that job is simply skipped; groups with no usable GPUs are omitted.
func GetJobGroupGPUUtilFromDCGM() []JobGroupGPUUtil {
	return getJobGroupGPUUtilFromSnapshot(newGPUScrapeSnapshot())
}

func getJobGroupGPUUtilFromSnapshot(snapshot *gpuScrapeSnapshot) []JobGroupGPUUtil {
	// Aggregate per job group over GPUs.
	type agg struct {
		sum   float64
		count int
	}
	groupAgg := make(map[string]*agg)

	for _, job := range snapshot.jobAllocs {
		jobName := strings.TrimSpace(job.JobName)
		if jobName == "" {
			continue
		}
		jobGroup := extractJobGroup(jobName)
		if jobGroup == "" {
			continue
		}

		var sum float64
		var count int
		for _, ng := range snapshot.allocatedGPUs(job.JobID) {
			nodeName := strings.TrimSpace(ng.Node)
			if nodeName == "" || len(ng.GPUs) == 0 {
				continue
			}
			perGPU, err := snapshot.dcgm.gpuUtilPerGPU(nodeName)
			if err != nil {
				log.Warnf("DCGM exporter scrape failed on node %s for job %s: %v", nodeName, job.JobID, err)
				continue
			}
			for _, gpuIdx := range ng.GPUs {
				gpuKey := strconv.Itoa(gpuIdx)
				if u, ok := perGPU[gpuKey]; ok {
					sum += u
					count++
				}
			}
		}
		if count == 0 {
			continue
		}
		a, ok := groupAgg[jobGroup]
		if !ok {
			a = &agg{}
			groupAgg[jobGroup] = a
		}
		a.sum += sum
		a.count += count
	}

	out := make([]JobGroupGPUUtil, 0, len(groupAgg))
	for g, a := range groupAgg {
		if a.count == 0 {
			continue
		}
		out = append(out, JobGroupGPUUtil{
			JobGroup: g,
			UtilPct:  a.sum / float64(a.count),
		})
	}
	return out
}

// GetJobGroupGPUMemoryFromDCGM returns per-job-group average GPU memory used (MB) by scraping DCGM_FI_DEV_FB_USED
// from the DCGM exporter. Flow: (1) Identify which GPUs each job group is using (node + device via scontrol show job -d);
// (2) read GPU memory used from DCGM for those GPUs; (3) average per job group. Groups with no usable data are omitted.
func GetJobGroupGPUMemoryFromDCGM() []JobGroupGPUMemory {
	return getJobGroupGPUMemoryFromSnapshot(newGPUScrapeSnapshot())
}

func getJobGroupGPUMemoryFromSnapshot(snapshot *gpuScrapeSnapshot) []JobGroupGPUMemory {
	type agg struct {
		sum   float64
		count int
	}
	groupAgg := make(map[string]*agg)

	for _, job := range snapshot.jobAllocs {
		jobName := strings.TrimSpace(job.JobName)
		if jobName == "" {
			continue
		}
		jobGroup := extractJobGroup(jobName)
		if jobGroup == "" {
			continue
		}
		var sum float64
		var count int
		for _, ng := range snapshot.allocatedGPUs(job.JobID) {
			nodeName := strings.TrimSpace(ng.Node)
			if nodeName == "" || len(ng.GPUs) == 0 {
				continue
			}
			perGPU, err := snapshot.dcgm.gpuMemoryPerGPU(nodeName)
			if err != nil {
				log.Warnf("DCGM exporter scrape failed on node %s (job group GPU memory): %v", nodeName, err)
				continue
			}
			for _, gpuIdx := range ng.GPUs {
				gpuKey := strconv.Itoa(gpuIdx)
				if mb, ok := perGPU[gpuKey]; ok {
					sum += mb
					count++
				}
			}
		}
		if count == 0 {
			continue
		}
		a, ok := groupAgg[jobGroup]
		if !ok {
			a = &agg{}
			groupAgg[jobGroup] = a
		}
		a.sum += sum
		a.count += count
	}

	out := make([]JobGroupGPUMemory, 0, len(groupAgg))
	for g, a := range groupAgg {
		if a.count == 0 {
			continue
		}
		out = append(out, JobGroupGPUMemory{
			JobGroup: g,
			MemoryMB: a.sum / float64(a.count),
		})
	}
	return out
}

// GetJobGPUMemoryUtilFromDCGM returns average per-job GPU memory usage (DCGM_FI_DEV_FB_USED in MB) over allocated GPUs. -1 if any node's DCGM exporter is unreachable.
func GetJobGPUMemoryUtilFromDCGM() []JobGPUMemory {
	return getJobGPUMemoryUtilFromSnapshot(newGPUScrapeSnapshot())
}

func getJobGPUMemoryUtilFromSnapshot(snapshot *gpuScrapeSnapshot) []JobGPUMemory {
	jobs := snapshot.jobAllocs
	out := make([]JobGPUMemory, 0, len(jobs))
	for _, a := range jobs {
		nodeGPUs := snapshot.allocatedGPUs(a.JobID)
		if len(nodeGPUs) == 0 {
			out = append(out, JobGPUMemory{JobID: a.JobID, JobName: a.JobName, NodeList: a.NodeList, Partition: a.Partition, MemoryMB: -1})
			continue
		}
		var memorySum float64
		var count int
		allOk := true
		var expandedNodes []string
		for _, ng := range nodeGPUs {
			nodeName := strings.TrimSpace(ng.Node)
			if nodeName == "" || len(ng.GPUs) == 0 {
				continue
			}
			expandedNodes = append(expandedNodes, nodeName)
			perGPU, err := snapshot.dcgm.gpuMemoryPerGPU(nodeName)
			if err != nil {
				allOk = false
				break
			}
			for _, gpuIdx := range ng.GPUs {
				gpuKey := strconv.Itoa(gpuIdx)
				if mb, ok := perGPU[gpuKey]; ok {
					memorySum += mb
					count++
				}
			}
		}
		memVal := -1.0
		if allOk && count > 0 {
			memVal = memorySum / float64(count)
		}
		expandedList := a.NodeList
		if len(expandedNodes) > 0 {
			expandedList = strings.Join(expandedNodes, ",")
		}
		out = append(out, JobGPUMemory{
			JobID:     a.JobID,
			JobName:   a.JobName,
			NodeList:  expandedList,
			Partition: a.Partition,
			MemoryMB:  memVal,
		})
	}
	return out
}

// parseNodePartitionGPUs parses "sinfo -N -h -o %N %P %T %G" output into per-node GPU/partition info.
// Example line: "hk01dgx038 gpu,gpu-long idle gpu:8(S:0-1)"
func parseNodePartitionGPUs(text string) []nodePartitionGPUs {
	var result []nodePartitionGPUs
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		nodeName := fields[0]
		partitionsField := fields[1]
		stateField := fields[2]
		gresField := fields[3]

		// Extract GPU count from GRES field "gpu:<n>(...)".
		gresField = strings.TrimSpace(gresField)
		if !strings.HasPrefix(gresField, "gpu:") {
			continue
		}
		descriptor := strings.TrimPrefix(gresField, "gpu:")
		descriptor = strings.Split(descriptor, "(")[0]
		gpuCount, err := strconv.Atoi(descriptor)
		if err != nil || gpuCount <= 0 {
			continue
		}

		// Parse partitions (may be comma-separated).
		var partitions []string
		for _, p := range strings.Split(partitionsField, ",") {
			p = strings.TrimSpace(p)
			if p != "" && p != "none" {
				partitions = append(partitions, p)
			}
		}
		if len(partitions) == 0 {
			continue
		}

		// If a GPU partition filter is set, only keep partitions that match it.
		if gpuPartitionsFilter != "" {
			allowed := make(map[string]struct{})
			for _, p := range strings.Split(gpuPartitionsFilter, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					allowed[p] = struct{}{}
				}
			}
			var filtered []string
			for _, p := range partitions {
				if _, ok := allowed[p]; ok {
					filtered = append(filtered, p)
				}
			}
			if len(filtered) == 0 {
				continue
			}
			partitions = filtered
		}

		result = append(result, nodePartitionGPUs{
			Node:        nodeName,
			Partitions:  partitions,
			GPUCount:    gpuCount,
			Unavailable: isUnavailableNodeState(stateField),
		})
	}
	return result
}

func fetchGPUNodeSinfo() []nodePartitionGPUs {
	sinfoArgs := []string{"-N", "-h", "-o", "%N %P %T %G"}
	if gpuPartitionsFilter != "" {
		sinfoArgs = append(sinfoArgs, "-p", gpuPartitionsFilter)
	}
	raw, err := execute("sinfo", sinfoArgs)
	if err != nil {
		log.Warnf("sinfo failed (GPU node info): %v", err)
		return nil
	}
	return parseNodePartitionGPUs(string(raw))
}

// GetPartitionAvgGPUUtilFromDCGM returns average GPU utilization (0-100) over all GPUs in each partition.
// Flow: (1) Identify available nodes in the partition via sinfo -N -h -o "%N %P %T %G"; (2) for each node, iterate
// its GPUs and read DCGM_FI_DEV_GPU_UTIL from the DCGM exporter; (3) aggregate and compute the average per partition.
// gpuPartitionsFilter (if set) restricts which partitions are considered (sinfo -p).
func GetPartitionAvgGPUUtilFromDCGM() map[string]float64 {
	return getPartitionAvgGPUUtilFromSnapshot(&gpuScrapeSnapshot{
		dcgm:     newDCGMScrapeCache(),
		gpuNodes: fetchGPUNodeSinfo(),
	})
}

func getPartitionAvgGPUUtilFromSnapshot(snapshot *gpuScrapeSnapshot) map[string]float64 {
	out := make(map[string]float64)

	// Partition -> (sum of GPU utilizations, GPU count).
	type agg struct {
		sum   float64
		count int
	}
	partAgg := make(map[string]*agg)

	if snapshot == nil {
		return out
	}

	// 2. For each node, iterate its GPUs and read DCGM_FI_DEV_GPU_UTIL; 3. aggregate per partition.
	for _, n := range snapshot.nodes() {
		nodeName := strings.TrimSpace(n.Node)
		if nodeName == "" || n.GPUCount <= 0 || n.Unavailable {
			continue
		}
		perGPU, err := snapshot.dcgm.gpuUtilPerGPU(nodeName) // reads DCGM_FI_DEV_GPU_UTIL per GPU
		if err != nil {
			log.Warnf("DCGM exporter scrape failed on node %s: %v", nodeName, err)
			continue
		}
		for _, u := range perGPU {
			for _, p := range n.Partitions {
				a, ok := partAgg[p]
				if !ok {
					a = &agg{}
					partAgg[p] = a
				}
				a.sum += u
				a.count++
			}
		}
	}

	for p, a := range partAgg {
		if a.count > 0 {
			out[p] = a.sum / float64(a.count)
		}
	}
	return out
}

// GetPartitionAllocatedAvgGPUUtilFromDCGM returns average GPU utilization (0-100) over only the GPUs
// that are currently allocated to running jobs in each partition. Flow: (1) Identify running jobs (with
// GPU allocation); (2) for each job, identify which GPUs it uses (node + device index via scontrol show job -d);
// (3) read DCGM_FI_DEV_GPU_UTIL from DCGM for those GPUs; (4) aggregate and compute the average per partition.
func GetPartitionAllocatedAvgGPUUtilFromDCGM() map[string]float64 {
	return getPartitionAllocatedAvgGPUUtilFromSnapshot(newGPUScrapeSnapshot())
}

func getPartitionAllocatedAvgGPUUtilFromSnapshot(snapshot *gpuScrapeSnapshot) map[string]float64 {
	out := make(map[string]float64)
	type agg struct {
		sum   float64
		count int
	}
	partAgg := make(map[string]*agg)

	// 1. Identify running jobs (with GPU allocation).
	for _, a := range snapshot.jobAllocs {
		partition := strings.TrimSpace(a.Partition)
		if partition == "" {
			continue
		}
		// 2. Identify which GPUs this job is using (e.g. hk01dgx001, device 1).
		nodeGPUs := snapshot.allocatedGPUs(a.JobID)
		if len(nodeGPUs) == 0 {
			continue
		}
		if partAgg[partition] == nil {
			partAgg[partition] = &agg{}
		}
		// 3. Read GPU util from DCGM for each allocated GPU; aggregate by partition.
		for _, ng := range nodeGPUs {
			nodeName := strings.TrimSpace(ng.Node)
			if nodeName == "" || len(ng.GPUs) == 0 {
				continue
			}
			perGPU, err := snapshot.dcgm.gpuUtilPerGPU(nodeName)
			if err != nil {
				log.Warnf("DCGM exporter scrape failed on node %s (partition allocated util): %v", nodeName, err)
				continue
			}
			for _, gpuIdx := range ng.GPUs {
				gpuKey := strconv.Itoa(gpuIdx)
				if u, ok := perGPU[gpuKey]; ok {
					partAgg[partition].sum += u
					partAgg[partition].count++
				}
			}
		}
	}

	for p, a := range partAgg {
		if a != nil && a.count > 0 {
			out[p] = a.sum / float64(a.count)
		}
	}
	return out
}

func ParseTotalGPUs() float64 {
	return totalGPUsFromNodes(fetchGPUNodeSinfo())
}

// ParseUnavailableGPUs returns the number of GPUs on nodes in drain, down, inval, or maint state.
// Uses: sinfo -N -h -o "%N %T %G" and filters for states containing drain, down, inval, or maint.
func ParseUnavailableGPUs() float64 {
	return unavailableGPUsFromNodes(fetchGPUNodeSinfo())
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	nodes := fetchGPUNodeSinfo()
	total_gpus := totalGPUsFromNodes(nodes)
	allocated_gpus := ParseAllocatedGPUs()
	unavailable_gpus := unavailableGPUsFromNodes(nodes)
	gm.alloc = allocated_gpus
	gm.idle = total_gpus - allocated_gpus - unavailable_gpus
	gm.total = total_gpus
	gm.unavailable = unavailable_gpus
	if total_gpus > 0 {
		gm.utilization = allocated_gpus / total_gpus
	}
	return &gm
}

// execute runs a command and returns stdout; on failure returns error and stderr in message.
func execute(command string, arguments []string) ([]byte, error) {
	cmd := exec.Command(command, arguments...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return stdout.Bytes(), fmt.Errorf("%w: %s", err, msg)
	}
	return stdout.Bytes(), nil
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc:             prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:              prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total:             prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization:       prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
		unavailable:       prometheus.NewDesc("slurm_gpus_unavailable", "GPUs on nodes in drain, down, inval, or maint state", nil, nil),
		byJobGroup:        prometheus.NewDesc("slurm_gpus_by_job_group", "Number of GPUs in use by job group (sum of all running jobs with that group)", []string{"job_group"}, nil),
		allocToJob:        prometheus.NewDesc("slurm_gpus_allocated_to_job", "GPUs allocated to a specific job (job_id, job_name, nodes, partition)", []string{"job_id", "job_name", "nodes", "partition"}, nil),
		jobGPUUtil:        prometheus.NewDesc("slurm_job_gpu_utilization_pct", "GPU utilization (0-100) for the job from DCGM exporter on its nodes; -1 if DCGM exporter unreachable", []string{"job_id", "job_name", "nodes", "partition"}, nil),
		jobGPUMemory:      prometheus.NewDesc("slurm_job_gpu_memory_used_mb", "Average GPU memory used (MB, DCGM_FI_DEV_FB_USED) for the job on its allocated GPUs; -1 if DCGM exporter unreachable", []string{"job_id", "job_name", "nodes", "partition"}, nil),
		partGPUUtil:       prometheus.NewDesc("slurm_partition_gpu_utilization_pct", "Average GPU utilization (0-100) over all GPUs in the partition from DCGM_FI_DEV_GPU_UTIL", []string{"partition"}, nil),
		partAllocGPUUtil:  prometheus.NewDesc("slurm_partition_allocated_gpu_utilization_pct", "Average GPU utilization (0-100) over allocated GPUs in the partition from DCGM_FI_DEV_GPU_UTIL (running jobs only)", []string{"partition"}, nil),
		jobGroupGPUUtil:   prometheus.NewDesc("slurm_job_group_gpu_utilization_pct", "Average GPU utilization (0-100) over all GPUs in the job group from DCGM exporter", []string{"job_group"}, nil),
		jobGroupGPUMemory: prometheus.NewDesc("slurm_job_group_gpu_memory_used_mb", "Average GPU memory used (MB, DCGM_FI_DEV_FB_USED) over all GPUs in the job group", []string{"job_group"}, nil),
	}
}

type GPUsCollector struct {
	alloc             *prometheus.Desc
	idle              *prometheus.Desc
	total             *prometheus.Desc
	utilization       *prometheus.Desc
	unavailable       *prometheus.Desc
	byJobGroup        *prometheus.Desc
	allocToJob        *prometheus.Desc
	jobGPUUtil        *prometheus.Desc
	jobGPUMemory      *prometheus.Desc
	partGPUUtil       *prometheus.Desc
	partAllocGPUUtil  *prometheus.Desc
	jobGroupGPUUtil   *prometheus.Desc
	jobGroupGPUMemory *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
	ch <- cc.unavailable
	ch <- cc.byJobGroup
	ch <- cc.allocToJob
	ch <- cc.jobGPUUtil
	ch <- cc.jobGPUMemory
	ch <- cc.partGPUUtil
	ch <- cc.partAllocGPUUtil
	ch <- cc.jobGroupGPUUtil
	ch <- cc.jobGroupGPUMemory
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot := newGPUScrapeSnapshot()
	cm := gpuMetricsFromSnapshot(snapshot)
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
	ch <- prometheus.MustNewConstMetric(cc.unavailable, prometheus.GaugeValue, cm.unavailable)
	for jobGroup, gpus := range jobGroupsFromJobAllocs(snapshot.jobAllocs) {
		ch <- prometheus.MustNewConstMetric(cc.byJobGroup, prometheus.GaugeValue, gpus, jobGroup)
	}
	for _, a := range snapshot.jobAllocs {
		ch <- prometheus.MustNewConstMetric(cc.allocToJob, prometheus.GaugeValue, a.GPUs, a.JobID, a.JobName, a.NodeList, a.Partition)
	}
	for _, u := range getJobGPUUtilFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.jobGPUUtil, prometheus.GaugeValue, u.UtilPct, u.JobID, u.JobName, u.NodeList, u.Partition)
	}
	for _, m := range getJobGPUMemoryUtilFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.jobGPUMemory, prometheus.GaugeValue, m.MemoryMB, m.JobID, m.JobName, m.NodeList, m.Partition)
	}
	for partition, util := range getPartitionAvgGPUUtilFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.partGPUUtil, prometheus.GaugeValue, util, partition)
	}
	for partition, util := range getPartitionAllocatedAvgGPUUtilFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.partAllocGPUUtil, prometheus.GaugeValue, util, partition)
	}
	for _, g := range getJobGroupGPUUtilFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.jobGroupGPUUtil, prometheus.GaugeValue, g.UtilPct, g.JobGroup)
	}
	for _, m := range getJobGroupGPUMemoryFromSnapshot(snapshot) {
		ch <- prometheus.MustNewConstMetric(cc.jobGroupGPUMemory, prometheus.GaugeValue, m.MemoryMB, m.JobGroup)
	}
}
