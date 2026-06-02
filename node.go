/* Copyright 2021 Chris Read

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
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

// NodeMetrics stores metrics for each node
type NodeMetrics struct {
	memAlloc   uint64
	memTotal   uint64
	cpuAlloc   uint64
	cpuIdle    uint64
	cpuOther   uint64
	cpuTotal   uint64
	nodeStatus string
	partitions []string
	gpuAlloc   uint64
	gpuTotal   uint64
}

func NodeGetMetrics() map[string]*NodeMetrics {
	out := scontrolNodeData()
	if len(bytes.TrimSpace(out)) == 0 {
		return map[string]*NodeMetrics{}
	}
	return ParseScontrolShowNode(out)
}

func parseGPUTotalFromGres(gres string) uint64 {
	gres = strings.TrimSpace(gres)
	if gres == "" || gres == "(null)" || strings.EqualFold(gres, "N/A") {
		return 0
	}
	idx := strings.Index(gres, "gpu:")
	if idx < 0 {
		return 0
	}
	rest := gres[idx+4:]
	end := len(rest)
	for i, c := range rest {
		if c == '(' || c == ',' || c == ' ' {
			end = i
			break
		}
	}
	if end <= 0 {
		return 0
	}
	n, err := strconv.ParseUint(rest[:end], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseGPUAllocFromTRES(tres string) uint64 {
	tres = strings.TrimSpace(tres)
	if tres == "" || tres == "(null)" || strings.EqualFold(tres, "N/A") {
		return 0
	}
	for _, part := range strings.Split(tres, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "gres/gpu=") {
			s := strings.TrimPrefix(part, "gres/gpu=")
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

func parsePartitionsField(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "(null)" || strings.EqualFold(s, "N/A") {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" && !strings.EqualFold(p, "none") {
			out = append(out, p)
		}
	}
	return out
}

// normalizeNodeState maps Slurm state to short labels (e.g. mixed -> mix).
func normalizeNodeState(stateLong string) string {
	s := strings.ToLower(strings.TrimSpace(stateLong))
	s = strings.TrimSuffix(s, "*")

	// Composite Slurm states like IDLE+DRAIN should export the drain state.
	parts := strings.Split(s, "+")
	for _, p := range parts {
		switch strings.TrimSpace(p) {
		case "drain", "drained", "draining":
			return "drain"
		}
	}

	base := strings.TrimSpace(parts[0])
	switch base {
	case "mixed":
		return "mix"
	case "allocated":
		return "alloc"
	case "completing":
		return "comp"
	case "idle":
		return "idle"
	case "down":
		return "down"
	case "drain", "drained", "draining":
		return "drain"
	case "fail":
		return "fail"
	case "error":
		return "err"
	case "maint", "maintained":
		return "maint"
	case "reserved", "resv":
		return "resv"
	case "planned":
		return "planned"
	case "power_down", "powering_down":
		return "power_down"
	case "power_up", "powering_up":
		return "power_up"
	default:
		return strings.ReplaceAll(strings.ReplaceAll(base, " ", "_"), "+", "_")
	}
}

func mergePartitionsUnique(existing, add []string) []string {
	seen := make(map[string]struct{})
	for _, p := range existing {
		seen[p] = struct{}{}
	}
	for _, p := range add {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			existing = append(existing, p)
		}
	}
	return existing
}

// nodePartitionLabels returns partition names for slurm_node_partition.
// Uses scontrol Partitions when present; otherwise falls back to -gpu-partitions
// (so e.g. slurm_node_partition{node="gpu001",partition="gpu"} appears when you pass -gpu-partitions=gpu).
func nodePartitionLabels(m *NodeMetrics) []string {
	if len(m.partitions) > 0 {
		return m.partitions
	}
	if gpuPartitionsFilter == "" {
		return nil
	}
	var out []string
	seen := make(map[string]struct{})
	for _, p := range strings.Split(gpuPartitionsFilter, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// ParseNodeMetrics takes sinfo output (parsable2 with | or legacy whitespace lines).
func ParseNodeMetrics(input []byte) map[string]*NodeMetrics {
	nodes := make(map[string]*NodeMetrics)
	lines := strings.Split(string(input), "\n")

	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var nodeName string
		var memAlloc, memTotal, cpuAlloc, cpuIdle, cpuOther, cpuTotal uint64
		var nodeStatus string
		var partitions []string
		var gpuTotal, gpuAlloc uint64

		if strings.Contains(line, "|") {
			parts := strings.Split(line, "|")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			if len(parts) < 5 {
				continue
			}
			nodeName = parts[0]
			memAlloc, _ = strconv.ParseUint(parts[1], 10, 64)
			memTotal, _ = strconv.ParseUint(parts[2], 10, 64)
			cpuInfo := strings.Split(parts[3], "/")
			if len(cpuInfo) >= 4 {
				cpuAlloc, _ = strconv.ParseUint(cpuInfo[0], 10, 64)
				cpuIdle, _ = strconv.ParseUint(cpuInfo[1], 10, 64)
				cpuOther, _ = strconv.ParseUint(cpuInfo[2], 10, 64)
				cpuTotal, _ = strconv.ParseUint(cpuInfo[3], 10, 64)
			}
			nodeStatus = parts[4]
			if len(parts) >= 6 {
				partitions = parsePartitionsField(parts[5])
			}
			if len(parts) >= 7 {
				gpuTotal = parseGPUTotalFromGres(parts[6])
			}
			if len(parts) >= 8 {
				gpuAlloc = parseGPUAllocFromTRES(parts[7])
			}
		} else {
			node := strings.Fields(line)
			if len(node) < 5 {
				continue
			}
			nodeName = node[0]
			nodeStatus = node[4]
			memAlloc, _ = strconv.ParseUint(node[1], 10, 64)
			memTotal, _ = strconv.ParseUint(node[2], 10, 64)
			cpuInfo := strings.Split(node[3], "/")
			if len(cpuInfo) >= 4 {
				cpuAlloc, _ = strconv.ParseUint(cpuInfo[0], 10, 64)
				cpuIdle, _ = strconv.ParseUint(cpuInfo[1], 10, 64)
				cpuOther, _ = strconv.ParseUint(cpuInfo[2], 10, 64)
				cpuTotal, _ = strconv.ParseUint(cpuInfo[3], 10, 64)
			}
			if len(node) >= 6 {
				partitions = parsePartitionsField(node[5])
			}
			if len(node) >= 7 {
				gpuTotal = parseGPUTotalFromGres(node[6])
			}
			if len(node) >= 8 {
				gpuAlloc = parseGPUAllocFromTRES(node[7])
			}
		}

		if nodeName == "" {
			continue
		}

		if existing, ok := nodes[nodeName]; ok {
			existing.partitions = mergePartitionsUnique(existing.partitions, partitions)
			existing.memAlloc = memAlloc
			existing.memTotal = memTotal
			existing.cpuAlloc = cpuAlloc
			existing.cpuIdle = cpuIdle
			existing.cpuOther = cpuOther
			existing.cpuTotal = cpuTotal
			existing.nodeStatus = nodeStatus
			if gpuTotal > 0 {
				existing.gpuTotal = gpuTotal
			}
			if gpuAlloc > existing.gpuAlloc {
				existing.gpuAlloc = gpuAlloc
			}
			continue
		}

		nodes[nodeName] = &NodeMetrics{
			memAlloc: memAlloc, memTotal: memTotal,
			cpuAlloc: cpuAlloc, cpuIdle: cpuIdle, cpuOther: cpuOther, cpuTotal: cpuTotal,
			nodeStatus: nodeStatus,
			partitions: partitions,
			gpuAlloc:   gpuAlloc,
			gpuTotal:   gpuTotal,
		}
	}

	return nodes
}

var (
	nodeDataFailureOnce sync.Once
	reNodeBlockStart    = regexp.MustCompile(`(?m)^NodeName=(\S+)`)
	reSctlUint          = func(key string) *regexp.Regexp {
		return regexp.MustCompile(key + `=(\d+)`)
	}
	reSctlState      = regexp.MustCompile(`State=(\S+)`)
	reSctlPartitions = regexp.MustCompile(`Partitions=(\S+)`)
	reSctlGres        = regexp.MustCompile(`Gres=(\S+(?:\([^)]*\))*)`)
	reSctlAllocTRES   = regexp.MustCompile(`AllocTRES=(\S+)`)
	reSctlCfgTRES     = regexp.MustCompile(`CfgTRES=(\S+)`)
)

func sctlUint(block, key string) uint64 {
	re := reSctlUint(key)
	m := re.FindStringSubmatch(block)
	if len(m) < 2 {
		return 0
	}
	u, _ := strconv.ParseUint(m[1], 10, 64)
	return u
}

// parseGPUTotalFromGresString sums gpu:N segments in a Gres= field (e.g. gpu:8(S:0-1)).
func parseGPUTotalFromGresString(gres string) uint64 {
	var sum uint64
	s := gres
	for {
		i := strings.Index(s, "gpu:")
		if i < 0 {
			break
		}
		rest := s[i+4:]
		end := len(rest)
		for j, c := range rest {
			if c == '(' || c == ',' || c == ' ' || c == '+' {
				end = j
				break
			}
		}
		if end > 0 {
			if n, err := strconv.ParseUint(strings.TrimSpace(rest[:end]), 10, 64); err == nil {
				sum += n
			}
		}
		s = rest[end:]
	}
	return sum
}

func parseGresGPUFromTRES(tres string) uint64 {
	return parseGPUAllocFromTRES(tres)
}

// nodeMatchesGPUPartitionFilter returns true if the node should be included (empty filter = all).
func nodeMatchesGPUPartitionFilter(partitions []string) bool {
	if gpuPartitionsFilter == "" {
		return true
	}
	allowed := make(map[string]struct{})
	for _, p := range strings.Split(gpuPartitionsFilter, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			allowed[p] = struct{}{}
		}
	}
	for _, p := range partitions {
		if _, ok := allowed[p]; ok {
			return true
		}
	}
	return false
}

// ParseScontrolShowNode parses `scontrol show node` output into per-node metrics.
func ParseScontrolShowNode(data []byte) map[string]*NodeMetrics {
	nodes := make(map[string]*NodeMetrics)
	s := string(data)
	idxs := reNodeBlockStart.FindAllStringIndex(s, -1)
	if len(idxs) == 0 {
		return nodes
	}
	for i, loc := range idxs {
		end := len(s)
		if i+1 < len(idxs) {
			end = idxs[i+1][0]
		}
		block := strings.TrimSpace(s[loc[0]:end])
		if block == "" {
			continue
		}
		m := reNodeBlockStart.FindStringSubmatch(block)
		if len(m) < 2 {
			continue
		}
		name := m[1]
		cpuAlloc := sctlUint(block, "CPUAlloc")
		cpuTotal := sctlUint(block, "CPUTot")
		cpuErr := sctlUint(block, "CPUErr")
		memTotal := sctlUint(block, "RealMemory")
		memAlloc := sctlUint(block, "AllocMem")
		var nodeStatus string
		if sm := reSctlState.FindStringSubmatch(block); len(sm) >= 2 {
			nodeStatus = sm[1]
		}
		var partitions []string
		if pm := reSctlPartitions.FindStringSubmatch(block); len(pm) >= 2 {
			partitions = parsePartitionsField(pm[1])
		}
		gpuTotal := uint64(0)
		if gm := reSctlGres.FindStringSubmatch(block); len(gm) >= 2 {
			gpuTotal = parseGPUTotalFromGresString(gm[1])
		}
		if gpuTotal == 0 {
			if cm := reSctlCfgTRES.FindStringSubmatch(block); len(cm) >= 2 {
				gpuTotal = parseGresGPUFromTRES(cm[1])
			}
		}
		gpuAlloc := uint64(0)
		if am := reSctlAllocTRES.FindStringSubmatch(block); len(am) >= 2 {
			gpuAlloc = parseGresGPUFromTRES(strings.TrimSpace(am[1]))
		}
		cpuIdle := uint64(0)
		if cpuTotal > cpuAlloc+cpuErr {
			cpuIdle = cpuTotal - cpuAlloc - cpuErr
		}
		if !nodeMatchesGPUPartitionFilter(partitions) {
			continue
		}
		nodes[name] = &NodeMetrics{
			memAlloc: memAlloc, memTotal: memTotal,
			cpuAlloc: cpuAlloc, cpuIdle: cpuIdle, cpuOther: cpuErr, cpuTotal: cpuTotal,
			nodeStatus: nodeStatus,
			partitions: partitions,
			gpuAlloc:   gpuAlloc,
			gpuTotal:   gpuTotal,
		}
	}
	return nodes
}

func runScontrolShowNode() ([]byte, error) {
	cmd := exec.Command("scontrol", "show", "node")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return out, fmt.Errorf("%w: %s", err, msg)
	}
	return out, nil
}

func scontrolNodeData() []byte {
	out, err := runScontrolShowNode()
	if err != nil {
		nodeDataFailureOnce.Do(func() {
			log.Warnf("scontrol show node failed; slurm_node_* metrics will be empty. Example: %v", err)
		})
		return nil
	}
	return out
}
type NodeCollector struct {
	allocCPUs    *prometheus.Desc
	cpus         *prometheus.Desc
	allocMemory  *prometheus.Desc
	realMemory   *prometheus.Desc
	nodeState    *prometheus.Desc
	nodePart     *prometheus.Desc
	allocGPUs    *prometheus.Desc
	totalGPUs    *prometheus.Desc
}

// NewNodeCollector exposes per-node Slurm resource metrics for Prometheus.
func NewNodeCollector() *NodeCollector {
	return &NodeCollector{
		allocCPUs: prometheus.NewDesc("slurm_node_alloc_cpus",
			"Allocated CPUs on the node", []string{"node"}, nil),
		cpus: prometheus.NewDesc("slurm_node_cpus",
			"Total CPUs on the node", []string{"node"}, nil),
		allocMemory: prometheus.NewDesc("slurm_node_alloc_memory",
			"Allocated memory on the node (MB)", []string{"node"}, nil),
		realMemory: prometheus.NewDesc("slurm_node_real_memory",
			"Real memory on the node (MB)", []string{"node"}, nil),
		nodeState: prometheus.NewDesc("slurm_node_state",
			"Node state (1 = node is in this state)", []string{"node", "state"}, nil),
		nodePart: prometheus.NewDesc("slurm_node_partition",
			"1 if the node is in the partition (scontrol Partitions, or -gpu-partitions when empty)", []string{"node", "partition"}, nil),
		allocGPUs: prometheus.NewDesc("slurm_node_alloc_gpus",
			"Allocated GPUs on the node (scontrol AllocTRES gres/gpu)", []string{"node"}, nil),
		totalGPUs: prometheus.NewDesc("slurm_node_gpus",
			"Total GPUs on the node (scontrol Gres or CfgTRES gres/gpu)", []string{"node"}, nil),
	}
}

// Describe sends metric descriptions.
func (nc *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.allocCPUs
	ch <- nc.cpus
	ch <- nc.allocMemory
	ch <- nc.realMemory
	ch <- nc.nodeState
	ch <- nc.nodePart
	ch <- nc.allocGPUs
	ch <- nc.totalGPUs
}

// Collect emits per-node metrics.
func (nc *NodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := NodeGetMetrics()
	for name, m := range nodes {
		ch <- prometheus.MustNewConstMetric(nc.allocCPUs, prometheus.GaugeValue, float64(m.cpuAlloc), name)
		ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, float64(m.cpuTotal), name)
		ch <- prometheus.MustNewConstMetric(nc.allocMemory, prometheus.GaugeValue, float64(m.memAlloc), name)
		ch <- prometheus.MustNewConstMetric(nc.realMemory, prometheus.GaugeValue, float64(m.memTotal), name)
		st := normalizeNodeState(m.nodeStatus)
		ch <- prometheus.MustNewConstMetric(nc.nodeState, prometheus.GaugeValue, 1, name, st)
		partLabels := nodePartitionLabels(m)
		for _, p := range partLabels {
			ch <- prometheus.MustNewConstMetric(nc.nodePart, prometheus.GaugeValue, 1, name, p)
		}
		ch <- prometheus.MustNewConstMetric(nc.allocGPUs, prometheus.GaugeValue, float64(m.gpuAlloc), name)
		ch <- prometheus.MustNewConstMetric(nc.totalGPUs, prometheus.GaugeValue, float64(m.gpuTotal), name)
	}
}
