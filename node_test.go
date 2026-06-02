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
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeMetrics(t *testing.T) {
	data, err := ioutil.ReadFile("test_data/sinfo_mem.txt")
	if err != nil {
		t.Fatalf("Can not open test data: %v", err)
	}
	metrics := ParseNodeMetrics(data)
	t.Logf("%+v", metrics)

	assert.Contains(t, metrics, "b001")
	assert.Equal(t, uint64(327680), metrics["b001"].memAlloc)
	assert.Equal(t, uint64(386000), metrics["b001"].memTotal)
	assert.Equal(t, uint64(32), metrics["b001"].cpuAlloc)
	assert.Equal(t, uint64(0), metrics["b001"].cpuIdle)
	assert.Equal(t, uint64(0), metrics["b001"].cpuOther)
	assert.Equal(t, uint64(32), metrics["b001"].cpuTotal)
}

func TestNodeMetricsParsableGPU(t *testing.T) {
	line := "gpu001|409600|524288|96/32/0/128|mixed|gpu|gpu:8(S:0-1)|cpu=96,mem=409600,gres/gpu=6\n"
	metrics := ParseNodeMetrics([]byte(line))
	assert.Contains(t, metrics, "gpu001")
	g := metrics["gpu001"]
	assert.Equal(t, uint64(409600), g.memAlloc)
	assert.Equal(t, uint64(524288), g.memTotal)
	assert.Equal(t, uint64(96), g.cpuAlloc)
	assert.Equal(t, uint64(32), g.cpuIdle)
	assert.Equal(t, uint64(128), g.cpuTotal)
	assert.Equal(t, "mixed", g.nodeStatus)
	assert.Equal(t, []string{"gpu"}, g.partitions)
	assert.Equal(t, uint64(8), g.gpuTotal)
	assert.Equal(t, uint64(6), g.gpuAlloc)
}

func TestNormalizeNodeState(t *testing.T) {
	assert.Equal(t, "mix", normalizeNodeState("mixed"))
	assert.Equal(t, "mix", normalizeNodeState("MIXED*"))
	assert.Equal(t, "alloc", normalizeNodeState("allocated"))
	assert.Equal(t, "idle", normalizeNodeState("idle"))
}

func TestParseScontrolShowNode(t *testing.T) {
	prev := gpuPartitionsFilter
	defer func() { gpuPartitionsFilter = prev }()
	gpuPartitionsFilter = ""

	data, err := ioutil.ReadFile("test_data/scontrol_show_node.txt")
	if err != nil {
		t.Fatal(err)
	}
	n := ParseScontrolShowNode(data)
	assert.Contains(t, n, "gpu001")
	assert.Equal(t, uint64(96), n["gpu001"].cpuAlloc)
	assert.Equal(t, uint64(128), n["gpu001"].cpuTotal)
	assert.Equal(t, uint64(409600), n["gpu001"].memAlloc)
	assert.Equal(t, uint64(524288), n["gpu001"].memTotal)
	assert.Equal(t, uint64(6), n["gpu001"].gpuAlloc)
	assert.Equal(t, uint64(8), n["gpu001"].gpuTotal)
	assert.Contains(t, n["gpu001"].partitions, "gpu")
	assert.Contains(t, n, "gpu002")
	assert.Equal(t, uint64(0), n["gpu002"].gpuAlloc)
	assert.Equal(t, uint64(4), n["gpu002"].gpuTotal)
}

func TestParseScontrolShowNodePartitionFilter(t *testing.T) {
	prev := gpuPartitionsFilter
	defer func() { gpuPartitionsFilter = prev }()
	data, err := ioutil.ReadFile("test_data/scontrol_show_node.txt")
	if err != nil {
		t.Fatal(err)
	}
	gpuPartitionsFilter = "gpu-long"
	n := ParseScontrolShowNode(data)
	assert.Contains(t, n, "gpu001")
	assert.NotContains(t, n, "gpu002")
}

func TestNodePartitionLabels(t *testing.T) {
	prev := gpuPartitionsFilter
	defer func() { gpuPartitionsFilter = prev }()

	gpuPartitionsFilter = "gpu,gpu-long"
	assert.Equal(t, []string{"gpu", "gpu-long"}, nodePartitionLabels(&NodeMetrics{}))
	gpuPartitionsFilter = "gpu"
	assert.Equal(t, []string{"gpu"}, nodePartitionLabels(&NodeMetrics{}))
	assert.Equal(t, []string{"from-sinfo"}, nodePartitionLabels(&NodeMetrics{partitions: []string{"from-sinfo"}}))
}
