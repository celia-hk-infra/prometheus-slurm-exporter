/* Copyright 2017-2020 Victor Penso, Matteo Dessalvi, Joeri Hermans

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
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// ScratchPath is set from the -scratch-path flag (see main.go).
var ScratchPath string

type ScratchMetrics struct {
	sizeBytes  float64
	usedBytes  float64
	availBytes float64
	usePct     float64
}

func scratchData(path string) []byte {
	cmd := exec.Command("df", "-P", "-B1", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("scratch df stdout pipe: %v", err)
		return nil
	}
	if err := cmd.Start(); err != nil {
		log.Printf("scratch df start: %v", err)
		return nil
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Printf("scratch df wait: %v", err)
		return nil
	}
	return out
}

// Parse scratch df output. df -P -B1 gives: header line then one data line.
// Last 5 fields: Mounted on, Capacity (e.g. 89%), Available, Used, 1B-blocks (total).
var scratchSpaceRE = regexp.MustCompile(`\s+`)

func parseScratchMetrics(input []byte) *ScratchMetrics {
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	if len(lines) < 2 {
		return nil
	}
	fields := scratchSpaceRE.Split(strings.TrimSpace(lines[1]), -1)
	if len(fields) < 5 {
		return nil
	}
	// From the end: mount, capacity%, avail, used, total
	n := len(fields)
	totalStr := fields[n-5]
	usedStr := fields[n-4]
	availStr := fields[n-3]
	usePctStr := strings.TrimSuffix(fields[n-2], "%")

	total, err1 := strconv.ParseFloat(totalStr, 64)
	used, err2 := strconv.ParseFloat(usedStr, 64)
	avail, err3 := strconv.ParseFloat(availStr, 64)
	usePct, err4 := strconv.ParseFloat(usePctStr, 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return nil
	}
	return &ScratchMetrics{
		sizeBytes:  total,
		usedBytes:  used,
		availBytes: avail,
		usePct:     usePct,
	}
}

func ScratchGetMetrics(path string) *ScratchMetrics {
	if path == "" {
		return nil
	}
	data := scratchData(path)
	if data == nil {
		return nil
	}
	return parseScratchMetrics(data)
}

/*
 * Implement the Prometheus Collector interface for scratch/filesystem space.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewScratchCollector() *ScratchCollector {
	return &ScratchCollector{
		sizeBytes:  prometheus.NewDesc("scratch_size_bytes", "Total size of scratch filesystem in bytes", []string{"path"}, nil),
		usedBytes:  prometheus.NewDesc("scratch_used_bytes", "Used space on scratch filesystem in bytes", []string{"path"}, nil),
		availBytes: prometheus.NewDesc("scratch_avail_bytes", "Available space on scratch filesystem in bytes", []string{"path"}, nil),
		usePct:     prometheus.NewDesc("scratch_usage_percent", "Usage of scratch filesystem in percent (0-100)", []string{"path"}, nil),
	}
}

type ScratchCollector struct {
	sizeBytes  *prometheus.Desc
	usedBytes  *prometheus.Desc
	availBytes *prometheus.Desc
	usePct     *prometheus.Desc
}

func (sc *ScratchCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- sc.sizeBytes
	ch <- sc.usedBytes
	ch <- sc.availBytes
	ch <- sc.usePct
}

func (sc *ScratchCollector) Collect(ch chan<- prometheus.Metric) {
	if ScratchPath == "" {
		return
	}
	sm := ScratchGetMetrics(ScratchPath)
	if sm == nil {
		return
	}
	path := ScratchPath
	ch <- prometheus.MustNewConstMetric(sc.sizeBytes, prometheus.GaugeValue, sm.sizeBytes, path)
	ch <- prometheus.MustNewConstMetric(sc.usedBytes, prometheus.GaugeValue, sm.usedBytes, path)
	ch <- prometheus.MustNewConstMetric(sc.availBytes, prometheus.GaugeValue, sm.availBytes, path)
	ch <- prometheus.MustNewConstMetric(sc.usePct, prometheus.GaugeValue, sm.usePct, path)
}
