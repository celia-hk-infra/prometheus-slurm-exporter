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
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"net/http"
	"time"
)

func init() {
	// Metrics have to be registered to be exposed
	prometheus.MustRegister(NewAccountsCollector())       // from accounts.go
	prometheus.MustRegister(NewCPUsCollector())           // from cpus.go
	prometheus.MustRegister(NewNodesCollector())          // from nodes.go
	prometheus.MustRegister(NewNodeCollector())           // from node.go
	prometheus.MustRegister(NewNodeJobsCollector())       // from node_jobs.go
	prometheus.MustRegister(NewPartitionsCollector())     // from partitions.go
	prometheus.MustRegister(NewQueueCollector())          // from queue.go
	prometheus.MustRegister(NewSchedulerCollector())      // from scheduler.go
	prometheus.MustRegister(NewFairShareCollector())      // from sshare.go
	prometheus.MustRegister(NewUsersCollector())          // from users.go
}

var listenAddress = flag.String(
	"listen-address",
	":8080",
	"The address to listen on for HTTP requests.")

var gpuAcct = flag.Bool(
	"gpus-acct",
	false,
	"Enable GPUs accounting")

var gpuPartitions = flag.String(
	"gpu-partitions",
	"",
	"Comma-separated partitions for GPU metrics and slurm_node_* (sinfo -p, sacct -r). Empty = all partitions.")

var dcgmPortFlag = flag.String(
	"dcgm-exporter-port",
	"9400",
	"Port on GPU nodes where DCGM exporter is exposed (for slurm_job_gpu_utilization_pct).")

var scratchPath = flag.String(
	"directory-path",
	"",
	"Path to monitor for scratch filesystem usage (df). Empty string disables scratch metrics.")

var completedJobs = flag.Bool(
	"completed-jobs",
	false,
	"Enable completed Slurm job snapshot metrics from sacct.")

var completedJobsLookback = flag.String(
	"completed-jobs-lookback",
	"168h",
	"How far back to query terminal jobs via sacct (e.g. 24h, 168h).")

var completedJobsCacheTTL = flag.String(
	"completed-jobs-cache-ttl",
	"720h",
	"TTL for in-memory completed job GPU average cache (e.g. 720h = 30d).")

var completedJobsSampleInterval = flag.String(
	"completed-jobs-sample-interval",
	"30s",
	"Sampling interval for running-job GPU utilization/memory accumulation (e.g. 15s, 30s, 1m).")

var completedJobsStates = flag.String(
	"completed-jobs-states",
	"COMPLETED,FAILED,CANCELLED,TIMEOUT,NODE_FAIL,PREEMPTED,OUT_OF_MEMORY",
	"Comma-separated terminal job states to include from sacct.")

var pendingJobs = flag.Bool(
	"pending-jobs",
	false,
	"Enable pending Slurm job detail metrics from squeue+scontrol.")

func main() {
	flag.Parse()

	SetGPUPartitions(*gpuPartitions)

	if *scratchPath != "" {
		ScratchPath = *scratchPath
		prometheus.MustRegister(NewScratchCollector())
	}

	if *gpuAcct {
		SetDCGMExporterPort(*dcgmPortFlag)
		prometheus.MustRegister(NewGPUsCollector()) // from gpus.go
	}
	if *completedJobs {
		lookback := parseDurationOrDefault(*completedJobsLookback, 168*time.Hour)
		cacheTTL := parseDurationOrDefault(*completedJobsCacheTTL, 720*time.Hour)
		sampleInterval := parseDurationOrDefault(*completedJobsSampleInterval, 30*time.Second)
		if err := validateCompletedJobsConfig(lookback, cacheTTL, sampleInterval); err != nil {
			log.Fatalf("invalid completed jobs config: %v", err)
		}
		prometheus.MustRegister(NewCompletedJobsCollector(lookback, cacheTTL, sampleInterval, *completedJobsStates))
	}
	if *pendingJobs {
		prometheus.MustRegister(NewPendingJobsCollector())
	}

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Infof("Starting Server: %s", *listenAddress)
	if *scratchPath != "" {
		log.Infof("Scratch path (df): %s", *scratchPath)
	}
	log.Infof("GPUs Accounting: %t", *gpuAcct)
	log.Infof("Completed Jobs Snapshot: %t", *completedJobs)
	log.Infof("Pending Jobs Snapshot: %t", *pendingJobs)
	if *gpuPartitions != "" {
		log.Infof("Partition filter (GPU metrics, slurm_node_*): %s", *gpuPartitions)
	}
	if *gpuAcct {
		log.Infof("DCGM exporter port (job GPU util): %s", *dcgmPortFlag)
	}
	if *completedJobs {
		log.Infof("Completed jobs lookback: %s", *completedJobsLookback)
		log.Infof("Completed jobs states: %s", *completedJobsStates)
		log.Infof("Completed jobs sample interval: %s", *completedJobsSampleInterval)
	}
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
