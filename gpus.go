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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strings"
	"strconv"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	output := string(Execute("sacct", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				for _, field := range strings.Split(line, ",") {
            				// Match gres/gpu=N or gres/gpu:<type>=N
            				if strings.HasPrefix(field, "gres/gpu=") || strings.HasPrefix(field, "gres/gpu:") {
                				// Find the '=' — everything after it is the count
                				eqIdx := strings.LastIndex(field, "=")
                				if eqIdx == -1 {
                    					continue
                				}
                				countStr := field[eqIdx+1:]
                
                				if count, err := strconv.ParseFloat(countStr, 64); err == nil {
                    					num_gpus += count
                				}
					}
				}
			}
		}
	}
	return num_gpus
}

func ParseTotalGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				fields := strings.Fields(line)
        			if len(fields) < 2 {
            				continue
        			}
        
        			gresField := fields[1]
        
        			// Skip nodes with no GRES: "(null)"
        			if !strings.HasPrefix(gresField, "gpu:") {
            				continue
        			}
        
        			// GRES format is one of:
        			//   gpu:N(S:cores)              - untyped
        			//   gpu:<type>:N(S:cores)       - typed (e.g., gpu:a100:8(S:...))
        			// A node line may also have multiple GRES separated by comma, e.g. "gpu:a100:4,gpu:a40:4"
        
        			for _, gres := range strings.Split(gresField, ",") {
            				if !strings.HasPrefix(gres, "gpu:") {
                				continue
            				}
            
            				// Strip trailing "(...)" if present
            				if idx := strings.Index(gres, "("); idx != -1 {
                				gres = gres[:idx]
            				}
            
            				// Now gres is either "gpu:N" or "gpu:<type>:N"
            				// The count is always the LAST colon-separated component
            				parts := strings.Split(gres, ":")
            				countStr := parts[len(parts)-1]
            
            				if count, err := strconv.ParseFloat(countStr, 64); err == nil {
                				num_gpus += count
            				}
        			}
			}
		}
	}

	return num_gpus
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	total_gpus := ParseTotalGPUs()
	allocated_gpus := ParseAllocatedGPUs()
	gm.alloc = allocated_gpus
	gm.idle = total_gpus - allocated_gpus
	gm.total = total_gpus
	gm.utilization = allocated_gpus / total_gpus
	return &gm
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total: prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
}
