package main

import (
	"fmt"
	"math"
)

type Resource struct {
	Cpu  [TimeSampleCount]float64
	Mem  [TimeSampleCount]float64
	Disk int
	P    int
	M    int
	PM   int

	CpuMax       float64
	CpuMin       float64
	CpuAvg       float64
	CpuDeviation float64
	MemMax       float64
	MemMin       float64
	MemAvg       float64
	MemDeviation float64

	cpuCostValid bool
	CpuCost      float64
}

func (r *Resource) AddResource(p *Resource) {
	for i, v := range p.Cpu {
		r.Cpu[i] += v
	}
	for i, v := range p.Mem {
		r.Mem[i] += v
	}
	r.Disk += p.Disk
	r.M += p.M
	r.P += p.P
	r.PM += p.PM

	r.cpuCostValid = false
}

func (r *Resource) RemoveResource(p *Resource) {
	for i, v := range p.Cpu {
		r.Cpu[i] -= v
	}
	for i, v := range p.Mem {
		r.Mem[i] -= v
	}
	r.Disk -= p.Disk
	r.M -= p.M
	r.P -= p.P
	r.PM -= p.PM

	r.cpuCostValid = false
}

func (r *Resource) CalcTimedResourceStatistics() {
	r.CpuAvg, r.CpuDeviation, r.CpuMin, r.CpuMax = Statistics(r.Cpu)
	r.MemAvg, r.MemDeviation, r.MemMin, r.MemMax = Statistics(r.Mem)
}

func (r *Resource) GetCpuDerivation() float64 {
	avg := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		ratio := r.Cpu[i] / HighCpu
		avg += ratio
	}

	avg = avg / float64(TimeSampleCount)
	d := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		ratio := r.Cpu[i] / HighCpu
		d += (ratio - avg) * (ratio - avg)
	}
	d = math.Sqrt(d / TimeSampleCount)

	return d
}

func (r *Resource) GetCpuCost(cpuLimit float64, instanceCount int) float64 {
	if r.cpuCostValid {
		return r.CpuCost
	}

	totalCost := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		r := r.Cpu[i] / cpuLimit
		if r > 0.5 {
			totalCost += 1 + (1+float64(instanceCount))*(Exp(r-0.5)-1)
		} else {
			totalCost += 1
		}
	}

	r.CpuCost = totalCost / TimeSampleCount
	r.cpuCostValid = true

	return r.CpuCost
}

func (r *Resource) GetLinearCpuCost(cpuLimit float64) float64 {
	totalCost := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		r := r.Cpu[i] / cpuLimit
		if r > 0.5 {
			totalCost += 1 + 10*(Exp(r-0.5)-1)
		} else {
			totalCost += r * 2
		}
	}

	return totalCost / TimeSampleCount
}

func (r *Resource) GetCostWithInstance(instance *Instance, cpuLimit float64) float64 {
	totalCost := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		r := (r.Cpu[i] + instance.Config.Cpu[i]) / cpuLimit
		if r > 0.5 {
			totalCost += 1 + 10*(Exp(r-0.5)-1)
		} else {
			totalCost += r * 2
		}
	}

	return totalCost / TimeSampleCount
}

func (r *Resource) DebugPrint() {
	r.CalcTimedResourceStatistics()

	fmt.Printf("Resource.Print ")
	fmt.Printf("Disk=%4d,", r.Disk)
	fmt.Printf("P=%d,M=%d,PM=%d,", r.P, r.M, r.PM)
	fmt.Printf("CpuAvg=%4.1f,CpuDev=%4.1f,CpuMin=%4.1f,CpuMax=%4.1f,",
		r.CpuAvg, r.CpuDeviation, r.CpuMin, r.CpuMax)
	fmt.Printf("MemAvg=%5.1f,MemDev=%5.1f,MemMin=%5.1f,MemMax=%5.1f\n",
		r.MemAvg, r.MemDeviation, r.MemMin, r.MemMax)
}
