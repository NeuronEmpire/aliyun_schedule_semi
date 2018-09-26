package main

import "sort"

func SortInstanceByTotalMaxLowWithInference(p []*Instance, inferenceLimit int) {
	sort.Slice(p, func(i, j int) bool {
		c1 := p[i].Config
		c2 := p[j].Config
		a1 := float64(c1.Disk)/float64(LowDisk) + c1.CpuMax/float64(LowCpu*0.5) + c1.MemMax/float64(LowMem) +
			float64(c1.P)/float64(7) + float64(c1.M)/float64(3) + float64(c1.PM)/float64(7)
		a2 := float64(c2.Disk)/float64(LowDisk) + c2.CpuMax/float64(LowCpu*0.5) + c2.MemMax/float64(LowMem) +
			float64(c2.P)/float64(7) + float64(c2.M)/float64(3) + float64(c2.PM)/float64(7)

		if c1.InferenceAppCount < inferenceLimit && c2.InferenceAppCount < inferenceLimit {
			return a1 > a2
		}

		if c1.InferenceAppCount > c2.InferenceAppCount {
			return true
		} else if c1.InferenceAppCount == c2.InferenceAppCount {
			return a1 > a2
		} else {
			return false
		}
	})
}
