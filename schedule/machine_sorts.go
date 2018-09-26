package main

import "sort"

func SortMachineByCpuCost(p []*Machine) {
	sort.Slice(p, func(i, j int) bool {
		return p[i].GetCpuCost() > p[j].GetCpuCost()
	})
}

func SortMachineByConfigAndCpuCost(p []*Machine) {
	sort.Slice(p, func(i, j int) bool {
		m1 := p[i]
		m2 := p[j]
		if m1.Config.Cpu == m2.Config.Cpu {
			return m1.GetCpuCost() > m2.GetCpuCost()
		} else {
			return m1.Config.Cpu > m2.Config.Cpu
		}

		return p[i].GetCpuCost() > p[j].GetCpuCost()
	})
}
