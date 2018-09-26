package main

func MachinesCopy(p []*Machine) (r []*Machine) {
	if p == nil {
		return nil
	}

	r = make([]*Machine, len(p))
	for i, v := range p {
		r[i] = v
	}

	return r
}

func MachinesContains(machines []*Machine, machineId int) bool {
	for _, v := range machines {
		if v.MachineId == machineId {
			return true
		}
	}

	return false
}

func MachinesRemove(machines []*Machine, removes []*Machine) (rest []*Machine) {
	rest = make([]*Machine, 0)
	for _, v := range machines {
		has := false
		for _, i := range removes {
			if i.MachineId == v.MachineId {
				has = true
				break
			}
		}
		if !has {
			rest = append(rest, v)
		}
	}

	return rest
}

func MachinesGetInstances(machines []*Machine) (instances []*Instance) {
	instances = make([]*Instance, 0)
	for _, m := range machines {
		instances = append(instances, m.InstanceList[:m.InstanceListCount]...)
	}
	return instances
}

func MachinesCloneWithInstances(p []*Machine) (r []*Machine) {
	r = make([]*Machine, len(p))
	for i, m := range p {
		machine := NewMachine(m.R, m.MachineId, m.Config)
		instances := InstancesCopy(m.InstanceList[:m.InstanceListCount])
		for _, instance := range instances {
			machine.AddInstance(instance)
		}
		machine.beginOffline()
		r[i] = machine
	}
	return r
}

func MachinesGetScore(machines []*Machine) (totalScore float64) {
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalScore += m.GetCpuCost()
		}
	}

	return totalScore
}

func MachinesGetScoreReal(machines []*Machine) (totalScore float64) {
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalScore += m.GetCpuCostReal()
		}
	}

	return totalScore
}
