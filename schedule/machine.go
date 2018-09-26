package main

import (
	"fmt"
	"math"
)

type Machine struct {
	Resource
	Cpu [TimeSampleCount * 15]float64
	Mem [TimeSampleCount * 15]float64

	R                  *ResourceManagement
	MachineId          int
	Config             *MachineConfig
	InstanceList       []*Instance
	InstanceListCount  int
	appCountCollection *AppCountCollection
	JobList            []*Job
	JobListCount       int

	cpuCost      float64 //延迟计算得分
	cpuCostValid bool    //延迟计算得分
}

func NewMachine(r *ResourceManagement, machineId int, config *MachineConfig) *Machine {
	m := &Machine{}
	m.R = r
	m.MachineId = machineId
	m.Config = config
	m.InstanceList = make([]*Instance, MaxInstancePerMachine)
	m.JobList = make([]*Job, MaxJobPerMachine)
	m.appCountCollection = NewAppCountCollection()

	return m
}

func (m *Machine) AddInstance(instance *Instance) {
	//debugLog("Machine.AddInstance %d %d", m.MachineId, instance.InstanceId)

	m.InstanceList[m.InstanceListCount] = instance
	m.InstanceListCount++
	m.appCountCollection.Add(instance.Config.AppId)
	m.AddResource(&instance.Config.Resource)

	m.cpuCostValid = false

	if DebugEnabled {
		//m.debugValidation()
	}
}

func (m *Machine) RemoveInstance(instanceId int) {
	//debugLog("Machine.RemoveInstance machineId=%d,instanceId=%d", m.MachineId, instanceId)
	has := false
	for i, instance := range m.InstanceList[:m.InstanceListCount] {
		if instance.InstanceId == instanceId {
			//debugLog("Machine.RemoveInstance appId=%d", instance.Config.AppId)
			m.InstanceList[i] = nil
			if m.InstanceListCount > 1 && i < m.InstanceListCount-1 {
				for j := i; j < m.InstanceListCount-1; j++ {
					m.InstanceList[j] = m.InstanceList[j+1]
				}
				m.InstanceList[m.InstanceListCount-1] = nil
			}

			m.InstanceListCount--
			m.appCountCollection.Remove(instance.Config.AppId)
			m.RemoveResource(&instance.Config.Resource)

			m.cpuCostValid = false
			has = true

			break
		}
	}

	if !has {
		panic(fmt.Sprintf("Machine.RemoveInstance failed,machineId=%d,instanceId=%d", m.MachineId, instanceId))
	}

	if DebugEnabled {
		//m.debugValidation()
	}
}

func (m *Machine) AddJob(job *Job) {
	m.JobList[m.JobListCount] = job
	m.JobListCount++
	for i := job.StartMinutes; i < job.StartMinutes+job.Config.ExecMinutes; i++ {
		//fmt.Println(m.Cpu[job.StartMinutes+i], job.Cpu, job.Config.Cpu,
		//	m.Mem[job.StartMinutes+i], job.Mem, job.Config.Mem, job.InstanceCount)
		m.Cpu[i] += job.Cpu
		m.Mem[i] += job.Mem
	}
	m.cpuCostValid = false
}

func (m *Machine) RemoveJob(jobInstanceId int) {
	for i, job := range m.JobList[:m.JobListCount] {
		if job.JobInstanceId == jobInstanceId {
			//debugLog("Machine.RemoveJob JobInstanceId=%d", jobInstanceId)
			m.JobList[i] = nil
			if m.JobListCount > 1 && i < m.JobListCount-1 {
				for j := i; j < m.JobListCount-1; j++ {
					m.JobList[j] = m.JobList[j+1]
				}
				m.JobList[m.JobListCount-1] = nil
			}
			m.JobListCount--
			for i := job.StartMinutes; i < job.StartMinutes+job.Config.ExecMinutes; i++ {
				m.Cpu[i] -= job.Cpu
				m.Mem[i] -= job.Mem
			}
			m.cpuCostValid = false
			break
		}
	}
}

//将实例的98点转换为任务的98*15点，提高性能
func (m *Machine) beginOffline() {
	for i, cpu := range m.Resource.Cpu {
		for j := 0; j < 15; j++ {
			m.Cpu[i*15+j] = cpu
		}
	}

	for i, mem := range m.Resource.Mem {
		for j := 0; j < 15; j++ {
			m.Mem[i*15+j] = mem
		}
	}
}

func (m *Machine) ConstraintCheck(instance *Instance, maxCpuRatio float64) bool {
	//debugLog("Machine.ConstraintCheck %s %s", m.MachineId, instance.InstanceId)

	if !ConstraintCheckResourceLimit(&m.Resource, &instance.Config.Resource, m.Config, maxCpuRatio) {
		//debugLog("Machine.ConstraintCheck constraintCheckResourceLimit failed")
		return false
	}

	if !ConstraintCheckAppInterferenceAddInstance(
		instance.Config.AppId,
		m.appCountCollection,
		m.R.AppInterferenceConfigMap) {
		//debugLog("Machine.ConstraintCheck constraintCheckAppInterferenceAddInstance failed")
		return false
	}

	return true
}

func (m *Machine) ConstraintCheckResourceLimit(instance *Instance, maxCpuRatio float64) bool {
	return ConstraintCheckResourceLimit(&m.Resource, &instance.Config.Resource, m.Config, maxCpuRatio)
}

func (m *Machine) ConstraintCheckAppInterferenceAddInstance(instance *Instance) bool {
	return ConstraintCheckAppInterferenceAddInstance(instance.Config.AppId,
		m.appCountCollection,
		m.R.AppInterferenceConfigMap)
}

func (m *Machine) HasBadConstraint() bool {
	return !ConstraintCheckAppInterference(m.appCountCollection, m.R.AppInterferenceConfigMap)
}

func (m *Machine) GetCpuCostReal() float64 {
	totalCost := float64(0)
	for i := 0; i < TimeSampleCount*15; i++ {
		r := m.Cpu[i] / m.Config.Cpu
		if r > 0.5 {
			totalCost += 1 + (1+float64(m.InstanceListCount))*(math.Exp(r-0.5)-1)
		} else {
			totalCost += 1
		}
	}

	return totalCost / (TimeSampleCount * 15)
}

func (m *Machine) GetCpuCost() float64 {
	if m.cpuCostValid {
		return m.cpuCost
	}
	m.cpuCostValid = true

	if m.JobListCount == 0 {
		m.cpuCost = m.Resource.GetCpuCost(m.Config.Cpu, m.InstanceListCount)
	} else {
		totalCost := float64(0)
		for i := 0; i < TimeSampleCount*15; i++ {
			r := m.Cpu[i] / m.Config.Cpu
			if r > 0.5 {
				totalCost += 1 + (1+float64(m.InstanceListCount))*(Exp(r-0.5)-1)
			} else {
				totalCost += 1
			}
		}
		m.cpuCost = totalCost / (TimeSampleCount * 15)
	}

	return m.cpuCost
}

func (m *Machine) CalcCpuCostOne(cpu float64) float64 {
	r := cpu / m.Config.Cpu
	if r > 0.5 {
		return 1 + (1+float64(m.InstanceListCount))*(Exp(r-0.5)-1)
	} else {
		return 1
	}
}

func (m *Machine) GetCostWithInstance(instance *Instance) float64 {
	totalCost := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		r := (m.Cpu[i] + instance.Config.Cpu[i]) / m.Config.Cpu
		if r > 0.5 {
			totalCost += 1 + 10*(Exp(r-0.5)-1)
		} else {
			totalCost += 1
		}
	}

	return totalCost / TimeSampleCount
}

func (m *Machine) GetLinearCostWithInstance(instance *Instance) float64 {
	totalCost := float64(0)
	for i := 0; i < TimeSampleCount; i++ {
		r := (m.Cpu[i] + instance.Config.Cpu[i]) / m.Config.Cpu
		if r > 0.5 {
			totalCost += 1 + 10*(Exp(r-0.5)-1)
		} else {
			totalCost += 2 * r
		}
	}
	totalCost = totalCost / TimeSampleCount

	return totalCost
}

func (m *Machine) debugValidation() {
	for i := 0; i < m.InstanceListCount; i++ {
		if m.InstanceList[i] == nil {
			panic(fmt.Errorf("Machine.debugValidation machineId=%d,i=%d", m.MachineId, i))
		}
	}

	m.appCountCollection.debugValidation()
}

func (m *Machine) DebugPrint() {
	fmt.Printf("Machine.DebugPrint %d %v cost=%f linearCost=%f\n",
		m.MachineId, m.Config, m.GetCpuCost(), m.GetLinearCpuCost(m.Config.Cpu))
	for i := 0; i < m.appCountCollection.ListCount; i++ {
		fmt.Printf("    %v,", m.appCountCollection.List[i])
		m.R.AppResourcesConfigMap[m.appCountCollection.List[i].AppId].DebugPrint()
	}

	m.Resource.DebugPrint()

	for _, job := range m.JobList[:m.JobListCount] {
		job.DebugPrint()
	}
}

func (m *Machine) CanFirstFitJob(job *Job, startTimeMin int, startTimeMax int, cpuRatio float64) (ok bool, startMinutes int) {
	//fmt.Printf("checkMachineAddJob %d,%d,%d,%d,%d,%d,%d\n",
	//	m.MachineId, job.JobInstanceId, job.Config.ExecMinutes, startTimeMin, startTimeMax)

	cpuMax := m.Config.Cpu*cpuRatio + ConstraintE
	memMax := m.Config.Mem + ConstraintE
	for i := startTimeMin; i <= startTimeMax; i++ {
		failed := false
		for j := i; j < i+job.Config.ExecMinutes; j++ {
			if m.Cpu[j]+job.Cpu > cpuMax || m.Mem[j]+job.Mem > memMax {
				failed = true
				i = j
				break
			}
		}
		if !failed {
			return true, i
		}
	}

	return false, 0
}

func (m *Machine) GetJobsByTime(t int) (jobs []*Job) {
	for _, job := range m.JobList[:m.JobListCount] {
		if job.StartMinutes <= t && job.StartMinutes+job.Config.ExecMinutes > t {
			jobs = append(jobs, job)
		}
	}

	return jobs
}

func (m *Machine) GetMaxCpuTimeWithJobs() (maxCpu float64, maxCpuTime int, jobs []*Job) {
	for i, v := range m.Cpu {
		if v > maxCpu {
			tempJobs := m.GetJobsByTime(i)
			if len(tempJobs) > 0 {
				maxCpu = v
				maxCpuTime = i
				jobs = tempJobs
			}
		}
	}

	return maxCpu, maxCpuTime, jobs
}

func (m *Machine) BestFitJob(job *Job, startTimeMin int, startTimeMax int) (ok bool, minStartMinutes int, minScoreAdd float64) {
	timeCount := startTimeMax + job.Config.ExecMinutes - startTimeMin
	currentScores := make([]float64, timeCount)
	newScores := make([]float64, timeCount)

	//计算当前得分表
	for i := startTimeMin; i < startTimeMax+job.Config.ExecMinutes; i++ {
		currentScores[i-startTimeMin] = m.CalcCpuCostOne(m.Cpu[i])
	}

	cpuMax := m.Config.Cpu + ConstraintE
	memMax := m.Config.Mem + ConstraintE
	minScoreAdd = math.MaxFloat64
	minStartMinutes = -1
	for i := startTimeMin; i <= startTimeMax; i++ {
		failed := false
		for j := i; j < i+job.Config.ExecMinutes; j++ {
			if m.Cpu[j]+job.Cpu > cpuMax || m.Mem[j]+job.Mem > memMax {
				failed = true
				i = j
				break
			}
		}
		if !failed {
			newScoreAdd := float64(0)
			for j := i; j < i+job.Config.ExecMinutes; j++ {
				score := newScores[j-startTimeMin]
				if score == 0 {
					newScores[j-startTimeMin] = m.CalcCpuCostOne(m.Cpu[j] + job.Cpu)
				}
				newScoreAdd += newScores[j-startTimeMin] - currentScores[j-startTimeMin]
			}
			if newScoreAdd < minScoreAdd {
				minScoreAdd = newScoreAdd
				minStartMinutes = i
				ok = true
			}
		}
	}

	return ok, minStartMinutes, minScoreAdd
}
