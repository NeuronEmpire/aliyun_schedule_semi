package main

import "fmt"

type Replay struct {
	R                    *ResourceManagement
	InstanceMoveCommands []*InstanceMoveCommand
	JobDeployCommands    []*JobDeployCommand
}

func NewReplay(
	r *ResourceManagement,
	instanceMoveCommands []*InstanceMoveCommand,
	jobDeployCommands []*JobDeployCommand) (replay *Replay) {

	replay = &Replay{}
	replay.R = r
	replay.InstanceMoveCommands = instanceMoveCommands
	replay.JobDeployCommands = jobDeployCommands

	return replay
}

func (r *Replay) Run() (err error) {
	//创建机器
	machines := make(map[int]*Machine)
	for _, m := range r.R.MachineList {
		machine := NewMachine(m.R, m.MachineId, m.Config)
		machines[machine.MachineId] = machine
	}

	//初始化
	deploys := make(map[int]*Machine)
	for _, config := range r.R.InstanceDeployConfigList {
		instance := r.R.InstanceMap[config.InstanceId]
		m := machines[config.MachineId]
		m.AddInstance(instance)
		deploys[instance.InstanceId] = m
	}

	totalScore := float64(0)
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalScore += m.GetCpuCost()
		}
	}
	r.R.log("replay 1 score=%f\n", totalScore)

	//部署实例
	for _, move := range r.InstanceMoveCommands {
		//fmt.Println(move.Round, move.InstanceId, move.MachineId)
		instance := r.R.InstanceMap[move.InstanceId]
		deploys[instance.InstanceId].RemoveInstance(instance.InstanceId)
		m := machines[move.MachineId]
		if !m.ConstraintCheck(instance, 1) {
			return fmt.Errorf("replay ConstraintCheck failed machineId=%d,instanceId=%d", m.MachineId, instance.InstanceId)
		}
		m.AddInstance(instance)
		deploys[instance.InstanceId] = m
	}

	//转化为任务部署状态
	for _, m := range machines {
		m.beginOffline()
	}

	totalScore = float64(0)
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalScore += m.GetCpuCostReal()
		}
	}
	r.R.log("replay 2 score=%f\n", totalScore)

	jobConfigMap := make(map[string]*JobConfig)
	for _, c := range r.R.JobConfigMap {
		if c != nil {
			jobConfigMap[c.RealJobId] = c
		}
	}

	//部署任务
	jobDeployed := make(map[int]int)
	for _, v := range r.JobDeployCommands {
		m := machines[v.MachineId]
		config := jobConfigMap[v.JobId]
		jobDeployed[config.JobId] += v.Count
		job := NewJob(r.R, v.JobInstanceId, config, v.Count)
		job.StartMinutes = v.StartMinutes
		m.AddJob(job)
		for i := v.StartMinutes; i < v.StartMinutes+config.ExecMinutes; i++ {
			if m.Cpu[i] > m.Config.Cpu+ConstraintE {
				return fmt.Errorf(fmt.Sprintf("Replay failed job cpu=%f,maxCPu=%f", m.Cpu[i], m.Config.Cpu))
			}
			if m.Mem[i] > m.Config.Mem+ConstraintE {
				return fmt.Errorf(fmt.Sprintf("Replay failed job mem=%f,maxMem=%f", m.Mem[i], m.Config.Mem))
			}
		}
	}
	for _, config := range r.R.JobConfigMap {
		if config == nil {
			continue
		}
		if jobDeployed[config.JobId] != config.InstanceCount {
			return fmt.Errorf(fmt.Sprintf("Replay failed job not all deployd,jobid=%s", config.RealJobId))
		}
	}

	totalScore = float64(0)
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalScore += m.GetCpuCostReal()
		}
	}

	r.R.log("Replay ok,total score=%f\n", totalScore)

	return nil
}
