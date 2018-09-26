package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type JobMergeRoundData struct {
	Round    int                 `json:"round"`
	Commands []*JobDeployCommand `json:"commands"`
}

func (r *ResourceManagement) getInstanceSaveFilepath() string {
	return r.OutputDir + fmt.Sprintf("/save_%d_%d.json", r.GetDatasetMachineCount(), r.GetDatasetInstanceLoop())
}

func (r *ResourceManagement) loadInstanceMoveCommands() (moveCommands []*InstanceMoveCommand, err error) {
	data, err := ioutil.ReadFile(r.getInstanceSaveFilepath())
	if err != nil {
		r.log("loadInstanceMoveCommands ReadFile failed,%s\n", err.Error())
		return nil, err
	}

	err = json.Unmarshal(data, &moveCommands)
	if err != nil {
		r.log("loadInstanceMoveCommands json.Unmarshal failed,%s\n", err.Error())
		return nil, err
	}

	//初始状态部署
	for _, config := range r.InstanceDeployConfigList {
		instance := r.InstanceMap[config.InstanceId]
		m := r.MachineMap[config.MachineId]
		m.AddInstance(instance)
		r.DeployMap[instance.InstanceId] = m
	}

	//迁移
	for _, move := range moveCommands {
		//fmt.Println(move.Round, move.InstanceId, move.MachineId)
		instance := r.InstanceMap[move.InstanceId]
		r.DeployMap[instance.InstanceId].RemoveInstance(instance.InstanceId)
		m := r.MachineMap[move.MachineId]
		if !m.ConstraintCheck(instance, 1) {
			return nil,
				fmt.Errorf("loadInstanceMoveCommands ConstraintCheck failed machineId=%d,instanceId=%d",
					m.MachineId, instance.InstanceId)
		}
		m.AddInstance(instance)
		r.DeployMap[instance.InstanceId] = m
	}

	r.log("loadInstanceMoveCommands ok,totalScore=%f,file=%s\n", MachinesGetScore(r.MachineList), r.getInstanceSaveFilepath())

	return moveCommands, nil
}

func (r *ResourceManagement) saveInstanceMoveCommands(moveCommands []*InstanceMoveCommand) {
	data, err := json.Marshal(moveCommands)
	if err != nil {
		r.log("saveInstanceMoveCommands failed,%s\n", err.Error())
		return
	}

	err = ioutil.WriteFile(r.getInstanceSaveFilepath(), data, os.ModePerm)
	if err != nil {
		r.log("saveInstanceMoveCommands failed,%s\n", err.Error())
		return
	}

	r.log("saveInstanceMoveCommands ok,commandCount=%d,file=%s\n", len(moveCommands), r.getInstanceSaveFilepath())
}

func (r *ResourceManagement) buildJobDeployCommands(machines []*Machine) (commands []*JobDeployCommand) {
	commands = make([]*JobDeployCommand, 0)
	for _, m := range machines {
		if m.JobListCount == 0 {
			continue
		}

		for _, job := range m.JobList[:m.JobListCount] {
			commands = append(commands, &JobDeployCommand{
				JobInstanceId: job.JobInstanceId,
				JobId:         job.Config.RealJobId,
				MachineId:     m.MachineId,
				Count:         job.InstanceCount,
				StartMinutes:  job.StartMinutes,
			})
		}
	}

	return commands
}

func (r *ResourceManagement) getJobDeploySaveFilepath() string {
	return r.OutputDir + fmt.Sprintf("/save_%d_%d_job_%f_%d_%d_%d.json",
		r.GetDatasetMachineCount(), r.GetDatasetInstanceLoop(), JobScheduleCpuLimitStep, JobPackCpu, JobPackMem, JobPackLimit)
}

func (r *ResourceManagement) loadJobDeployCommands(
	machines []*Machine, scheduleState []*JobScheduleState) (
	commands []*JobDeployCommand, err error) {
	data, err := ioutil.ReadFile(r.getJobDeploySaveFilepath())
	if err != nil {
		r.log("loadJobDeployCommands ReadFile failed,%s\n", err.Error())
		return nil, err
	}

	err = json.Unmarshal(data, &commands)
	if err != nil {
		r.log("loadJobDeployCommands json.Unmarshal failed,%s\n", err.Error())
		return nil, err
	}

	machineMap := make(map[int]*Machine)
	for _, m := range machines {
		machineMap[m.MachineId] = m
	}

	for _, cmd := range commands {
		job := r.JobMap[cmd.JobInstanceId]
		job.StartMinutes = cmd.StartMinutes
		scheduleState[job.Config.JobId].UpdateTime()
		machineMap[cmd.MachineId].AddJob(job)
	}

	r.log("loadJobDeployCommands ok,totalScore=%f,file=%s\n", MachinesGetScore(machines), r.getJobDeploySaveFilepath())

	return commands, nil
}

func (r *ResourceManagement) saveJobDeployCommands(commands []*JobDeployCommand) {
	data, err := json.Marshal(commands)
	if err != nil {
		r.log("saveJobDeployCommands failed,%s\n", err.Error())
		return
	}

	err = ioutil.WriteFile(r.getJobDeploySaveFilepath(), data, os.ModePerm)
	if err != nil {
		r.log("saveJobDeployCommands failed,%s\n", err.Error())
		return
	}

	r.log("saveJobDeployCommands ok,file=%s\n", r.getJobDeploySaveFilepath())
}

func (r *ResourceManagement) getJobMergeSaveFilepath() string {
	return r.getJobDeploySaveFilepath() + "_merge.json"
}

func (r *ResourceManagement) loadJobMergeRound(machines []*Machine, scheduleState []*JobScheduleState) {
	data, err := ioutil.ReadFile(r.getJobMergeSaveFilepath())
	if err != nil {
		r.log("loadJobMergeCommands ReadFile failed,%s\n", err.Error())
		return
	}

	roundData := JobMergeRoundData{}
	err = json.Unmarshal(data, &roundData)
	if err != nil {
		r.log("loadJobMergeCommands json.Unmarshal failed,%s\n", err.Error())
		return
	}

	deployMap := make(map[int]*JobDeployCommand)
	for _, cmd := range roundData.Commands {
		deployMap[cmd.JobInstanceId] = cmd
	}
	machineMap := make(map[int]*Machine)
	for _, m := range machines {
		machineMap[m.MachineId] = m
	}

	r.JobMergeRound = roundData.Round
	for _, m := range machines {
		for _, job := range JobsCopy(m.JobList[:m.JobListCount]) {
			cmd := deployMap[job.JobInstanceId]
			if cmd.MachineId != m.MachineId || cmd.StartMinutes != job.StartMinutes {
				m.RemoveJob(job.JobInstanceId)
				job.StartMinutes = cmd.StartMinutes
				scheduleState[job.Config.JobId].UpdateTime()
				machineMap[cmd.MachineId].AddJob(job)
			}
		}
	}

	r.log("loadJobMergeRound ok,round=%d\n", r.JobMergeRound)

	return
}

func (r *ResourceManagement) saveJobMergeRound(machines []*Machine) {
	commands := r.buildJobDeployCommands(machines)
	roundData := &JobMergeRoundData{Round: r.JobMergeRound, Commands: commands}
	data, err := json.Marshal(roundData)
	if err != nil {
		r.log("saveJobMergeRound failed,%s\n", err.Error())
		return
	}

	err = ioutil.WriteFile(r.getJobMergeSaveFilepath(), data, os.ModePerm)
	if err != nil {
		r.log("saveJobMergeRound failed,%s\n", err.Error())
		return
	}

	r.log("saveJobMergeRound ok,file=%s\n", r.getJobMergeSaveFilepath())
}
