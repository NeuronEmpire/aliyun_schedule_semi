package main

import "fmt"

type Job struct {
	R             *ResourceManagement
	JobInstanceId int
	Config        *JobConfig
	InstanceCount int
	Cpu           float64
	Mem           float64

	StartMinutes int //todo 分离状态
}

func NewJob(r *ResourceManagement, jobInstanceId int, config *JobConfig, instanceCount int) *Job {
	job := &Job{}
	job.R = r
	job.JobInstanceId = jobInstanceId
	job.Config = config
	job.InstanceCount = instanceCount
	job.Cpu = job.Config.Cpu * float64(job.InstanceCount)
	job.Mem = job.Config.Mem * float64(job.InstanceCount)
	job.StartMinutes = -1

	return job
}

func (job *Job) RecursiveGetTimeRange(scheduleState []*JobScheduleState) (
	startTimeMin int, startTimeMax int, endTimeMin int, endTimeMax int) {
	c := job.Config

	startTimeMin = c.StartTimeMin
	startTimeMax = c.StartTimeMax
	endTimeMin = c.EndTimeMin
	endTimeMax = c.EndTimeMax

	//fmt.Println("RecursiveGetTimeRange 1", job.JobInstanceId, startTimeMin, startTimeMax, endTimeMin, endTimeMax)

	if c.Parents != nil {
		for _, v := range c.Parents {
			//fmt.Println("parent", v.JobId, startTimeMin, v.State.EndTime)
			if startTimeMin < scheduleState[v.JobId].EndTime {
				startTimeMin = scheduleState[v.JobId].EndTime
			}
		}
	}

	if c.Children != nil {
		for _, v := range c.Children {
			//fmt.Println("children", v.JobId, endTimeMax, v.State.StartTime)
			if endTimeMax > scheduleState[v.JobId].StartTime {
				endTimeMax = scheduleState[v.JobId].StartTime
			}
		}
	}

	endTimeMin = startTimeMin + c.ExecMinutes
	startTimeMax = endTimeMax - c.ExecMinutes

	//fmt.Println("RecursiveGetTimeRange 2", startTimeMin, startTimeMax, endTimeMin, endTimeMax)

	return startTimeMin, startTimeMax, endTimeMin, endTimeMax
}

func (job *Job) DebugPrint() {
	fmt.Printf("Job cpu=%f,mem=%f,instanceCount=%d,t=%d,"+
		"startTimeMin=%d,startTimeMax=%d\n",
		job.Cpu, job.Mem, job.InstanceCount, job.StartMinutes,
		job.Config.StartTimeMin, job.Config.StartTimeMax)
}
