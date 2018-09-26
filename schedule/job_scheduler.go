package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type JobScheduler struct {
	R             *ResourceManagement
	Machines      []*Machine
	ScheduleState []*JobScheduleState

	limits [1024]int //纪录各水平线部署的任务数量
}

func NewJobScheduler(r *ResourceManagement, machines []*Machine, scheduleState []*JobScheduleState) (s *JobScheduler) {
	s = &JobScheduler{}
	s.R = r
	s.Machines = machines
	s.ScheduleState = scheduleState

	return s
}

func (s *JobScheduler) bestFit(
	machines []*Machine, job *Job, scheduleState []*JobScheduleState, cpuRatio float64) (
	minStartMinutes int, minMachine *Machine) {

	minStartMinutes = TimeSampleCount * 15
	startTimeMin, startTimeMax, _, _ := job.RecursiveGetTimeRange(scheduleState)
	for _, m := range machines {
		ok, startTime := m.CanFirstFitJob(job, startTimeMin, startTimeMax, cpuRatio)
		if !ok {
			continue
		}
		if startTime < minStartMinutes {
			minStartMinutes = startTime
			minMachine = m
		}
	}

	return minStartMinutes, minMachine
}

func (s *JobScheduler) parallelBestFit(
	machines []*Machine, job *Job, scheduleState []*JobScheduleState) (
	minStartMinutes int, minMachine *Machine) {

	minStartMinutes = TimeSampleCount * 15

	//分割机器，并发BestFit
	const parallelCount = ParallelCpuCount * 4
	var minStartMinutesList [parallelCount]int
	for i := 0; i < parallelCount; i++ {
		minStartMinutesList[i] = TimeSampleCount * 15
	}
	var minMachineList [parallelCount]*Machine
	size := len(machines) / parallelCount
	for i := 0; ; i++ {
		//水平逐步线上移
		cpuRatio := 0.5 + float64(i)*JobScheduleCpuLimitStep
		if cpuRatio > 1 {
			cpuRatio = 1
		}
		wg := &sync.WaitGroup{}
		for pI := 0; pI < parallelCount; pI++ {
			start := pI * size
			count := size
			if pI == parallelCount-1 {
				count = len(machines) - start
			}
			wg.Add(1)
			go func(index int, subMachines []*Machine, cpuRatio float64) {
				defer wg.Done()
				minStartMinutesList[index], minMachineList[index] = s.bestFit(
					subMachines, job, scheduleState, cpuRatio)
			}(pI, machines[start:start+count], cpuRatio)
		}
		wg.Wait()
		for i, v := range minStartMinutesList {
			if v < minStartMinutes {
				minStartMinutes = v
				minMachine = minMachineList[i]
			}
		}

		if cpuRatio >= 1 {
			break
		}

		if minMachine != nil {
			s.limits[i]++
			break
		}
	}

	return minStartMinutes, minMachine
}

func (s *JobScheduler) bestFitJobs(machines []*Machine, jobs []*Job) (result []*Machine, err error) {
	result = machines

	//BFD
	for i, job := range jobs {
		if i > 0 && i%10000 == 0 {
			s.R.log("bestFitJobs %d\n", i)
		}

		minStartMinutes, minMachine := s.parallelBestFit(machines, job, s.ScheduleState)
		if minMachine == nil {
			return nil, fmt.Errorf("bestFitJobs failed")
		}
		job.StartMinutes = minStartMinutes
		s.ScheduleState[job.Config.JobId].UpdateTime()
		minMachine.AddJob(job)
	}

	jobWithInstanceCount := 0
	for _, m := range result {
		if m.InstanceListCount > 0 {
			jobWithInstanceCount += m.JobListCount
		}
	}

	s.R.log("bestFitJobs totalScore=%f,jobWithInstanceCount=%d,machineCount=%d\n",
		MachinesGetScore(result), jobWithInstanceCount, len(result))
	fmt.Println(s.limits)

	return result, nil
}

//任务部署都能部署到实例机器上，基本上不需要自动伸缩部署任务
//这段代码主要是用来实验任务单独部署所需的机器数量
func (s *JobScheduler) RunOld() (err error) {
	s.R.log("JobScheduler.Run\n")
	if len(s.R.JobList) == 0 {
		return nil
	}

	s.sortJobs()

	var lastResult []*Machine
	lastSucceed := false

	scaleUp := true
	scaleDividing := false
	scaleStep := 512
	scaleCurrent := s.R.DeployedMachineCount - scaleStep
	machineCount := 0
	for {
		//如果已经开始二分搜索，区间大小减半
		if scaleDividing {
			scaleStep /= 2
		}

		//正向或反向搜索
		if scaleUp {
			s.R.log("JobScheduler.Run scale up last=%d,scaleStep=%d,now=%d\n",
				scaleCurrent, scaleStep, scaleCurrent+scaleStep)
			scaleCurrent += scaleStep

		} else {
			s.R.log("JobScheduler.Run scale down last=%d,scaleStep=%d,now=%d\n",
				scaleCurrent, scaleStep, scaleCurrent-scaleStep)
			scaleCurrent -= scaleStep
		}

		//已到最大机器数//todo这里需要调整步长，暂不优化
		if scaleCurrent > len(s.Machines) {
			scaleCurrent = len(s.Machines)
			s.R.log("JobScheduler.Run reach max scaleCurrent=%d\n", scaleCurrent)
		}

		result, err := s.bestFitJobs(MachinesCloneWithInstances(s.Machines[:scaleCurrent]), s.R.JobList)
		if err != nil {
			s.R.log("JobScheduler.Run failed scaleCurrent=%d\n", scaleCurrent)
			//已达到最大机器数，并且调度失败
			if scaleCurrent == len(s.Machines) {
				return fmt.Errorf("JobScheduler.Run failed,max machine used")
			}

			//部署失败，减少机器数量
			if lastSucceed {
				//开始分割
				scaleDividing = true
				//反向
				scaleUp = !scaleUp
			}
			lastSucceed = false

			//已分割完
			if scaleStep == 1 {
				s.R.log("JobScheduler.Run scaleStep=1 failed\n")
				machineCount = scaleCurrent + 1
				//todo这里可以优化
				lastResult, err = s.bestFitJobs(MachinesCloneWithInstances(s.Machines[:machineCount]), s.R.JobList)
				if err != nil {
					panic("bestFitJobs last one failed")
				}
				break
			}
		} else {
			s.R.log("JobScheduler.Run succeed scaleCurrent=%d\n", scaleCurrent)
			if !lastSucceed {
				//开始分割
				scaleDividing = true
				//反向
				scaleUp = !scaleUp
			}
			lastSucceed = true

			//保存最后成功结果
			lastResult = result

			//已分割完
			if scaleStep == 1 {
				s.R.log("JobScheduler.Run scaleStep=1 ok\n")
				machineCount = scaleCurrent
				break
			}
		}
	}

	for i, m := range lastResult {
		for _, job := range m.JobList[:m.JobListCount] {
			s.Machines[i].AddJob(job)
		}
	}

	s.R.log("JobScheduler.Run totalScore=%f,machineCount=%d\n", MachinesGetScore(s.Machines), machineCount)

	return nil
}

func (s *JobScheduler) sortJobs() {
	//按照最早结束时间排序
	sort.Slice(s.R.JobList, func(i, j int) bool {
		job1 := s.R.JobList[i]
		job2 := s.R.JobList[j]

		if job1.Config.isParentOf(job2.Config) {
			//保证父节点在前
			return true
		} else if job1.Config.isChildOf(job2.Config) {
			//保证子节点在后
			return false
		} else {
			if math.Abs(float64(job1.Config.EndTimeMin-job2.Config.EndTimeMin)) < 8 {
				//结束时间接近的按面积排序
				return job1.Cpu*float64(job1.Config.ExecMinutes) > job2.Cpu*float64(job2.Config.ExecMinutes)
			} else {
				return job1.Config.EndTimeMin < job2.Config.EndTimeMin
			}
		}
	})
}

func (s *JobScheduler) Run() (err error) {
	s.R.log("JobScheduler.Run,totalScore=%f\n", MachinesGetScore(s.Machines))
	if len(s.R.JobList) == 0 {
		return nil
	}

	s.sortJobs()

	_, err = s.bestFitJobs(s.Machines, s.R.JobList)
	if err != nil {
		return err
	}

	return nil
}
