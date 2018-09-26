package main

type AppInterferenceConfig struct {
	AppId1       int
	AppId2       int
	Interference int
}

type AppResourcesConfig struct {
	AppId int
	Resource

	InferenceAppCount int
}

type InstanceDeployConfig struct {
	InstanceId int
	AppId      int
	MachineId  int
}

type MachineResourcesConfig struct {
	MachineId int
	MachineConfig
}

type JobConfig struct {
	JobId         int
	RealJobId     string
	Cpu           float64
	Mem           float64
	InstanceCount int
	ExecMinutes   int
	PreJobs       []int

	Parents                 []*JobConfig
	Children                []*JobConfig
	StartTimeMin            int
	StartTimeMax            int
	EndTimeMin              int
	EndTimeMax              int
	TimeRangeMinInitialized bool
	TimeRangeMaxInitialized bool
}

func (c *JobConfig) isParentOf(p *JobConfig) bool {
	for _, v := range c.Children {
		if v == p {
			return true
		}
	}

	return false
}

func (c *JobConfig) isChildOf(p *JobConfig) bool {
	for _, v := range c.Parents {
		if v == p {
			return true
		}
	}

	return false
}
