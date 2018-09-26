package main

type JobScheduleState struct {
	Jobs      []*Job
	StartTime int
	EndTime   int
}

func NewJobScheduleState(r *ResourceManagement, jobs []*Job) (result []*JobScheduleState) {
	result = make([]*JobScheduleState, len(r.JobConfigMap)+1)
	for i := 0; i < len(result); i++ {
		s := &JobScheduleState{}
		s.StartTime = TimeSampleCount * 15
		s.EndTime = 0
		result[i] = s
	}

	for _, job := range jobs {
		job.StartMinutes = -1
		result[job.Config.JobId].Jobs = append(result[job.Config.JobId].Jobs, job)
	}

	return result
}

func (s *JobScheduleState) UpdateTime() {
	for _, job := range s.Jobs {
		if job.StartMinutes == -1 {
			continue
		}

		if job.StartMinutes < s.StartTime {
			s.StartTime = job.StartMinutes
		}

		endTime := job.StartMinutes + job.Config.ExecMinutes
		if endTime > s.EndTime {
			s.EndTime = endTime
		}
	}
}
