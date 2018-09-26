package main

func recursiveInitInitJobTimeRangeMin(config *JobConfig) {
	if config.TimeRangeMinInitialized {
		return
	}

	if config.Parents == nil || len(config.Parents) == 0 {
		config.StartTimeMin = 0
	} else {
		for _, p := range config.Parents {
			recursiveInitInitJobTimeRangeMin(p)
		}
		for _, p := range config.Parents {
			if config.StartTimeMin < p.EndTimeMin {
				config.StartTimeMin = p.EndTimeMin
			}
		}
	}

	config.EndTimeMin = config.StartTimeMin + config.ExecMinutes
	config.TimeRangeMinInitialized = true
}

func recursiveInitInitJobTimeRangeMax(config *JobConfig) {
	if config.TimeRangeMaxInitialized {
		return
	}

	if config.Children == nil || len(config.Children) == 0 {
		config.EndTimeMax = TimeSampleCount * 15
	} else {
		for _, c := range config.Children {
			recursiveInitInitJobTimeRangeMax(c)
		}
		for _, c := range config.Children {
			if config.EndTimeMax == 0 || config.EndTimeMax > c.StartTimeMax {
				config.EndTimeMax = c.StartTimeMax
			}
		}
	}

	config.StartTimeMax = config.EndTimeMax - config.ExecMinutes
	config.TimeRangeMaxInitialized = true
}

func (r *ResourceManagement) initJobConfigs() {
	for _, config := range r.JobConfigMap {
		if config == nil {
			continue
		}

		recursiveInitInitJobTimeRangeMin(config)
		recursiveInitInitJobTimeRangeMax(config)
	}
}
