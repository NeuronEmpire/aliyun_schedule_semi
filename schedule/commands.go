package main

type InstanceMoveCommand struct {
	Round      int `json:"round"`
	InstanceId int `json:"instance_id"`
	MachineId  int `json:"machine_id"`
}

type JobDeployCommand struct {
	JobInstanceId int    `json:"job_instance_id"`
	JobId         string `json:"job_id"`
	MachineId     int    `json:"machine_id"`
	Count         int    `json:"count"`
	StartMinutes  int    `json:"start_minutes"`
}
