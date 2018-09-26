package main

type Instance struct {
	ResourceManagement *ResourceManagement
	InstanceId         int
	Config             *AppResourcesConfig

	IsGhost bool
	Fixed   bool
}

func NewInstance(r *ResourceManagement, instanceId int, config *AppResourcesConfig) *Instance {
	i := &Instance{}
	i.ResourceManagement = r
	i.InstanceId = instanceId
	i.Config = config

	return i
}

func (i *Instance) CreateGhost() (ghost *Instance) {
	ghost = &Instance{ResourceManagement: i.ResourceManagement, InstanceId: i.InstanceId, Config: i.Config}
	ghost.IsGhost = true
	return ghost
}
