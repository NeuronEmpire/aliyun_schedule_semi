package main

func InstancesCopy(p []*Instance) (r []*Instance) {
	if p == nil {
		return nil
	}

	r = make([]*Instance, len(p))
	for i, v := range p {
		r[i] = v
	}

	return r
}

func InstancesRemove(instances []*Instance, removes []*Instance) (rest []*Instance) {
	rest = make([]*Instance, 0)
	for _, v := range instances {
		has := false
		for _, i := range removes {
			if i.InstanceId == v.InstanceId {
				has = true
				break
			}
		}
		if !has {
			rest = append(rest, v)
		}
	}

	return rest
}

func InstancesContains(instances []*Instance, instanceId int) bool {
	for _, v := range instances {
		if v.InstanceId == instanceId {
			return true
		}
	}

	return false
}

func InstancesContainsApp(instances []*Instance, appId int) bool {
	for _, v := range instances {
		if v.Config.AppId == appId {
			return true
		}
	}

	return false
}
