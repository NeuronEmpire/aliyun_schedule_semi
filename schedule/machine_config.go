package main

type MachineConfig struct {
	Cpu  float64
	Mem  float64
	Disk int
	P    int
	M    int
	PM   int
}

func (c *MachineConfig) isEqual(v *MachineConfig) bool {
	return v.Cpu == c.Cpu && v.Mem == c.Mem && v.Disk == c.Disk && v.P == c.P && v.M == c.M && v.PM == c.PM
}

func (c *MachineConfig) Less(v *MachineConfig) bool {
	l1 := c
	l2 := v

	if l1.Cpu < l2.Cpu {
		return true
	} else if l1.Cpu == l2.Cpu {
		if l1.Mem < l2.Mem {
			return true
		} else if l1.Mem == l2.Mem {
			if l1.Disk < l2.Disk {
				return true
			} else if l1.Disk == l2.Disk {
				if l1.P < l2.P {
					return true
				} else if l1.P == l2.P {
					if l1.M < l2.M {
						return true
					} else if l1.M == l2.M {
						if l1.PM < l2.PM {
							return true
						} else {
							return false
						}
					} else {
						return false
					}
				} else {
					return false
				}
			} else {
				return false
			}
		} else {
			return false
		}
	} else {
		return false
	}
}

type MachineConfigPool struct {
	ConfigList []*MachineConfig
}

func NewMachineConfigPool() *MachineConfigPool {
	p := &MachineConfigPool{}
	return p
}

func (p *MachineConfigPool) GetConfig(config *MachineConfig) (result *MachineConfig) {
	for _, v := range p.ConfigList {
		if v.isEqual(config) {
			return v
		}
	}

	result = &(*config)
	p.ConfigList = append(p.ConfigList, result)

	return result
}
