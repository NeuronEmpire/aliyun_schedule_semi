package main

import "fmt"

type AppCount struct {
	AppId int
	Count int
}

type AppCountCollection struct {
	List      [MaxAppPerMachine]AppCount
	ListCount int
}

func NewAppCountCollection() *AppCountCollection {
	c := &AppCountCollection{}

	return c
}

func (c *AppCountCollection) debugValidation() {
	for i := 0; i < c.ListCount; i++ {
		if c.List[i].AppId == 0 {
			panic(fmt.Errorf("AppCountCollection.debugValidation %d", i))
		}
	}
}

func (c *AppCountCollection) debugPrint() {
	for _, v := range c.List[:c.ListCount] {
		fmt.Printf("    %d %d\n", v.AppId, v.Count)
	}
}

func (c *AppCountCollection) Add(appId int) {
	for i := 0; i < c.ListCount; i++ {
		if c.List[i].AppId == appId {
			c.List[i].Count++
			return
		}
	}

	item := &c.List[c.ListCount]
	item.AppId = appId
	item.Count = 1
	c.ListCount++

	//if DebugEnabled {
	//c.debugValidation()
	//}
}

func (c *AppCountCollection) Remove(appId int) {
	for i := 0; i < c.ListCount; i++ {
		item := &c.List[i]
		if item.AppId == appId {
			if item.Count <= 0 {
				panic(fmt.Errorf("AppCountCollection.Remove appId %d count<=0", appId))
			}

			item.Count--
			if item.Count == 0 {
				if i != c.ListCount-1 {
					last := &c.List[c.ListCount-1]
					item.AppId = last.AppId
					item.Count = last.Count
					last.AppId = 0
					last.Count = 0
				}

				c.ListCount--
			}

			//if DebugEnabled {
			//c.debugValidation()
			//}

			return
		}
	}

	//if DebugEnabled {
	//c.debugValidation()
	//}

	panic(fmt.Errorf("AppCountCollection.Remove appId %d not exists", appId))
}

func (c *AppCountCollection) GetAppCount(appId int) int {
	for _, v := range c.List[:c.ListCount] {
		if v.AppId == appId {
			return v.Count
		}
	}

	return 0
}

func (c *AppCountCollection) Debug() {
	fmt.Println("AppCountCollection", c.List[:c.ListCount])
}
