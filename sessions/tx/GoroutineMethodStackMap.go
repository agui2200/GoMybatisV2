package tx

import (
	"sync"
)

type GoroutineMethodStackMap struct {
	m    map[int64]*StructField
	lock sync.RWMutex
}

func (g GoroutineMethodStackMap) New() GoroutineMethodStackMap {
	return GoroutineMethodStackMap{
		m: make(map[int64]*StructField),
	}
}
func (g *GoroutineMethodStackMap) Put(k int64, methodInfo *StructField) {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.m[k] = methodInfo
}
func (g *GoroutineMethodStackMap) Get(k int64) *StructField {
	return g.m[k]
}
