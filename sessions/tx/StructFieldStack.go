package tx

import (
	"reflect"
	"sync"
)

type StructField struct {
	i    int
	data []reflect.StructField //方法队列
	l    sync.Mutex
}

func (t StructField) New() StructField {
	return StructField{
		data: []reflect.StructField{},
		i:    0,
	}
}

func (t *StructField) Push(k reflect.StructField) {
	t.l.Lock()
	t.data = append(t.data, k)
	t.i++
	t.l.Unlock()
}

func (t *StructField) Pop() (ret reflect.StructField) {
	t.l.Lock()
	t.i--
	ret = t.data[t.i]
	t.data = t.data[0:t.i]
	t.l.Unlock()
	return
}

func (t *StructField) Len() int {
	return t.i
}
