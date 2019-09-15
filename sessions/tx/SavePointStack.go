package tx

import "sync"

type SavePointStack struct {
	i    int
	data []string //方法队列
	l    sync.Mutex
}

func (t SavePointStack) New() SavePointStack {
	return SavePointStack{
		data: []string{},
		i:    0,
	}
}

func (t *SavePointStack) Push(k string) {
	t.l.Lock()
	t.data = append(t.data, k)
	t.i++
	t.l.Unlock()
}

func (t *SavePointStack) Pop() *string {
	if t.i == 0 {
		return nil
	}
	t.l.Lock()
	t.i--
	var ret = t.data[t.i]
	t.data = t.data[0:t.i]
	t.l.Unlock()
	return &ret
}

func (t *SavePointStack) Len() int {
	return t.i
}
