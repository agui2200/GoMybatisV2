package GoMybatisV2

import "reflect"

type TagArg struct {
	Name  string
	Index int
}

//代理数据
type ProxyArg struct {
	TagArgs    []TagArg
	TagArgsLen int
	Args       []reflect.Value
	ArgsLen    int
}

func (it ProxyArg) New(tagArgs []TagArg, args []reflect.Value) ProxyArg {
	return ProxyArg{
		TagArgs:    tagArgs,
		Args:       args,
		TagArgsLen: len(tagArgs),
		ArgsLen:    len(args),
	}
}
