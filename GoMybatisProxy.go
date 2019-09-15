package GoMybatis

import (
	"context"
	"reflect"
	"strings"
)

type build func(funcField reflect.StructField, field reflect.Value) buildResult
type buildResult func(ctx context.Context, arg ProxyArg) []reflect.Value

// AopProxy 可写入每个函数代理方法.proxyPtr:代理对象指针，buildFunc:构建代理函数
func Proxy(proxyPtr interface{}, buildFunc build) {
	v := reflect.ValueOf(proxyPtr)
	if v.Kind() != reflect.Ptr {
		panic("AopProxy: AopProxy arg must be a pointer")
	}
	buildProxy(v, buildFunc)
}

// AopProxy 可写入每个函数代理方法
func ProxyValue(mapperValue reflect.Value, buildFunc build) {
	buildProxy(mapperValue, buildFunc)
}

func buildProxy(v reflect.Value, buildFunc build) {
	for {
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		} else {
			break
		}
	}
	t := v.Type()
	et := t
	if et.Kind() == reflect.Ptr {
		et = et.Elem()
	}
	ptr := v
	var obj reflect.Value
	if ptr.Kind() == reflect.Ptr {
		obj = ptr.Elem()
	} else {
		obj = ptr
	}
	count := obj.NumField()
	for i := 0; i < count; i++ {
		f := obj.Field(i)
		sf := et.Field(i)
		ft := f.Type()
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if f.CanSet() {
			switch ft.Kind() {
			case reflect.Struct:
				if buildFunc != nil {
					buildProxy(f, buildFunc) //循环扫描
				}
			case reflect.Func:
				if buildFunc != nil {
					buildRemoteMethod(v, f, ft, sf, buildFunc(sf, f))
				}
			}
		}
	}
	if t.Kind() == reflect.Ptr {
		v.Set(ptr)
	} else {
		v.Set(obj)
	}
}

func buildRemoteMethod(source reflect.Value, f reflect.Value, ft reflect.Type, sf reflect.StructField, proxyFunc buildResult) {
	var tagParams []string
	var mapperParams = sf.Tag.Get(`mapperParams`)
	if mapperParams != `` {
		tagParams = strings.Split(mapperParams, `,`)
	}
	var tagParamsLen = len(tagParams)
	var inputCount int
	for i := 0; i < ft.NumIn(); i++ {
		if ft.In(i).String() == GoMybatis_Context {
			continue
		}
		inputCount++
	}
	if tagParamsLen > inputCount {
		panic(`[GoMybatisProxy] method fail! the tag "mapperParams" length can not > arg length ! filed=` + sf.Name)
	}
	var tagArgs = make([]TagArg, 0)
	if tagParamsLen != 0 {
		for index, v := range tagParams {
			var tagArg = TagArg{
				Index: index,
				Name:  v,
			}
			tagArgs = append(tagArgs, tagArg)
		}
	}
	var tagArgsLen = len(tagArgs)
	if tagArgsLen > 0 && inputCount != tagArgsLen {
		panic(`[GoMybatisProxy] method fail! the tag "mapperParams" length  != args length ! filed = ` + sf.Name)
	}
	var fn = func(args []reflect.Value) (results []reflect.Value) {
		ctx := context.TODO()
		// 找出context 进行注入和args扔掉 context
		var newArgs []reflect.Value
		for _, arg := range args {
			if arg.Kind() == reflect.Interface {
				//arg = arg.Elem()
				if v, o := arg.Interface().(context.Context); o {
					ctx = v
				}
				continue
			}
			newArgs = append(newArgs, arg)
		}
		proxyResults := proxyFunc(ctx, ProxyArg{}.New(tagArgs, newArgs))
		for _, returnV := range proxyResults {
			results = append(results, returnV)
		}
		return results
	}
	if f.Kind() == reflect.Ptr {
		fp := reflect.New(ft)
		fp.Elem().Set(reflect.MakeFunc(ft, fn))
		f.Set(fp)
	} else {
		f.Set(reflect.MakeFunc(ft, fn))
	}
	//println("[GoMybatis] write method success:" + source.Type().Name() + " > " + sf.Name + " " + f.Type().String())
	tagParams = nil
}
