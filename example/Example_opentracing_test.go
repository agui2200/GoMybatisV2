package example

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"log"
	"net/http"
	"net/url"
	"sourcegraph.com/sourcegraph/appdash"
	appdashtracer "sourcegraph.com/sourcegraph/appdash/opentracing"
	"sourcegraph.com/sourcegraph/appdash/traceapp"
	"testing"
	"time"
)

var tapp *traceapp.App
var ctx context.Context
var rootSpan opentracing.Span

func initOpentracing() {
	var err error
	// Create a recent in-memory store, evicting data after 20s.
	//
	// The store defines where information about traces (i.e. spans and
	// annotations) will be stored during the lifetime of the application. This
	// application uses a MemoryStore store wrapped by a RecentStore with an
	// eviction time of 20s (i.e. all data after 20s is deleted from memory).
	memStore := appdash.NewMemoryStore()
	store := &appdash.RecentStore{
		MinEvictAge: 20 * time.Second,
		DeleteStore: memStore,
	}

	// Start the Appdash web UI on port 8700.
	//
	// This is the actual Appdash web UI -- usable as a Go package itself, We
	// embed it directly into our application such that visiting the web server
	// on HTTP port 8700 will bring us to the web UI, displaying information
	// about this specific web-server (another alternative would be to connect
	// to a centralized Appdash collection server).
	url, err := url.Parse("http://localhost:8700")
	if err != nil {
		log.Fatal(err)
	}
	tapp, err = traceapp.New(nil, url)
	if err != nil {
		log.Fatal(err)
	}
	tapp.Store = store
	tapp.Queryer = memStore
	// We will use a local collector (as we are running the Appdash web UI
	// embedded within our app).
	//
	// A collector is responsible for collecting the information about traces
	// (i.e. spans and annotations) and placing them into a store. In this app
	// we use a local collector (we could also use a remote collector, sending
	// the information to a remote Appdash collection server).
	collector := appdash.NewLocalCollector(store)

	// Here we use the local collector to create a new opentracing.Tracer
	tracer := appdashtracer.NewTracer(collector)
	opentracing.InitGlobalTracer(tracer) // 一定要加

	rootCtx := context.Background()
	s, sctx := opentracing.StartSpanFromContextWithTracer(rootCtx, tracer, "testQuery")
	if MysqlUri == "" || MysqlUri == "*" {
		panic("no database url define in MysqlConfig.go , you must set the mysql link!")

	}
	fmt.Println(s, sctx)
	ctx = sctx
	rootSpan = s
}

func Test_queryTracing(t *testing.T) {
	initOpentracing()
	//使用mapper
	result, err := exampleActivityMapper.SelectTemplete(ctx, "hello")
	rootSpan.Finish()
	fmt.Println("result=", result, "error=", err)
	log.Println("Appdash web UI running on HTTP :8700")
	log.Fatal(http.ListenAndServe(":8700", tapp))
}

func Test_updateTracing(t *testing.T) {
	initOpentracing()
	//使用mapper
	result, err := exampleActivityMapper.UpdateById(ctx, nil, Activity{Id: "1", Name: "testName"})
	rootSpan.Finish()
	fmt.Println("result=", result, "error=", err)
	log.Println("Appdash web UI running on HTTP :8700")
	log.Fatal(http.ListenAndServe(":8700", tapp))
}

// 本地事务的例子
func Test_txTracing(t *testing.T) {
	initOpentracing()
	var session, err = exampleActivityMapper.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = session.Begin() //开启事务
	if err != nil {
		t.Fatal(err)
	}
	var activityBean = Activity{
		Id:   "170",
		Name: "rs168-8",
	}
	var updateNum, e = exampleActivityMapper.UpdateById(nil, &session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	fmt.Println("updateNum=", updateNum)
	if e != nil {
		panic(e)
	}
	activityBean.Id = "171"
	activityBean.Name = "test-123"
	updateNum, e = exampleActivityMapper.UpdateById(nil, &session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	if e != nil {
		panic(e)
	}
	session.Commit() //提交事务
	session.Close()  //关闭事务
	rootSpan.Finish()
	log.Fatal(http.ListenAndServe(":8700", tapp))
}

// 嵌套事务
func Test_insideTx(t *testing.T) {
	initOpentracing()
	var session, err = exampleActivityMapper.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = session.Begin() //开启事务
	if err != nil {
		t.Fatal(err)
	}
	var activityBean = Activity{
		Id:         "170",
		Name:       "rs168-8",
		DeleteFlag: 1,
	}
	var updateNum, e = exampleActivityMapper.UpdateById(nil, &session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	fmt.Println("updateNum=", updateNum)
	if e != nil {
		panic(e)
	}
	err = session.Begin()
	if err != nil {
		t.Fatal(err)
	}
	activityBean.Id = "170"
	activityBean.Name = "test-123456"
	updateNum, e = exampleActivityMapper.UpdateById(nil, &session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	if e != nil {
		panic(e)
	}
	activityBean.Id = "170"
	activityBean.Name = "test-123456789"
	updateNum, e = exampleActivityMapper.UpdateById(nil, &session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	if e != nil {
		panic(e)
	}
	updateNum, e = exampleActivityMapper.UpdateTemplete(&session, activityBean) //sessionId 有值则使用已经创建的session，否则新建一个session
	if e != nil {
		panic(e)
	}
	session.Rollback()
	session.Commit()
	session.Close() //关闭事务
	rootSpan.Finish()
	log.Fatal(http.ListenAndServe(":8700", tapp))
}
