package GoMybatis

import (
	"database/sql"
	"github.com/agui2200/GoMybatis/logger"
	"github.com/agui2200/GoMybatis/sessions"
	"github.com/agui2200/GoMybatis/sqlbuilder"
	"github.com/agui2200/GoMybatis/templete"
	"github.com/agui2200/GoMybatis/templete/ast"
	"github.com/agui2200/GoMybatis/templete/engines"
	"github.com/agui2200/GoMybatis/utils"
	"reflect"
	"sync"
)

type GoMybatisEngine struct {
	mutex  sync.RWMutex //读写锁
	isInit bool         //是否初始化

	objMap sync.Map

	dataSourceRouter    sessions.DataSourceRouter     //动态数据源路由器
	log                 logger.Log                    //日志实现类
	logEnable           bool                          //是否允许日志输出（默认开启）
	logSystem           *logger.LogSystem             //日志发送系统
	sessionFactory      *sessions.SessionFactory      //session 工厂
	expressionEngine    ast.ExpressionEngine          //表达式解析引擎
	sqlBuilder          sessions.SqlBuilder           //sql 构建
	sqlResultDecoder    sessions.SqlResultDecoder     //sql查询结果解析引擎
	templeteDecoder     sessions.TempleteDecoder      //模板解析引擎
	goroutineSessionMap *sessions.GoroutineSessionMap //map[协程id]Session
	goroutineIDEnable   bool                          //是否启用goroutineIDEnable（注意（该方法需要在多协程环境下调用）启用会从栈获取协程id，有一定性能消耗，换取最大的事务定义便捷,单线程处理场景可以关闭此配置）
}

func (it GoMybatisEngine) New() GoMybatisEngine {
	it.logEnable = true
	it.isInit = true
	if it.logEnable == true && it.log == nil {
		it.log = &logger.LogStandard{}
	}
	if it.logEnable {
		var logSystem, err = logger.LogSystem{}.New(it.log, it.log.QueueLen())
		if err != nil {
			panic(err)
		}
		it.logSystem = &logSystem
	}
	if it.dataSourceRouter == nil {
		var newRouter = GoMybatisDataSourceRouter{}.New(nil)
		it.SetDataSourceRouter(&newRouter)
	}
	if it.expressionEngine == nil {
		it.expressionEngine = &engines.ExpressionEngineGoExpress{}
	}
	if it.sqlResultDecoder == nil {
		it.sqlResultDecoder = sqlbuilder.GoMybatisSqlResultDecoder{}
	}
	if it.templeteDecoder == nil {
		it.SetTempleteDecoder(&templete.GoMybatisTempleteDecoder{})
	}

	if it.sqlBuilder == nil {
		var builder = sqlbuilder.New(it.ExpressionEngine(), it.logEnable, it.Log())
		it.sqlBuilder = &builder
	}

	if it.sessionFactory == nil {
		var factory = sessions.SessionFactory{}.New(&it)
		it.sessionFactory = &factory
	}
	if it.goroutineSessionMap == nil {
		var gr = sessions.GoroutineSessionMap{}.New()
		it.goroutineSessionMap = &gr
	}
	it.goroutineIDEnable = true
	return it
}

func (it GoMybatisEngine) initCheck() {
	if it.isInit == false {
		panic(utils.NewError("GoMybatisEngine", "must call GoMybatisEngine{}.New() to init!"))
	}
}

func (it *GoMybatisEngine) WriteMapperPtr(ptr interface{}, xml []byte) {
	it.initCheck()
	WriteMapperPtrByEngine(ptr, xml, it)
}

func (it *GoMybatisEngine) Name() string {
	return "GoMybatisEngine"
}

func (it *GoMybatisEngine) DataSourceRouter() sessions.DataSourceRouter {
	it.initCheck()
	return it.dataSourceRouter
}
func (it *GoMybatisEngine) SetDataSourceRouter(router sessions.DataSourceRouter) {
	it.initCheck()
	it.dataSourceRouter = router
}

func (it *GoMybatisEngine) NewSession(mapperName string) (sessions.Session, error) {
	it.initCheck()
	var session, err = it.DataSourceRouter().Router(mapperName, it)
	return session, err
}

//获取日志实现类，是否启用日志
func (it *GoMybatisEngine) LogEnable() bool {
	it.initCheck()
	return it.logEnable
}

//设置日志实现类，是否启用日志
func (it *GoMybatisEngine) SetLogEnable(enable bool) {
	it.initCheck()
	it.logEnable = enable
	it.sqlBuilder.SetEnableLog(enable)
}

//获取日志实现类
func (it *GoMybatisEngine) Log() logger.Log {
	it.initCheck()
	return it.log
}

//设置日志实现类
func (it *GoMybatisEngine) SetLog(log logger.Log) {
	it.initCheck()
	it.log = log
}

//session工厂
func (it *GoMybatisEngine) SessionFactory() *sessions.SessionFactory {
	it.initCheck()
	return it.sessionFactory
}

//设置session工厂
func (it *GoMybatisEngine) SetSessionFactory(factory *sessions.SessionFactory) {
	it.initCheck()
	it.sessionFactory = factory
}

//表达式执行引擎
func (it *GoMybatisEngine) ExpressionEngine() ast.ExpressionEngine {
	it.initCheck()
	return it.expressionEngine
}

//设置表达式执行引擎
func (it *GoMybatisEngine) SetExpressionEngine(engine ast.ExpressionEngine) {
	it.initCheck()
	it.expressionEngine = engine
	var proxy = it.sqlBuilder.ExpressionEngineProxy()
	proxy.SetExpressionEngine(it.expressionEngine)
}

//sql构建器
func (it *GoMybatisEngine) SqlBuilder() sessions.SqlBuilder {
	it.initCheck()
	return it.sqlBuilder
}

//设置sql构建器
func (it *GoMybatisEngine) SetSqlBuilder(builder sessions.SqlBuilder) {
	it.initCheck()
	it.sqlBuilder = builder
}

//sql查询结果解析器
func (it *GoMybatisEngine) SqlResultDecoder() sessions.SqlResultDecoder {
	it.initCheck()
	return it.sqlResultDecoder
}

//设置sql查询结果解析器
func (it *GoMybatisEngine) SetSqlResultDecoder(decoder sessions.SqlResultDecoder) {
	it.initCheck()
	it.sqlResultDecoder = decoder
}

//打开数据库
//driverName: 驱动名称例如"mysql", dataSourceName: string 数据库url
func (it *GoMybatisEngine) Open(driverName, dataSourceName string) (*sql.DB, error) {
	it.initCheck()
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	it.dataSourceRouter.SetDB(driverName, dataSourceName, db)
	return db, nil
}

//模板解析器
func (it *GoMybatisEngine) TempleteDecoder() sessions.TempleteDecoder {
	return it.templeteDecoder
}

//设置模板解析器
func (it *GoMybatisEngine) SetTempleteDecoder(decoder sessions.TempleteDecoder) {
	it.templeteDecoder = decoder
}

func (it *GoMybatisEngine) GoroutineSessionMap() *sessions.GoroutineSessionMap {
	return it.goroutineSessionMap
}

func (it *GoMybatisEngine) RegisterObj(ptr interface{}, name string) {
	var v = reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		panic("GoMybatis Engine Register obj not a ptr value!")
	}
	it.objMap.Store(name, ptr)
}

func (it *GoMybatisEngine) GetObj(name string) interface{} {
	v, _ := it.objMap.Load(name)
	return v
}

func (it *GoMybatisEngine) SetGoroutineIDEnable(enable bool) {
	it.goroutineIDEnable = enable
}

func (it *GoMybatisEngine) GoroutineIDEnable() bool {
	return it.goroutineIDEnable
}

func (it *GoMybatisEngine) LogSystem() *logger.LogSystem {
	return it.logSystem
}
