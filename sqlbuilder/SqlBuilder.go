package sqlbuilder

import (
	"github.com/agui2200/GoMybatis/logger"
	"github.com/agui2200/GoMybatis/templete/ast"
)

func New(engine ast.ExpressionEngine, logEnable bool, logger logger.Log) SqlBuilder {
	var expressionEngineProxy = ExpressionEngineProxy{}.New(engine, true)
	var builder = SqlBuilder{}.New(GoMybatisSqlArgTypeConvert{}, expressionEngineProxy, logger, logEnable)
	return builder
}

type SqlBuilder struct {
	sqlArgTypeConvert     ast.SqlArgTypeConvert
	expressionEngineProxy ExpressionEngineProxy
	enableLog             bool
	nodeParser            ast.NodeParser
}

func (it *SqlBuilder) ExpressionEngineProxy() *ExpressionEngineProxy {
	return &it.expressionEngineProxy
}
func (it *SqlBuilder) SqlArgTypeConvert() ast.SqlArgTypeConvert {
	return it.sqlArgTypeConvert
}

func (it SqlBuilder) New(SqlArgTypeConvert ast.SqlArgTypeConvert, expressionEngine ExpressionEngineProxy, log logger.Log, enableLog bool) SqlBuilder {
	it.sqlArgTypeConvert = SqlArgTypeConvert
	it.expressionEngineProxy = expressionEngine
	it.enableLog = enableLog
	it.nodeParser = ast.NodeParser{
		Holder: ast.NodeConfigHolder{
			Convert: SqlArgTypeConvert,
			Proxy:   &expressionEngine,
		},
	}
	return it
}

func (it *SqlBuilder) BuildSql(paramMap map[string]interface{}, nodes []ast.Node) (string, error) {
	//抽象语法树节点构建
	var sql, err = ast.DoChildNodes(nodes, paramMap)
	if err != nil {
		return "", err
	}
	var sqlStr = string(sql)
	return sqlStr, nil
}

func (it *SqlBuilder) SetEnableLog(enable bool) {
	it.enableLog = enable
}
func (it *SqlBuilder) EnableLog() bool {
	return it.enableLog
}

func (it *SqlBuilder) NodeParser() ast.NodeParser {
	return it.nodeParser
}
