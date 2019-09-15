package sessions

import (
	"github.com/agui2200/GoMybatisV2/sqlbuilder"
	"github.com/agui2200/GoMybatisV2/templete/ast"
)

//sql文本构建
type SqlBuilder interface {
	BuildSql(paramMap map[string]interface{}, nodes []ast.Node) (string, error)
	ExpressionEngineProxy() *sqlbuilder.ExpressionEngineProxy
	SqlArgTypeConvert() ast.SqlArgTypeConvert
	SetEnableLog(enable bool)
	EnableLog() bool
	NodeParser() ast.NodeParser
}
