package sessions

import "github.com/agui2200/GoMybatisV2/sqlbuilder"

//sql查询结果解码
type SqlResultDecoder interface {
	//resultMap = in xml resultMap element
	//dbData = select the SqlResult
	//decodeResultPtr = need decode result type
	Decode(resultMap map[string]*sqlbuilder.ResultProperty, SqlResult []map[string][]byte, decodeResultPtr interface{}) error
}
