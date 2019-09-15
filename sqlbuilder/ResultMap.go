package sqlbuilder

type ResultProperty struct {
	XMLName  string //`xml:"result/id"`
	Column   string
	Property string
	GoType   string
}
