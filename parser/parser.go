package summerSQLparser

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func Parse(query string) ([]ast.StmtNode, error) {
	p := parser.New()
	sn, _, err := p.Parse(query, "", "")
	if err != nil {
		return nil, err
	}
	return sn, nil
}
