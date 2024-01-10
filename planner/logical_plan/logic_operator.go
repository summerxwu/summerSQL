package logical_plan

import "bytes"

// Closure is the prototype of the routine applied to the logical operator
type Closure func(operator ILogicOperator) (goon bool, err error)

func VisitOperatorTrees(closure Closure, root ...ILogicOperator) error {
	for _, logicOperator := range root {
		if logicOperator == nil {
			continue
		}
		goon, err := closure(logicOperator)
		if err != nil {
			return err
		}
		if goon {
			err = logicOperator.VisitSubOperator(closure)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ILogicOperator (e.g. The relation algebra operator) represent a
// relation which consist of tuples with the relation schema
type ILogicOperator interface {
	Build() error
	VisitSubOperator(closure Closure) error
	ToString() string
}

func PrintPretty(plan ILogicOperator) string {
	buffer := bytes.Buffer{}
	err := VisitOperatorTrees(func(operator ILogicOperator) (bool, error) {
		buffer.WriteString(operator.ToString())
		return true, nil
	}, plan)
	if err != nil {

	}
	return buffer.String()
}
