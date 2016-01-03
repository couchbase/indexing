package queryutil

import qexpr "github.com/couchbase/query/expression"
import qparser "github.com/couchbase/query/expression/parser"

func IsArrayExpression(exp string) (bool, bool, error) {
	cExpr, err := qparser.Parse(exp)
	if err != nil {
		return false, false, err
	}

	expr := cExpr.(qexpr.Expression)
	isArray, isDistinct := expr.IsArrayIndexKey()
	return isArray, isDistinct, nil
}

func GetArrayExpressionPosition(exprs []string) (bool, bool, int, error) {
	isArrayIndex := false
	isArrayDistinct := true // Default is true as we do not yet support duplicate entries
	arrayExprPos := -1
	for i, exp := range exprs {
		isArray, isDistinct, err := IsArrayExpression(exp)
		if err != nil {
			return false, false, -1, err
		}
		if isArray == true {
			isArrayIndex = isArray
			isArrayDistinct = isDistinct
			arrayExprPos = i
		}
	}
	return isArrayIndex, isArrayDistinct, arrayExprPos, nil
}
