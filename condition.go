package pipeflow

import (
	"fmt"
	"reflect"
)

type compiledCondition struct {
	predicate     func(any) (bool, error)
	predicateType reflect.Type
}

func compileCondition(step *Step, condition any) (*compiledCondition, error) {
	t := reflect.TypeOf(condition)
	if t == nil || t.Kind() != reflect.Func || t.NumIn() > 1 || t.NumOut() != 1 || t.Out(0).Kind() != reflect.Bool {
		return nil, fmt.Errorf("pipeflow: step %q condition must be func() bool or func(T) bool", step.name)
	}
	var predicateType reflect.Type
	if t.NumIn() == 1 {
		predicateType = t.In(0)
	}
	v := reflect.ValueOf(condition)
	predicate := func(input any) (result bool, err error) {
		defer recoverPanic(&err)
		args := make([]reflect.Value, 0, 1)
		if predicateType != nil {
			if inputErr := acceptsValue(predicateType, input); inputErr != nil {
				return false, fmt.Errorf("condition input: %w", inputErr)
			}
			args = append(args, reflectValue(input, predicateType))
		}
		result = v.Call(args)[0].Bool()
		return result, err
	}
	return &compiledCondition{predicate: predicate, predicateType: predicateType}, nil
}
