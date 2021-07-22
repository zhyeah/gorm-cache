package util

import (
	"fmt"
	"reflect"
	"strings"
)

// GetPointToType get the type of ptr point to
func GetPointToType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// GetSpecifiedFieldValue get specified field value
func GetSpecifiedFieldValue(obj interface{}, fieldName string) interface{} {
	objValue := reflect.ValueOf(obj)
	objType := reflect.TypeOf(obj)
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
		objValue = objValue.Elem()
	}
	if objValue.FieldByName(fieldName).IsValid() {
		return objValue.FieldByName(fieldName).Interface()
	}
	return nil
}

// GetFieldStringValue get field string value
func GetFieldStringValue(obj interface{}, fieldName string) string {
	val := GetSpecifiedFieldValue(obj, fieldName)
	if val == nil {
		return ""
	}
	return GeneralToString(val)
}

// GetFieldsStringValues get field string value
func GetFieldsStringValues(obj interface{}, fieldNames []string) []string {
	fieldStrValues := make([]string, 0)
	for _, k := range fieldNames {
		strValue := GetFieldStringValue(obj, k)
		fieldStrValues = append(fieldStrValues, strValue)
	}
	return fieldStrValues
}

// GeneralToString 转字符串
func GeneralToString(val interface{}) string {
	valType := reflect.TypeOf(val)
	valVal := reflect.ValueOf(val)
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
		val = valVal.Elem().Interface()
	}
	if valType.Kind() == reflect.Slice || valType.Kind() == reflect.Map || valType.Kind() == reflect.Struct {
		return GenMd5(fmt.Sprintf("%v", val))
	}
	return fmt.Sprintf("%v", val)
}

// GetMetodParameterList get param list by methodName
func GetMetodParameterList(object interface{}, methodName string) []string {
	objectValue := reflect.ValueOf(object)
	method := objectValue.MethodByName(methodName)

	paramNameList := make([]string, 0)
	methodType := method.Type()
	for i := 0; i < methodType.NumIn(); i++ {
		vType := methodType.In(i)
		if vType.Kind() == reflect.Ptr {
			vType = vType.Elem()
		}
		paramNameList = append(paramNameList, strings.Join([]string{vType.PkgPath(), vType.Name()}, "_"))
	}
	return paramNameList
}

// RealTypeCheck is obj type of kind
func RealTypeCheck(obj interface{}, kind reflect.Kind) bool {
	objType := reflect.TypeOf(obj)
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
	}
	return (objType.Kind() == kind)
}

// GetListLength 获取list长度
func GetListLength(obj interface{}) int {
	objType := reflect.TypeOf(obj)
	objValue := reflect.ValueOf(obj)
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
		objValue = objValue.Elem()
	}
	if objType.Kind() != reflect.Slice {
		return -1
	}
	return objValue.Len()
}

// GetListElement 获取列表中的元素
func GetListElement(obj interface{}, index int) interface{} {
	objType := reflect.TypeOf(obj)
	objValue := reflect.ValueOf(obj)
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
		objValue = objValue.Elem()
	}
	return objValue.Index(index).Interface()
}

// ReflectInvokeMethod invoke method by reflect
func ReflectInvokeMethod(object interface{}, methodName string, args ...interface{}) []interface{} {

	inputs := make([]reflect.Value, len(args))
	for i, arg := range args {
		inputs[i] = reflect.ValueOf(arg)
	}

	objectValue := reflect.ValueOf(object)
	method := objectValue.MethodByName(methodName)
	ret := method.Call(inputs)

	retList := []interface{}{}
	for _, retItem := range ret {
		retList = append(retList, retItem.Interface())
	}

	return retList
}

// NewListPtrValueByType new a slice of objType
func NewListPtrValueByType(objType reflect.Type) reflect.Value {
	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
	}
	return reflect.New(reflect.SliceOf(objType))
}
