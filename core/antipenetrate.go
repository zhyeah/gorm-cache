package core

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/bluele/gcache"
	"github.com/zhyeah/gorm-cache/log"
	"github.com/zhyeah/gorm-cache/util"
)

// SortEntry sorted entry
type SortEntry struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// WrappedValue wrapped value
type WrappedValue struct {
	WaitGroup *sync.WaitGroup
	Value     *[]interface{}
}

var antiPanetrateMap sync.Map
var gc gcache.Cache = gcache.New(8192).LRU().Build()

// AntiPenetrate proxy
func AntiPenetrate(proxyedFunc interface{}, inputValuesPtr, retValuesPtr *[]interface{}, timeoutMillis int64) error {
	return AntiPenetrateWithCache(proxyedFunc, inputValuesPtr, retValuesPtr, timeoutMillis, 0)
}

// AntiPenetrateWithCache proxy with cache
func AntiPenetrateWithCache(proxyedFunc interface{}, inputValuesPtr, retValuesPtr *[]interface{}, timeoutMillis int64, cacheMillis int64) error {
	// calculate map key based on `proxyedFunc` and `inputValues`
	key, err := MakePenetrateKey(proxyedFunc, inputValuesPtr)
	if err != nil {
		return err
	}
	log.Logger.Debugf("Penetrate key: %s", key)

	// check if cache has static cache
	retValue, err := gc.Get(key)
	if err == nil {
		*retValuesPtr = *(retValue.(*[]interface{}))
		return nil
	}

	// otherwise, do anti-penetrate
	wrappedValue := &WrappedValue{
		WaitGroup: &sync.WaitGroup{},
		Value:     &[]interface{}{},
	}
	wrappedValue.WaitGroup.Add(1)

	wgInter, ok := antiPanetrateMap.LoadOrStore(key, wrappedValue)
	if ok {
		// if map has value, the goroutine should wait untile it's done or timeout
		wg := wgInter.(*WrappedValue).WaitGroup
		wch := make(chan bool)
		go func() {
			defer close(wch)
			wg.Wait()
		}()

		select {
		case <-wch:
			log.Logger.Debug("Get result from main goroutine")
		case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
			log.Logger.Debug("Time out for waitting main goroutine")
		}

		*retValuesPtr = *wgInter.(*WrappedValue).Value
	} else {
		// if map doesn't have value, this goroutine should penetrate this method to find value
		log.Logger.Debug("Penetrate into method")
		defer wrappedValue.WaitGroup.Done()
		_, funcValue := util.GetRealTypeAndValue(proxyedFunc)

		inValue := make([]reflect.Value, 0)
		for i := range *inputValuesPtr {
			inValue = append(inValue, reflect.ValueOf((*inputValuesPtr)[i]))
		}
		retValues := funcValue.Call(inValue)
		for _, retValue := range retValues {
			*wrappedValue.Value = append(*wrappedValue.Value, retValue.Interface())
		}
		// method invoke done, clear map
		antiPanetrateMap.Delete(key)

		if cacheMillis > 0 {
			gc.SetWithExpire(key, wgInter.(*WrappedValue).Value, time.Duration(cacheMillis+100)*time.Millisecond)
		}

		*retValuesPtr = *wgInter.(*WrappedValue).Value
	}

	return nil
}

// MakePenetrateKey construct the key
func MakePenetrateKey(proxyedFunc interface{}, inputValues *[]interface{}) (string, error) {
	refFunc, _ := util.GetRealTypeAndValue(proxyedFunc)
	funcName := runtime.FuncForPC(reflect.ValueOf(proxyedFunc).Pointer()).Name()
	if len(*inputValues) != refFunc.NumIn() {
		return "", fmt.Errorf("unconsistent count of input values, method: %d, inputValues: %d", refFunc.NumIn(), len(*inputValues))
	}

	retStr := fmt.Sprintf("%s", funcName)
	for i := 0; i < refFunc.NumIn(); i++ {
		objType, objValue := util.GetRealTypeAndValue((*inputValues)[i])
		v := ""
		if objType.Kind() == reflect.Struct {
			v = util.GenMd5(fmt.Sprintf("%v", objValue.Interface()))
		} else if objType.Kind() == reflect.Array || objType.Kind() == reflect.Slice {
			newArray := make([]interface{}, 0)
			for i := 0; i < objValue.Len(); i++ {
				valValue := objValue.Index(i)
				newArray = append(newArray, valValue.Interface())
			}
			sort.Slice(newArray, func(i, j int) bool {
				return fmt.Sprintf("%v", newArray[i]) < fmt.Sprintf("%v", newArray[j])
			})
			v = util.GenMd5(fmt.Sprintf("%v", newArray))
		} else if objType.Kind() == reflect.Map {
			// 将map中的key分开放入到map array中, 排序后构造md5
			newMapArray := make([]SortEntry, 0)
			for _, keyValue := range objValue.MapKeys() {
				valValue := objValue.MapIndex(keyValue)
				tempMap := SortEntry{
					Key:   fmt.Sprintf("%v", keyValue.Interface()),
					Value: valValue.Interface(),
				}
				newMapArray = append(newMapArray, tempMap)
			}
			sort.Slice(newMapArray, func(i, j int) bool {
				return newMapArray[i].Key < newMapArray[j].Key
			})
			v = util.GenMd5(fmt.Sprintf("%v", newMapArray))
		} else {
			v = fmt.Sprintf("%v", objValue.Interface())
		}

		retStr += "_" + v
	}
	return retStr, nil
}
