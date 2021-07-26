package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/zhyeah/gorm-cache/constant"
	"github.com/zhyeah/gorm-cache/log"
	"github.com/zhyeah/gorm-cache/tag"
	"github.com/zhyeah/gorm-cache/util"
	"gorm.io/gorm"
)

// NotifyInfo Cache key update information
type NotifyInfo struct {
	Type             string   // refer: contant
	Fields           []string // fields that construct the cache key
	Args             []int    // method arg indexs that mapped to fields
	VersionKeyPrefix string   // version key prefix
}

// CacheDaoBase dao cache base class
type CacheDaoBase struct {
	Do           interface{} // database object model
	SQLDao       interface{} // sql dao
	ReadDBSource *gorm.DB    // get from SQLDao for specified 'GetById' and 'GetByIds'

	ExpireTime int // default

	IDFieldName         string
	ObjectCachePrefix   string
	VersionPrefix       string                 // version prefix for cache key prefix
	NotifyInfos         []*NotifyInfo          // when modify happended, upgrade the cache version tagged by this list
	MethodNotifyInfoMap map[string]*NotifyInfo // 'NotifyInfo' recorded by method name

	Serializer Serializer // which serializer use for cache
}

// Initialize 初始化信息
func (base *CacheDaoBase) Initialize(instance interface{}) error {
	if instance == nil {
		return errors.New("please give your cache instance to 'Initialize' method")
	}
	if base.ExpireTime == 0 {
		base.ExpireTime = 24 * 3600
	}
	if base.Serializer == nil {
		base.Serializer = &JSONSerializer{}
	}

	base.NotifyInfos = make([]*NotifyInfo, 0)
	base.MethodNotifyInfoMap = make(map[string]*NotifyInfo)

	// set object cache key prefix and id field name
	doType := util.GetPointToType(reflect.TypeOf(base.Do))
	if base.ObjectCachePrefix == "" {
		base.ObjectCachePrefix = doType.Name()
	} else {
		base.ObjectCachePrefix += "_" + doType.Name()
	}

	id := util.GetSpecifiedFieldValue(base.Do, "Id")
	if id != nil {
		base.IDFieldName = "Id"
	} else {
		id = util.GetSpecifiedFieldValue(base.Do, "ID")
		if id != nil {
			base.IDFieldName = "ID"
		} else {
			return errors.New("do object should contains 'Id' or 'ID' field that mapped to primary key")
		}
	}

	// initialize notify infos
	instanceType := util.GetPointToType(reflect.TypeOf(instance))
	filterMap := make(map[string]int)
	for i := 0; i < instanceType.NumField(); i++ {
		if !strings.HasPrefix(instanceType.Field(i).Name, "meta") {
			continue
		}
		notifyTag := instanceType.Field(i).Tag.Get(constant.TagNotify)
		notify, err := tag.ResolveNotifyTag(notifyTag)
		if err != nil {
			return err
		}

		versionKeyPrefix := "V_" + doType.Name()
		notifyInfo := NotifyInfo{
			Type:             notify.Type,
			Fields:           notify.Keys,
			Args:             notify.Args,
			VersionKeyPrefix: versionKeyPrefix,
		}

		// make notiyInfo array
		filterKey := versionKeyPrefix + "_" + strings.Join(notify.Keys, "_")
		if _, ok := filterMap[filterKey]; !ok {
			filterMap[filterKey] = 1
			base.NotifyInfos = append(base.NotifyInfos, &notifyInfo)
		}

		// make method notify map
		base.MethodNotifyInfoMap[notify.Func] = &notifyInfo
	}

	// get sql dao read gorm
	rets := util.ReflectInvokeMethod(base.SQLDao, "GetReadDbSource")
	if len(rets) == 0 {
		return errors.New("your sql dao should have method 'GetReadDbSource', which means you need extend 'BaseDao'")
	}
	base.ReadDBSource = rets[0].(*gorm.DB)

	return nil
}

// GetById try get from cache first, if absent, load it from sql
func (base *CacheDaoBase) GetById(id uint64) (interface{}, error) {
	if id <= 0 {
		return nil, errors.New("illegal id, should >= 0")
	}

	// firstly, get object cache key
	objCacheKey, err := base.GetObjectKey(id)
	if err != nil || objCacheKey == "" {
		log.Logger.Warnf("missed object key for id %d, err: %v", id, err)
		return base.SetObjectCacheForGetById(id)
	}

	// get object cache
	objCacheItem, err := MemcacheClient.Get(objCacheKey)
	if err != nil {
		log.Logger.Warnf("2. missed object cache for id %d, err: %v", id, err)
		return base.SetObjectCacheForGetById(id)
	}

	objInstancePtr := base.makeObjInstancePtr()
	err = base.Serializer.Deserialize(objCacheItem.Value, objInstancePtr)
	if err != nil {
		// some serialize error, throw it out!
		return nil, err
	}
	log.Logger.Debugf("hit cache for id %d", id)
	return objInstancePtr, nil
}

// GetByIds try to get from cache first, if absent, load them from sql
func (base *CacheDaoBase) GetByIds(ids []uint64) (interface{}, error) {
	if len(ids) <= 0 {
		return base.makeObjListPtr(), nil
	}

	absentIds := make([]uint64, 0)

	// get obj list cache versions
	startTime := time.Now().UnixNano() / 1e6
	objCacheKeys, err := base.GetObjectKeys(ids)
	log.Logger.Debugf("get ids while get by keys cost time: %d", time.Now().UnixNano()/1e6-startTime)
	if err != nil {
		// return from sql with cache set
		log.Logger.Warnf("missed object cache keys for ids %v, err: %v", ids, err)
		return base.SetObjectCachesForGetByIds(ids)
	}

	keys := make([]string, 0)
	retIds := make([]uint64, 0)
	for i := range ids {
		if v, ok := objCacheKeys[ids[i]]; !ok {
			absentIds = append(absentIds, ids[i])
		} else {
			keys = append(keys, v)
			retIds = append(retIds, ids[i])
		}
	}

	// getMulti from cache
	startTime = time.Now().UnixNano() / 1e6
	objCacheItems, err := MemcacheClient.GetMulti(keys)
	log.Logger.Debugf("get ids while gets cost time: %d", time.Now().UnixNano()/1e6-startTime)
	if err != nil {
		log.Logger.Warnf("missed object caches for ids %d, err: %v", ids, err)
		return base.SetObjectCachesForGetByIds(ids)
	}

	retList := base.makeObjListPtr()
	listVal := reflect.ValueOf(retList).Elem()
	cacheIdMap := make(map[uint64]int)
	for k, v := range objCacheItems {
		cacheIdMap[base.ResolveIdFromObjectCacheKey(k)] = 1
		objInstancePtr := base.makeObjInstancePtr()
		err = base.Serializer.Deserialize(v.Value, objInstancePtr)
		if err != nil {
			continue
		}
		listVal.Set(reflect.Append(listVal, reflect.ValueOf(objInstancePtr).Elem()))
	}

	for i := range retIds {
		if _, ok := cacheIdMap[retIds[i]]; !ok {
			absentIds = append(absentIds, retIds[i])
		}
	}

	log.Logger.Debugf("absent ids: %v", absentIds)

	if len(absentIds) > 0 {
		// try get from sql for absent ids
		absentList, err := base.SetObjectCachesForGetByIds(absentIds)
		if err != nil {
			log.Logger.Warnf("missed object caches for absentIds %d, err: %v", absentIds, err)
			return base.SetObjectCachesForGetByIds(ids)
		}

		// append absent list to retList
		absentListValue := reflect.ValueOf(absentList).Elem()
		log.Logger.Debugf("absent list vals: %v", absentListValue)
		for i := 0; i < absentListValue.Len(); i++ {
			listVal.Set(reflect.Append(listVal, absentListValue.Index(i)))
		}
	}

	return base.reorderByIds(ids, retList), nil
}

// GetByConcreteKey get single object by concrete key
func (base *CacheDaoBase) GetByConcreteKey(args ...interface{}) (interface{}, error) {
	sqlMethodName := util.GetLastExecuteFuncName()

	// try to get from cache first.
	cacheKey, err := base.GetKey(sqlMethodName, args...)
	if err != nil || cacheKey == "" {
		// get obj return value from sql dao
		log.Logger.Errorf("GetByConcreteKey missed for args: %v, err: %v", args, err)
		retVals := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, args...)
		obj := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		err := base.SetCache(obj, sqlMethodName, args...)
		if err != nil {
			log.Logger.Errorf("GetByConcreteKey set cache failed for args: %v, err: %v", args, err)
		}
		return obj, nil
	}

	// try to get from cache
	cacheItem, err := MemcacheClient.Get(cacheKey)
	if err != nil {
		log.Logger.Warnf("GetByConcreteKey missed for args %d, err: %v", args, err)
		retVals := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, args...)
		obj := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		err := base.SetCache(obj, sqlMethodName, args...)
		if err != nil {
			log.Logger.Errorf("GetByConcreteKey set cache failed for args: %v, err: %v", args, err)
		}
		return obj, nil
	}

	log.Logger.Debugf("hit concrete key cache.")
	idVal := util.ConvertStringToUNumber(string(cacheItem.Value))
	return base.GetById(idVal)
}

// GetByConcreteKeys get objecgts by concrete keys
func (base *CacheDaoBase) GetByConcreteKeys(args ...interface{}) (interface{}, error) {
	sqlMethodName := util.GetLastExecuteFuncName()

	// find out the list args
	listArgIndexs := make([]int, 0)
	listArgIndexMap := make(map[int]int)
	for i := range args {
		if util.RealTypeCheck(args[i], reflect.Slice) {
			listArgIndexs = append(listArgIndexs, i)
			listArgIndexMap[i] = 1
		}
	}
	log.Logger.Debugf("list args indexs: %v", listArgIndexs)
	if len(listArgIndexs) == 0 {
		log.Logger.Error("There is no list arg in args")
		return nil, errors.New("There is no list arg in args")
	}
	// check if the list sizes are equal
	var lastLength = -1
	for i := range listArgIndexs {
		currentLength := util.GetListLength(args[listArgIndexs[i]])
		if lastLength != -1 && lastLength != currentLength {
			log.Logger.Error("The length of list parameter is not equal")
			return nil, errors.New("the length of list parameter is not equal")
		}
		lastLength = currentLength
	}
	log.Logger.Debugf("list param length: %d", lastLength)
	// split params into arrays
	paramArrays := make([][]interface{}, lastLength)
	for i := 0; i < lastLength; i++ {
		currentParams := make([]interface{}, len(args))
		for j := 0; j < len(args); j++ {
			if _, ok := listArgIndexMap[j]; ok {
				currentParams[j] = util.GetListElement(args[j], i)
			} else {
				currentParams[j] = args[j]
			}
		}
		paramArrays[i] = currentParams
	}
	log.Logger.Debugf("paramArrays: %v", paramArrays)

	// make version keys
	versionsMap, err := base.GetVersions(sqlMethodName, paramArrays)
	if err != nil {
		log.Logger.Errorf("GetByConcreteKeys get versions failed, args: %v err: %v", args, err)
		retVals := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, args...)
		objs := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		go func() {
			err := base.SetCaches(objs, sqlMethodName, paramArrays)
			if err != nil {
				log.Logger.Errorf("GetByConcreteKeys set caches failed for args: %v, err: %v", args, err)
			}
		}()
		objsType := reflect.TypeOf(objs)
		objsValue := reflect.ValueOf(objs)
		if objsType.Kind() == reflect.Slice {
			retList := base.makeObjListPtr()
			listVal := reflect.ValueOf(retList).Elem()
			for i := 0; i < objsValue.Len(); i++ {
				listVal.Set(reflect.Append(listVal, objsValue.Index(i)))
			}
			return retList, nil
		}
		return objs, nil
	}
	log.Logger.Debugf("versionsMap: %v", versionsMap)
	cacheKey := make([]string, 0)
	for i := range paramArrays {
		akey := base.JoinArgs(sqlMethodName, paramArrays[i]...)
		if versionStr, ok := versionsMap[akey]; ok {
			// key prefix
			keyPrefix := base.MakeKeyPrefix(sqlMethodName, paramArrays[i]...)
			cacheKey = append(cacheKey, base.MakeKey(keyPrefix, versionStr))
		}
	}

	// get caches
	startTime := time.Now().UnixNano() / 1e6
	cacheItems, err := MemcacheClient.GetMulti(cacheKey)
	log.Logger.Debugf("get multi cost time: %d", time.Now().UnixNano()/1e6-startTime)
	if err != nil {
		log.Logger.Errorf("GetByConcreteKeys get caches failed, args: %v err: %v", args, err)
		retVals := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, args...)
		objs := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		go func() {
			err := base.SetCaches(objs, sqlMethodName, paramArrays)
			if err != nil {
				log.Logger.Errorf("GetByConcreteKeys set caches failed for args: %v, err: %v", args, err)
			}
		}()
		objsType := reflect.TypeOf(objs)
		objsValue := reflect.ValueOf(objs)
		if objsType.Kind() == reflect.Slice {
			retList := base.makeObjListPtr()
			listVal := reflect.ValueOf(retList).Elem()
			for i := 0; i < objsValue.Len(); i++ {
				listVal.Set(reflect.Append(listVal, objsValue.Index(i)))
			}
			return retList, nil
		}
		return objs, nil
	}

	idArr := make([]uint64, 0)
	for _, v := range cacheItems {
		idArr = append(idArr, util.ConvertStringToUNumber(string(v.Value)))
	}

	// get by ids
	objs, err := base.GetByIds(idArr)
	if err != nil {
		log.Logger.Errorf("GetByConcreteKeys get caches failed, args: %v err: %v", args, err)
		retVals := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, args...)
		objs := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		go func() {
			err := base.SetCaches(objs, sqlMethodName, paramArrays)
			if err != nil {
				log.Logger.Errorf("GetByConcreteKeys set caches failed for args: %v, err: %v", args, err)
			}
		}()
		objsType := reflect.TypeOf(objs)
		objsValue := reflect.ValueOf(objs)
		if objsType.Kind() == reflect.Slice {
			retList := base.makeObjListPtr()
			listVal := reflect.ValueOf(retList).Elem()
			for i := 0; i < objsValue.Len(); i++ {
				listVal.Set(reflect.Append(listVal, objsValue.Index(i)))
			}
			return retList, nil
		}
		return objs, nil
	}

	retList := base.makeObjListPtr()
	listVal := reflect.ValueOf(retList).Elem()
	objListValue := reflect.ValueOf(objs).Elem()
	for i := 0; i < objListValue.Len(); i++ {
		listVal.Set(reflect.Append(listVal, objListValue.Index(i)))
	}

	if listVal.Len() >= lastLength {
		return retList, nil
	}

	// get absent objs
	absentParams := make([]interface{}, len(args))
	for i := range args {
		if _, ok := listArgIndexMap[i]; !ok {
			absentParams[i] = args[i]
		} else {
			argsiType := reflect.TypeOf(args[i])
			if argsiType.Kind() == reflect.Ptr {
				argsiType = argsiType.Elem()
			}
			listPtrValue := util.NewListPtrValueByType(argsiType.Elem())
			if reflect.TypeOf(args[i]).Kind() == reflect.Slice {
				absentParams[i] = listPtrValue.Elem().Interface()
			} else {
				absentParams[i] = listPtrValue.Interface()
			}
		}
	}
	absent := false
	notifyInfo := base.MethodNotifyInfoMap[sqlMethodName]
	arrMap := base.getParamMap(paramArrays, notifyInfo)
	objMap := make(map[string]interface{})
	for i := 0; i < listVal.Len(); i++ {
		obj := listVal.Index(i).Interface()
		objMapKey := base.getObjMapKey(obj, notifyInfo)
		objMap[objMapKey] = obj
	}
	for k := range arrMap {
		if _, ok := objMap[k]; !ok {
			absent = true
			for _, li := range listArgIndexs {
				value := reflect.ValueOf(absentParams[li])
				if value.Type().Kind() == reflect.Slice {
					value = reflect.Append(value, reflect.ValueOf(arrMap[k][li]))
					absentParams[li] = value.Interface()
				} else {
					valueList := value.Elem()
					valueList.Set(reflect.Append(valueList, reflect.ValueOf(arrMap[k][li])))
				}
			}
		}
	}
	log.Logger.Debugf("absent params: %v, absent: %v", absentParams, absent)

	if absent {
		absentRet := util.ReflectInvokeMethod(base.SQLDao, sqlMethodName, absentParams...)
		if err != nil {
			log.Logger.Errorf("get absent objs from sql failed, absent args: %v", absentParams)
		}
		objs := absentRet[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
		go func() {
			err := base.SetCaches(objs, sqlMethodName, paramArrays) // here we pass paramArrays is ok, cause the implemention use map to find corresponding objs
			if err != nil {
				log.Logger.Errorf("GetByConcreteKeys set absent caches failed for args: %v, err: %v", absentParams, err)
			}
		}()
		// err := base.SetCaches(objs, sqlMethodName, paramArrays) // here we pass paramArrays is ok, cause the implemention use map to find corresponding objs
		// if err != nil {
		// 	log.Logger.Errorf("GetByConcreteKeys set absent caches failed for args: %v, err: %v", absentParams, err)
		// }
		absentListType := reflect.TypeOf(objs)
		absentListValue := reflect.ValueOf(objs)
		if absentListType.Kind() == reflect.Ptr {
			absentListValue = absentListValue.Elem()
		}
		for i := 0; i < absentListValue.Len(); i++ {
			listVal.Set(reflect.Append(listVal, absentListValue.Index(i)))
		}
	}

	return retList, nil
}

// GetByList list cache
func (base *CacheDaoBase) GetByList(args ...interface{}) (interface{}, error) {
	sqlMethodName := util.GetLastExecuteFuncName()

	// try to get from cache first.
	cacheKey, err := base.GetKey(sqlMethodName, args...)
	if err != nil || cacheKey == "" {
		log.Logger.Warnf("1. GetByRange get cache key failed for args: %v, err: %v", args, err)
		objList, err := base.SetListCache(sqlMethodName, args...)
		if err != nil {
			log.Logger.Errorf("GetByRange set cache failed for args: %v, err: %v", args, err)
		}
		return objList, nil
	}

	// try to get from cache
	cacheItem, err := MemcacheClient.Get(cacheKey)
	if err != nil {
		log.Logger.Warnf("2. GetByRange get cache failed for args: %v, err: %v", args, err)
		objList, err := base.SetListCache(sqlMethodName, args...)
		if err != nil {
			log.Logger.Errorf("GetByRange set cache failed for args: %v, err: %v", args, err)
		}
		return objList, nil
	}

	log.Logger.Debugf("GetByRange hit key %s", cacheKey)

	ids := make([]uint64, 0)
	err = json.Unmarshal(cacheItem.Value, &ids)
	if err != nil {
		return nil, err
	}
	return base.GetByIds(ids)
}

// NotifyModified when do action like add/edit/delete, invoke this to update cache
func (base *CacheDaoBase) NotifyModified(curDo interface{}) error {
	if curDo == nil {
		return nil
	}

	// delete object cache
	id := base.GetIdValue(curDo)
	objectKey, err := base.GetObjectKey(id)
	if err != nil {
		log.Logger.Errorf("Update single key field, id: %d err: %v", id, err)
	}
	log.Logger.Debugf("object key: %s", objectKey)
	MemcacheClient.Delete(objectKey)

	// update version cache
	for _, info := range base.NotifyInfos {
		fieldStrValues := util.GetFieldsStringValues(curDo, info.Fields)
		vKey := base.MakeVersionKey(info.VersionKeyPrefix, fieldStrValues)
		log.Logger.Debugf("ready to clear key: %s", vKey)
		err := base.UpdateVersion(vKey)
		if err != nil {
			log.Logger.Error(err)
		}
	}
	return nil
}

// UpdateVersion update version
func (base *CacheDaoBase) UpdateVersion(versionKey string) error {
	now := time.Now().UnixNano() / 1e6
	value := util.ConvertNumberToString(now)
	return MemcacheClient.Set(&memcache.Item{Key: versionKey, Value: []byte(value), Expiration: int32(base.ExpireTime)})
}

// GetObjectKey 获取对象缓存key
func (base *CacheDaoBase) GetObjectKey(id uint64) (string, error) {
	version, err := base.GetObjectVersion(id)
	if err != nil {
		return "", err
	}
	if version == "" {
		// version key missed
		return "", nil
	}
	return base.MakeObjectKey(id, version), nil
}

// GetObjectKeys get object cache keys, if verision is absent, the result map will be absent too.
func (base *CacheDaoBase) GetObjectKeys(ids []uint64) (map[uint64]string, error) {
	versions, err := base.GetObjectVersions(ids)
	if err != nil {
		return nil, err
	}
	for k, v := range versions {
		versions[k] = base.MakeObjectKey(k, v)
	}
	return versions, nil
}

// GetObjectVersion get object version from cache
func (base *CacheDaoBase) GetObjectVersion(id uint64) (string, error) {
	versionKey := base.MakeObjectVersionKey(id)
	val, err := MemcacheClient.Get(versionKey)
	if err == memcache.ErrCacheMiss {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(val.Value), nil
}

// GetObjectVersions get object versions
func (base *CacheDaoBase) GetObjectVersions(ids []uint64) (map[uint64]string, error) {
	versionKeys := make([]string, 0)
	for i := range ids {
		versionKeys = append(versionKeys, base.MakeObjectVersionKey(ids[i]))
	}
	val, err := MemcacheClient.GetMulti(versionKeys)
	if err != nil {
		return nil, err
	}
	ret := make(map[uint64]string)
	for k, v := range val {
		id := base.ResolveIdFromObjectVersionKey(k)
		ret[id] = string(v.Value)
	}
	return ret, nil
}

// MakeObjectKey make object key string
func (base *CacheDaoBase) MakeObjectKey(id uint64, version string) string {
	return fmt.Sprintf("%s_%d_%s", base.ObjectCachePrefix, id, version)
}

// MakeObjectVersionKey make object version key string
func (base *CacheDaoBase) MakeObjectVersionKey(id uint64) string {
	return fmt.Sprintf("V_%s_%d", base.ObjectCachePrefix, id)
}

// ResolveIdFromObjectVersionKey resolve id from verison key
func (base *CacheDaoBase) ResolveIdFromObjectVersionKey(versionKey string) uint64 {
	ps := strings.Split(versionKey, "_")
	return util.ConvertStringToUNumber(ps[len(ps)-1])
}

// ResolveIdFromObjectCacheKey resolve id from object cache key
func (base *CacheDaoBase) ResolveIdFromObjectCacheKey(cacheKey string) uint64 {
	ps := strings.Split(cacheKey, "_")
	return util.ConvertStringToUNumber(ps[len(ps)-2])
}

// SetBojectCacheForGetById helpful for the scene when we get obj from id and then update cache.
func (base *CacheDaoBase) SetObjectCacheForGetById(id uint64) (interface{}, error) {
	obj, err := base.sqlGetById(id)
	if err != nil {
		return nil, err
	}
	if obj != nil {
		err = base.SetObjectCache(obj)
		if err != nil {
			log.Logger.Error("set cache failed for id %d, obj: %v", id, obj)
		}
	}
	return obj, nil
}

// SetObjectCachesForGetByIds helpful for the scene when we get objs from ids and then update cache.
func (base *CacheDaoBase) SetObjectCachesForGetByIds(ids []uint64) (interface{}, error) {
	objList, err := base.sqlGetByIds(ids)
	if err != nil {
		return nil, err
	}
	go base.SetOjectCaches(objList)
	return objList, nil
}

// SetObjectCache set object cache for obj
func (base *CacheDaoBase) SetObjectCache(obj interface{}) error {
	id := base.GetIdValue(obj)

	// set cache first, that promise before obj stored successfully,
	// old cache can be readed from cache, it decrease the query amount
	// through DB.
	now := time.Now().UnixNano() / 1e6
	objCacheKey := base.MakeObjectKey(id, util.ConvertNumberToString(now))

	objData, err := base.Serializer.Serialize(obj)
	if err != nil {
		return err
	}

	err = MemcacheClient.Set(&memcache.Item{Key: objCacheKey, Value: objData, Expiration: int32(base.ExpireTime)})
	if err != nil {
		return err
	}

	// update version cache then, it's safe if version key set failed.
	return base.SetObjectVersion(id, now)
}

// SetOjectCaches set object caches for obj list
func (base *CacheDaoBase) SetOjectCaches(objList interface{}) {
	listValue := reflect.ValueOf(objList).Elem()
	if listValue.Len() > 0 {
		for i := 0; i < listValue.Len(); i++ {
			obj := listValue.Index(i).Interface()
			err := base.SetObjectCache(obj)
			if err != nil {
				log.Logger.Error("set cache failed for obj: %v when set object caches", obj)
			}
		}
	}
}

// SetObjectVersion set version cache
func (base *CacheDaoBase) SetObjectVersion(id uint64, ts int64) error {
	objVersionKey := base.MakeObjectVersionKey(id)
	return MemcacheClient.Set(&memcache.Item{Key: objVersionKey, Value: []byte(util.ConvertNumberToString(ts)), Expiration: int32(base.ExpireTime)})
}

// GetKey get cache key
func (base *CacheDaoBase) GetKey(methodName string, args ...interface{}) (string, error) {
	// Version first
	version, err := base.GetVersion(methodName, args...)
	if err != nil {
		return "", err
	}
	if version == "" {
		// version key missed
		return "", nil
	}

	// key prefix
	keyPrefix := base.MakeKeyPrefix(methodName, args...)
	return base.MakeKey(keyPrefix, version), nil
}

// GetVersion get current version
func (base *CacheDaoBase) GetVersion(methodName string, args ...interface{}) (string, error) {
	// get method info
	versionKey, err := base.MakeMethodVersionKey(methodName, args...)
	if err != nil {
		return "", err
	}

	item, err := MemcacheClient.Get(versionKey)
	if err == memcache.ErrCacheMiss {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(item.Value), nil
}

// GetVersions get the version of multi args
func (base *CacheDaoBase) GetVersions(methodName string, args [][]interface{}) (map[string]string, error) {
	ret := make(map[string]string)
	// make version keys
	versionMap := make(map[string]string)
	versionKeys := make([]string, 0)
	for i := range args {
		akey := base.JoinArgs(methodName, args[i]...)
		versionKey, err := base.MakeMethodVersionKey(methodName, args[i]...)
		if err != nil {
			log.Logger.Errorf("make versioin key failed, err: %v", err)
			continue
		}
		versionKeys = append(versionKeys, versionKey)
		versionMap[versionKey] = akey
	}

	items, err := MemcacheClient.GetMulti(versionKeys)
	if err != nil {
		return ret, err
	}

	for k, v := range items {
		ret[versionMap[k]] = string(v.Value)
	}
	return ret, nil
}

// MakeMethodVersionKey make method version key
func (base *CacheDaoBase) MakeMethodVersionKey(methodName string, args ...interface{}) (string, error) {
	// get method info
	info, ok := base.MethodNotifyInfoMap[methodName]
	if !ok {
		log.Logger.Warnf("no such method '%s' mapped info", methodName)
		return "", fmt.Errorf("no such method '%s' mapped info", methodName)
	}

	// try to get version from cache
	keyArgs := make([]string, 0)
	for i := range info.Args {
		argStr := util.GeneralToString(args[info.Args[i]])
		keyArgs = append(keyArgs, argStr)
	}
	versionKey := base.MakeVersionKey(info.VersionKeyPrefix, keyArgs)
	return versionKey, nil
}

// SetVersion set version cache
func (base *CacheDaoBase) SetVersion(methodName string, ts int64, args ...interface{}) error {
	// get method info
	versionKey, err := base.MakeMethodVersionKey(methodName, args...)
	if err != nil {
		return err
	}
	return MemcacheClient.Set(&memcache.Item{Key: versionKey, Value: []byte(util.ConvertNumberToString(ts)), Expiration: int32(base.ExpireTime)})
}

// AddVersion set version cache
func (base *CacheDaoBase) AddVersion(methodName string, ts int64, args ...interface{}) error {
	// get method info
	versionKey, err := base.MakeMethodVersionKey(methodName, args...)
	if err != nil {
		return err
	}
	err = MemcacheClient.Add(&memcache.Item{Key: versionKey, Value: []byte(util.ConvertNumberToString(ts)), Expiration: int32(base.ExpireTime)})
	if err == memcache.ErrNotStored {
		return nil
	}
	return err
}

// SetCache set cache for key query
func (base *CacheDaoBase) SetCache(obj interface{}, methodName string, args ...interface{}) error {
	idVal := base.GetIdValue(obj)
	now := time.Now().UnixNano() / 1e6

	// set object cache
	err := base.SetObjectCache(obj)

	// set cache
	oldVersion, err := base.GetVersion(methodName, args...)
	if err != nil {
		return err
	}
	if oldVersion != "" {
		now = util.ConvertStringToNumber(oldVersion)
	}
	keyPrefix := base.MakeKeyPrefix(methodName, args...)
	cacheKey := base.MakeKey(keyPrefix, util.ConvertNumberToString(now))

	err = MemcacheClient.Set(&memcache.Item{Key: cacheKey, Value: []byte(util.ConvertUNumberToString(idVal)), Expiration: int32(base.ExpireTime)})
	if err != nil {
		return err
	}

	// set version cache, if existed already, ignore
	err = base.AddVersion(methodName, now, args...)
	return err
}

// SetCaches set caches for keys query
func (base *CacheDaoBase) SetCaches(objs interface{}, methodName string, paramArray [][]interface{}) error {

	// set each key cache
	objsValue := reflect.ValueOf(objs)
	objsType := reflect.TypeOf(objs)
	if objsType.Kind() == reflect.Ptr {
		objsType = objsType.Elem()
		objsValue = objsValue.Elem()
	}

	notifyInfo := base.MethodNotifyInfoMap[methodName]
	arrMap := base.getParamMap(paramArray, notifyInfo)

	for i := 0; i < objsValue.Len(); i++ {
		obj := objsValue.Index(i).Interface()
		objMapKey := base.getObjMapKey(obj, notifyInfo)
		if param, ok := arrMap[objMapKey]; ok {
			log.Logger.Debugf("cache match for %v", param)
			base.SetCache(obj, methodName, param...)
		}
	}

	return nil
}

// SetListCache set list cache
func (base *CacheDaoBase) SetListCache(methodName string, args ...interface{}) (interface{}, error) {
	err := base.dbArgCheck(args...)
	if err != nil {
		return nil, err
	}

	// replace the first arg (we assume it's gorm.DB) with Select.('ID')
	copyArgs := make([]interface{}, len(args))
	for i := range args {
		copyArgs[i] = args[i]
	}
	copyArgs[0] = base.ReadDBSource.Select(base.IDFieldName)
	retVals := util.ReflectInvokeMethod(base.SQLDao, methodName, copyArgs...)
	objs := retVals[0] // TODO: 这里目前默认是第一个返回值作为db obj, 后续评估是否需要扫描结果数组
	ids, err := base.GetIdsValue(objs)
	if err != nil {
		return nil, err
	}

	retList, err := base.GetByIds(ids)
	if err != nil {
		return nil, err
	}

	// then we fall to get by ids
	now := time.Now().UnixNano() / 1e6
	oldVersion, err := base.GetVersion(methodName, args...)
	if err != nil {
		return nil, err
	}
	if oldVersion != "" {
		now = util.ConvertStringToNumber(oldVersion)
	}
	keyPrefix := base.MakeKeyPrefix(methodName, args...)
	cacheKey := base.MakeKey(keyPrefix, util.ConvertNumberToString(now))

	idsJSON, err := json.Marshal(&ids)
	if err != nil {
		return retList, err
	}
	err = MemcacheClient.Set(&memcache.Item{Key: cacheKey, Value: idsJSON, Expiration: int32(base.ExpireTime)})
	if err != nil {
		return retList, err
	}

	// set version cache, if existed already, ignore
	err = base.AddVersion(methodName, now, args...)
	return retList, err
}

// MakeKey make key
func (base *CacheDaoBase) MakeKey(keyPrefix string, version string) string {
	return fmt.Sprintf("%s_%s", keyPrefix, version)
}

// MakeVersionKey make version key string (V_{methodNmae}_{param list})
func (base *CacheDaoBase) MakeVersionKey(versionKeyPrefix string, fieldStrValues []string) string {
	arr := make([]string, 0)
	arr = append(arr, versionKeyPrefix)
	arr = append(arr, fieldStrValues...)
	return strings.Join(arr, "_")
}

// MakeKeyPrefix make key prefix ({methodName}_{param list})
func (base *CacheDaoBase) MakeKeyPrefix(methodName string, args ...interface{}) string {
	argsStr := make([]string, 0)
	argsStr = append(argsStr, methodName)
	argsName := util.GetMetodParameterList(base.SQLDao, methodName)
	for i := range argsName {
		if argsName[i] == "gorm.io/gorm_DB" {
			continue
		}
		argsStr = append(argsStr, util.GeneralToString(args[i]))
	}
	log.Logger.Debugf("Key prefix is: %s", strings.Join(argsStr, "_"))
	return strings.Join(argsStr, "_")
}

/* ------ below is some reflect method ------- */

// JoinArgs join args to a string
func (base *CacheDaoBase) JoinArgs(methodName string, args ...interface{}) string {
	var ret string = ""
	argsName := util.GetMetodParameterList(base.SQLDao, methodName)
	for i := range argsName {
		if argsName[i] == "gorm.io/gorm_DB" {
			continue
		}
		ret += util.GeneralToString(args[i]) + "_"
	}
	return ret[0 : len(ret)-1]
}

// GetIdValue get the id value of object
func (base *CacheDaoBase) GetIdValue(do interface{}) uint64 {
	val := util.GetSpecifiedFieldValue(do, base.IDFieldName)
	valType := reflect.TypeOf(val)
	var id uint64
	if valType.Kind() == reflect.Int64 {
		id = uint64(val.(int64))
	} else if valType.Kind() == reflect.Uint64 {
		id = val.(uint64)
	}
	return id
}

// GetIdsValue get ids list from do list
func (base *CacheDaoBase) GetIdsValue(doList interface{}) ([]uint64, error) {
	if !util.RealTypeCheck(doList, reflect.Slice) {
		return nil, errors.New("value type is not slice")
	}
	doListType := reflect.TypeOf(doList)
	doListValue := reflect.ValueOf(doList)
	if doListType.Kind() == reflect.Ptr {
		doListType = doListType.Elem()
		doListValue = doListValue.Elem()
	}

	ret := make([]uint64, 0)
	for i := 0; i < doListValue.Len(); i++ {
		val := doListValue.Index(i).Interface()
		ret = append(ret, base.GetIdValue(val))
	}
	return ret, nil
}

// dbArgCheck check if first arg is gorm.DB
func (base *CacheDaoBase) dbArgCheck(args ...interface{}) error {
	if len(args) <= 0 {
		return errors.New("args should contains 'gorm.DB' at least")
	}
	firstArgType := reflect.TypeOf(args[0])
	if firstArgType.Kind() == reflect.Ptr {
		firstArgType = firstArgType.Elem()
	}
	if firstArgType.PkgPath() != "gorm.io/gorm" || firstArgType.Name() != "DB" {
		return errors.New("first arg must be 'gorm.DB' at least")
	}
	return nil
}

// reorderByIds reorder objList by ids order
func (base *CacheDaoBase) reorderByIds(ids []uint64, objList interface{}) interface{} {
	orderedList := base.makeObjListPtr()

	objMap := make(map[uint64]reflect.Value)
	objListValue := reflect.ValueOf(objList).Elem()
	for i := 0; i < objListValue.Len(); i++ {
		obj := objListValue.Index(i).Interface()
		id := base.GetIdValue(obj)
		objMap[id] = objListValue.Index(i)
	}

	orderedListValue := reflect.ValueOf(orderedList).Elem()
	for i := range ids {
		if v, ok := objMap[ids[i]]; ok {
			orderedListValue.Set(reflect.Append(orderedListValue, v))
		}

	}
	return orderedList
}

func (base *CacheDaoBase) checkParamMatch(obj interface{}, args []interface{}, notifyInfo *NotifyInfo) bool {
	match := true
	for i := range notifyInfo.Fields {
		if util.GetSpecifiedFieldValue(obj, notifyInfo.Fields[i]) != args[notifyInfo.Args[i]] {
			match = false
			break
		}
	}
	return match
}

func (base *CacheDaoBase) getObjMapKey(obj interface{}, notifyInfo *NotifyInfo) string {
	key := ""
	for _, field := range notifyInfo.Fields {
		key += (util.GeneralToString(util.GetSpecifiedFieldValue(obj, field)) + "_")
	}
	return key[0 : len(key)-1]
}

func (base *CacheDaoBase) getParamMap(args [][]interface{}, notifyInfo *NotifyInfo) map[string][]interface{} {
	arrMap := make(map[string][]interface{})
	for i := range args {
		key := ""
		for _, j := range notifyInfo.Args {
			key += (util.GeneralToString(args[i][j]) + "_")
		}
		arrMap[key[0:len(key)-1]] = args[i]
	}
	return arrMap
}

/* ------ below is addtional sql method helper ------- */

func (base *CacheDaoBase) makeObjInstancePtr() interface{} {
	doType := reflect.TypeOf(base.Do)
	if doType.Kind() == reflect.Ptr {
		doType = doType.Elem()
	}
	return reflect.New(doType).Interface()
}

func (base *CacheDaoBase) makeObjListPtr() interface{} {
	doType := reflect.TypeOf(base.Do)
	if doType.Kind() == reflect.Ptr {
		doType = doType.Elem()
	}
	return reflect.New(reflect.SliceOf(doType)).Interface()
}

func (base *CacheDaoBase) sqlGetById(id uint64) (interface{}, error) {
	ret := base.makeObjInstancePtr()
	db := base.ReadDBSource.Model(ret)

	err := db.Where("id=?", id).First(ret).Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (base *CacheDaoBase) sqlGetByIds(ids []uint64) (interface{}, error) {
	doType := reflect.TypeOf(base.Do)
	if doType.Kind() == reflect.Ptr {
		doType = doType.Elem()
	}
	model := base.makeObjInstancePtr()
	db := base.ReadDBSource.Model(model)

	ret := base.makeObjListPtr()
	err := db.Where("id in ?", ids).Find(ret).Error
	if err != nil {
		return nil, err
	}

	// arr := make([]interface{}, 0)
	// retValue := reflect.ValueOf(ret).Elem()
	// for i := 0; i < retValue.Len(); i++ {
	// 	arr = append(arr, retValue.Index(i).Interface())
	// }
	return ret, nil
}
