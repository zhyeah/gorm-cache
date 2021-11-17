package core

import (
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/zhyeah/gorm-cache/util"
)

// CacheDaoMap 缓存dao map
var CacheDaoMap = make(map[string]func() interface{})

// MemcacheConfig memcache缓存配置
type MemcacheConfig struct {
	Servers      []string
	Timeout      int64
	MaxIdleConns int
}

// MemcacheClient global memcache client
var MemcacheClient *memcache.Client

// InitializeCache initialize
func InitializeCache(config *MemcacheConfig) {
	MemcacheClient = memcache.New(config.Servers...)
	MemcacheClient.Timeout = time.Duration(config.Timeout) * time.Millisecond
	MemcacheClient.MaxIdleConns = config.MaxIdleConns

	for _, v := range CacheDaoMap {
		cdao := v()
		util.ReflectInvokeMethod(cdao, "Initialize", cdao)
	}
}
