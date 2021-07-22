package tag

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/zhyeah/gorm-cache/constant"
)

// NotifyTag notify tag mapped struct
type NotifyTag struct {
	Func string
	Type string
	Keys []string
	Args []int
}

// ResolveNotifyTag resolve notify tag to NotifyTag struct
func ResolveNotifyTag(notifyTag string) (*NotifyTag, error) {
	tagMap := ConvertTagArrayToMap(notifyTag)
	if len(tagMap) < 3 {
		return nil, errors.New("the count of tag parameter is not enough")
	}
	keys := make([]string, 0)
	kval := tagMap[constant.NotifyTagKeys]
	kval = strings.ReplaceAll(kval, "'", "\"")
	err := json.Unmarshal([]byte(kval), &keys)
	if err != nil {
		return nil, err
	}

	args := make([]int, 0)
	aval := tagMap[constant.NotifyTagArgs]
	err = json.Unmarshal([]byte(aval), &args)
	if err != nil {
		return nil, err
	}

	return &NotifyTag{
		Func: tagMap[constant.NotifyTagFunc],
		Type: tagMap[constant.NotifyTagType],
		Keys: keys,
		Args: args,
	}, nil
}
