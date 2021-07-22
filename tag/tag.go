package tag

import "strings"

// ConvertTagArrayToMap convert tag parameter to map
func ConvertTagArrayToMap(tagStr string) map[string]string {
	tags := strings.Split(tagStr, ";")
	retMap := make(map[string]string)
	for _, tag := range tags {
		params := strings.Split(tag, "=")
		if len(params) < 2 {
			continue
		}
		retMap[params[0]] = params[1]
	}

	return retMap
}
