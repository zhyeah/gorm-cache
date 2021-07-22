package util

import (
	"runtime"
	"strings"
)

// GetLastExecuteFuncName get the last execute function name.
func GetLastExecuteFuncName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	fullName := f.Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}
