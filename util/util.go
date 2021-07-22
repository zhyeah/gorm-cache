package util

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

func ConvertNumberToString(n int64) string {
	return strconv.FormatInt(n, 10)
}

func ConvertUNumberToString(n uint64) string {
	return strconv.FormatUint(n, 10)
}

func ConvertFloatToString(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func ConvertStringToUNumber(s string) uint64 {
	val, _ := strconv.ParseUint(s, 10, 64)
	return val
}

func ConvertStringToNumber(s string) int64 {
	val, _ := strconv.ParseInt(s, 10, 64)
	return val
}

// GenMd5 generate md5
func GenMd5(str string) string {
	encoded := md5.Sum([]byte(str))
	return hex.EncodeToString(encoded[:])
}
