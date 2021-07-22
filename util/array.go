package util

// FindInArray check if ele in array
func FindInArray(array []int, ele int) int {
	for i := range array {
		if ele == array[i] {
			return i
		}
	}
	return -1
}
