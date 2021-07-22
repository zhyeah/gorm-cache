package core

import "encoding/json"

// Serializer serialize interface
type Serializer interface {
	Serialize(obj interface{}) ([]byte, error)
	Deserialize(bts []byte, obj interface{}) error
}

// JSONSerializer our default serializer now
type JSONSerializer struct {
}

// Serialize serialize obj
func (s *JSONSerializer) Serialize(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Deserialize deserialize
func (s *JSONSerializer) Deserialize(bts []byte, obj interface{}) error {
	return json.Unmarshal(bts, obj)
}
