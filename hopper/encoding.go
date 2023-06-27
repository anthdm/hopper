package hopper

import "encoding/json"

type DataEncoder interface {
	Encode(Map) ([]byte, error)
}

type DataDecoder interface {
	Decode([]byte, any) error
}

type JSONEncoder struct{}

func (JSONEncoder) Encode(data Map) ([]byte, error) {
	return json.Marshal(data)
}

type JSONDecoder struct{}

func (JSONDecoder) Decode(b []byte, v any) error {
	return json.Unmarshal(b, &v)
}
