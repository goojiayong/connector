package connector

import (
	"encoding/json"
)

//The data content of each monitoring point
type MonitorInfo struct {
	Name string
	Time string
	Quality string
	SystemTime string
	Value string
}

type MonitorPoints map[string]MonitorInfo

//Unformatted Json data
func (m *MonitorPoints)Unmarshaldata(j []byte) error {
	err := json.Unmarshal(j, &m)
	if err != nil {
		return err
	}

	return nil
}

