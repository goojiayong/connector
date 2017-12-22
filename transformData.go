package connector

//Unformatted monitoring data
func TransMonitorData(sourceData []byte) (*MonitorPoints, error) {
	var monitor MonitorPoints
	err := monitor.Unmarshaldata(sourceData)
	if err != nil {
		return nil, err
	}
	return &monitor, nil
}
