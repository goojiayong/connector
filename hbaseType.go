package connector

import (
	"time"
	"strings"
	"fmt"
	"strconv"
	"encoding/json"

	"github.com/goojiayong/hbase"
	log "common-core/aclog"
)

//Hbase configuration data
type HbaseConfig struct {
	Hosts string
	Table string
	MaxIdle uint32
	MaxOpen uint32
	ConnectTimeout time.Duration
	ReadTimeout time.Duration
	PutFamilys  []string
}

func NewHbaseConfig() *HbaseConfig {
	return &HbaseConfig{
		Hosts: "localhost:9090",
		Table: "tb_test",
		PutFamilys: strings.Split("info", ","),
		MaxIdle: uint32(10),
		MaxOpen: uint32(10),
		ConnectTimeout: 5 * time.Second,
		ReadTimeout:    10 * time.Second,
	}
}

//type TScan struct {
//	StartRow []byte `thrift:"startRow,1" db:"startRow" json:"startRow,omitempty"`
//	StopRow []byte `thrift:"stopRow,2" db:"stopRow" json:"stopRow,omitempty"`
//	Columns []*TColumn `thrift:"columns,3" db:"columns" json:"columns,omitempty"`
//	Caching *int32 `thrift:"caching,4" db:"caching" json:"caching,omitempty"`
//	MaxVersions int32 `thrift:"maxVersions,5" db:"maxVersions" json:"maxVersions,omitempty"`
//	TimeRange *TTimeRange `thrift:"timeRange,6" db:"timeRange" json:"timeRange,omitempty"`
//	FilterString []byte `thrift:"filterString,7" db:"filterString" json:"filterString,omitempty"`
//	BatchSize *int32 `thrift:"batchSize,8" db:"batchSize" json:"batchSize,omitempty"`
//	Attributes map[string][]byte `thrift:"attributes,9" db:"attributes" json:"attributes,omitempty"`
//	Authorizations *TAuthorization `thrift:"authorizations,10" db:"authorizations" json:"authorizations,omitempty"`
//	Reversed *bool `thrift:"reversed,11" db:"reversed" json:"reversed,omitempty"`
//}

//ScanFilters is scanning hbase table filter conditions
type ScanFilters struct {
	Table string
	PointName string
	MinStamp int64
	MaxStamp int64
}

func transTimeToStamp(t time.Time) (int64, error) {
	tStampstr := strconv.FormatInt(t.UTC().UnixNano(), 10)
	tStampstr = tStampstr[:13]
	tStamp, err := strconv.ParseInt(tStampstr, 10, 64)

	return tStamp, err
}

func NewScanFilters(table string, pointName string,
	minTime time.Time, maxTime time.Time) *ScanFilters {
	minStamp, _ := transTimeToStamp(minTime)
	maxStamp, _ := transTimeToStamp(maxTime)

	return &ScanFilters{
		Table:table,
		PointName:pointName,
		MinStamp:minStamp,
		MaxStamp:maxStamp,
	}
}

//ScanConfig is configuration of the scanning hbase table
type ScanConfig struct {
	TScan *hbase.TScan
	Table string
	ReadNum int32
	HaveReads int32
	OneReadNum int32
	Results chan []*hbase.TResult_
	MonitorDatas chan []string
	endScan bool
	endMarshaler bool
	endPrintMonitorDatas bool
}

func NewScanConfig(args ...interface{}) *ScanConfig {
	ScanCfg := ScanConfig{
		TScan: hbase.NewTScan(),
		ReadNum: 0,
		HaveReads: 0,
		OneReadNum: 5000,
		Results: make(chan []*hbase.TResult_, 10),
		MonitorDatas: make(chan []string, 10),
		endScan: false,
		endMarshaler: false,
		endPrintMonitorDatas: false,
	}

	minStamp, maxStamp := int64(0), int64(0)
	pointName := ""
	for _, arg := range args {
		switch arg := arg.(type) {
		case int64:
			if minStamp == int64(0) {
				minStamp = arg
			} else {
				maxStamp = arg
			}

			if maxStamp < minStamp {
				tmpnum := maxStamp
				maxStamp = minStamp
				minStamp = tmpnum
			}
			if minStamp == int64(0) {
				minStamp = maxStamp
			}
		case string:
			pointName = arg
		default:
		}
	}

	if pointName != "" {
		ScanCfg.setFilterOfPoint(pointName)
	}

	if minStamp != int64(0) && maxStamp != int64(0) {
		ScanCfg.setFilterOfTime(minStamp, maxStamp)
	}

	return &ScanCfg
}

//MarshalResults will hbase data formatted into Json string
func (ScanCfg *ScanConfig)marshalResults() {
	for exit := false; exit != true; {
		select {
		case results, ok := <-ScanCfg.Results:
			if !ok {
				exit = true
				break
			}
			mondatas := make([]string, 0)
			for _, result := range results {
				var datamap = make(map[string]string)
				var datamaps = make(map[string]map[string]string)

				for _, columnvalue := range result.ColumnValues {
					if string(columnvalue.Qualifier) != "" {
						datamap[string(columnvalue.Qualifier)] = string(columnvalue.Value)
					}
				}

				rowkey := string(result.Row)
				rowlen := len(rowkey)
				monPoint := rowkey
				if rowlen > 13 {
					monPoint = rowkey[:(rowlen - 13)]
				}
				datamaps[monPoint] = datamap

				jsondata, err := json.Marshal(datamaps)
				if err != nil {
					log.Warn(err.Error())
					continue
				}

				mondatas = append(mondatas, string(jsondata))
			}
			ScanCfg.MonitorDatas <- mondatas
		default:
			if ScanCfg.endScan && len(ScanCfg.Results) == 0 {
				exit = true
				ScanCfg.endMarshaler = true
			}
		}
	}
}

//reads Json string and print to the screen
func (ScanCfg *ScanConfig)PrintMonitorDatas()  {
	for exit := false; exit != true; {
		select {
		case mondatas, ok := <- ScanCfg.MonitorDatas:
			if !ok {
				exit = true
				return
			}
			for _, mondata := range mondatas {
				fmt.Println(mondata)
			}
		default:
			if ScanCfg.endMarshaler && len(ScanCfg.MonitorDatas) == 0 {
				exit = true
				ScanCfg.endPrintMonitorDatas = true
			}
		}
	}
}

//Set the scan hbase table time filter conditions
func (ScanCfg *ScanConfig)setFilterOfTime(minStamp, maxStamp int64) {
	ScanCfg.TScan.TimeRange = hbase.NewTTimeRange()
	ScanCfg.TScan.TimeRange.MinStamp = minStamp
	ScanCfg.TScan.TimeRange.MaxStamp = maxStamp
}

//Set the scan hbase table rowkey filter conditions
func (ScanCfg *ScanConfig)setFilterOfPoint(pointName string) {
	filterString := fmt.Sprintf("RowFilter(=, 'substring:%s')", pointName)
	ScanCfg.TScan.FilterString = []byte(filterString)
}

//Set the scan hbase table rowkey and time filter conditions(not use)
func (ScanCfg *ScanConfig)setFilterOfPointAndTime(pointName string, minStamp, maxStamp int64) {
	minStampStr := strconv.FormatInt(minStamp, 10)
	maxStampStr := strconv.FormatInt(maxStamp, 10)
	ScanCfg.TScan.StartRow = []byte(pointName + string(minStampStr))
	ScanCfg.TScan.StopRow  = []byte(pointName + string(maxStampStr))
}

//Set the scanning filter conditions
func (ScanCfg *ScanConfig)SetScanFilters(filters *ScanFilters) {
	if len(filters.Table) != 0 {
		ScanCfg.Table = filters.Table
	}

	if len(filters.PointName) != 0 {
		ScanCfg.setFilterOfPoint(filters.PointName)
	}

	if filters.MinStamp >= 0 && filters.MaxStamp > 0 {
		ScanCfg.setFilterOfTime(filters.MinStamp, filters.MaxStamp)
	}
}

//End of the scanning
func (ScanCfg *ScanConfig)ScanClose() {
	close(ScanCfg.Results)
	close(ScanCfg.MonitorDatas)
}
