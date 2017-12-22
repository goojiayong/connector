package connector

import (
	"time"
	"reflect"
	"strconv"
	"strings"

	"github.com/goojiayong/hbase"
	"github.com/collinmsn/thrift-client-pool"
	"git.apache.org/thrift.git/lib/go/thrift"
)

func getTimeStamp() string {
	t := time.Now()
	timeStamp := strconv.FormatInt(t.UTC().UnixNano(), 10)
	strTStamp := timeStamp[:13]
	//formatTime := t.Format("2006-01-02 15:04:05")
	//formatTime = strings.Replace(formatTime, " ", "", -1)

	//randnum := strconv.Itoa(rand.Intn(10))

	return strTStamp
}

func getTPuts(monData *MonitorPoints, putFamilys []string) []*hbase.TPut {
	tPuts := make([]*hbase.TPut, 0)
	putFamily := putFamilys[0]
	for key, value := range *monData {
		tPut := hbase.NewTPut()
		tStamp := getTimeStamp()
		tPut.Row = []byte(key + tStamp)

		ColumnValues := make([]*hbase.TColumnValue, 0)
		t := reflect.TypeOf(value)
		v := reflect.ValueOf(value)
		for k := 0; k < t.NumField(); k++ {
			ColumnValue := hbase.NewTColumnValue()
			ColumnValue.Family    = []byte(putFamily)
			ColumnValue.Qualifier = []byte(t.Field(k).Name)
			ColumnValue.Value     = []byte(v.Field(k).String())
			tmpStamp, _ := strconv.ParseInt(tStamp, 10, 64)
			ColumnValue.Timestamp = &tmpStamp

			ColumnValues = append(ColumnValues, ColumnValue)
		}

		tPut.ColumnValues = ColumnValues
		tPuts = append(tPuts, tPut)
	}

	return tPuts
}

func putDataToHbase(monData *MonitorPoints, pool *thrift_client_pool.ChannelClientPool, hbaseCfg *HbaseConfig) error {
	// get client from pool
	var pooledClient thrift_client_pool.PooledClient
	var hbaseClient *hbase.THBaseServiceClient
	var err error
	for i := uint32(0);; {
		pooledClient, err = pool.Get()
		if err != nil {
			return err
		}

		// use client
		hbaseClient = pooledClient.RawClient().(*hbase.THBaseServiceClient)
		_, err = hbaseClient.GetAllRegionLocations([]byte(hbaseCfg.Table))
		if err == nil {
			defer pooledClient.Close()
			break
		}

		pooledClient.MarkUnusable()
		pooledClient.Close()
		if i++; i > hbaseCfg.MaxOpen {
			return err
		}
	}

	tPuts := getTPuts(monData, hbaseCfg.PutFamilys)
	for _, tput := range tPuts {
		err := hbaseClient.Put([]byte(hbaseCfg.Table ), tput)
		if err != nil {
				pooledClient.MarkUnusable()
			return err
		}
	}

	return nil
}

func scanHbaseTable(ScanCfg *ScanConfig, pool *thrift_client_pool.ChannelClientPool, hbaseCfg *HbaseConfig) error {
	table := []byte(ScanCfg.Table)
	tScan := ScanCfg.TScan

	// get client from pool
	var pooledClient thrift_client_pool.PooledClient
	var hbaseClient *hbase.THBaseServiceClient
	var err error
	for i := uint32(0);; {
		pooledClient, err = pool.Get()
		if err != nil {
			return err
		}

		// use client
		hbaseClient = pooledClient.RawClient().(*hbase.THBaseServiceClient)
		_, err = hbaseClient.GetAllRegionLocations([]byte(hbaseCfg.Table))
		if err == nil {
			defer pooledClient.Close()
			break
		}

		pooledClient.MarkUnusable()
		pooledClient.Close()
		if i++; i > hbaseCfg.MaxOpen {
			return err
		}
	}
	sId, err := hbaseClient.OpenScanner(table, tScan)
	if err != nil {
		return err
	}
	defer hbaseClient.CloseScanner(sId)

	readCount := int32(0)
	for {
		results, err := hbaseClient.GetScannerResults(table, tScan, ScanCfg.OneReadNum)
		if err != nil {
			break
		}

		curlen := len(results)
		if readCount > 0 {
			results = results[1:]
		}
		ScanCfg.Results <- results
		readCount += int32(len(results))
		ScanCfg.HaveReads = readCount
		if ScanCfg.ReadNum != 0 && readCount >= ScanCfg.ReadNum {
			break
		}

		if int32(curlen) < ScanCfg.OneReadNum {
			break
		}

		tScan.StartRow = results[(len(results)-1)].Row
	}

	ScanCfg.endScan = true
	return err
}

func getThriftClientPool(cfg *HbaseConfig) *thrift_client_pool.ChannelClientPool {
	maxIdle := cfg.MaxIdle
	maxOpen := cfg.MaxOpen
	servers := strings.Split(cfg.Hosts, ",")
	connectTimeout := cfg.ConnectTimeout
	readTimeout := cfg.ReadTimeout
	if maxIdle < 1 {maxIdle = 1}
	if maxOpen < 1 {maxOpen = 1}

	pool := thrift_client_pool.NewChannelClientPool(maxIdle, maxOpen, servers,
		connectTimeout, readTimeout, func (openedSocket thrift.TTransport) thrift_client_pool.Client {
			return hbase.NewTHBaseServiceClientFactory(openedSocket, thrift.NewTBinaryProtocolFactoryDefault())
		},
	)

	return pool
}
