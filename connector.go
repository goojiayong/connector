//connet kafka and hbase
package connector

import (
	"github.com/collinmsn/thrift-client-pool"
	"github.com/Shopify/sarama"
	log "common-core/aclog"
	"runtime"
	"time"
)

func init() {
	ostype := runtime.GOOS
	if ostype == "windows" {
		log.InitLog("..\\config\\config.ini")
	} else if ostype == "linux" {
		log.InitLog("../config/config.ini")
	}
}

//Connector kafka and hbase, can read and write data in kafka and hbase.
type Connetor struct {
	totalRuntineNum uint32
	clientPool  *thrift_client_pool.ChannelClientPool
	kafkaCfg *KafkaConfig
	hbaseCfg *HbaseConfig
	putRuntineNum uint32
	scanRuntineNum uint32

	KafkaConsumer chan sarama.ConsumerMessage
	KafkaProducer chan string

	HbasePut chan []byte
	HbaseScan chan *ScanConfig
	//Result chan error;
	//Queue chan func() error;
	//FinishCallback func();
}

func CreateConnetor(kafkaCfg *KafkaConfig, hbaseCfg *HbaseConfig) *Connetor {
	connetor := &Connetor{}
	connetor.totalRuntineNum = hbaseCfg.MaxOpen
	connetor.hbaseCfg = hbaseCfg
	connetor.kafkaCfg = kafkaCfg
	connetor.clientPool = getThriftClientPool(hbaseCfg)
	connetor.scanRuntineNum = connetor.totalRuntineNum / 2
	connetor.putRuntineNum = connetor.totalRuntineNum - connetor.scanRuntineNum
	if connetor.scanRuntineNum <= 0 {connetor.scanRuntineNum = 1}
	if connetor.putRuntineNum  <= 0 {connetor.putRuntineNum = 1}

	connetor.HbasePut = make(chan[]byte, connetor.putRuntineNum)
	connetor.HbaseScan = make(chan *ScanConfig, connetor.scanRuntineNum)
	//self.Total = hbaseCfg.MaxIdle
	//self.Queue = make(chan func() error, total);
	//self.Result = make(chan error, total);

	return connetor
}

//connet kafka and hbase
func (connetor *Connetor)StartConnetor() {
	connetor.connetKafka()
	connetor.connetHbase()
}

func (connetor *Connetor)connetKafka() {
	go func() {
		Producer(connetor.kafkaCfg)
	}()

	go func() {
		Consumer(connetor.kafkaCfg)
	}()

	connetor.KafkaConsumer = connetor.kafkaCfg.cmessages
	connetor.KafkaProducer = connetor.kafkaCfg.pmessages
}

func (connetor *Connetor)connetHbase() {
	connetor.hbasePutData()
	connetor.hbaseScanTable()
}

//Start multiple write data to the hbase goroutine
func (connetor *Connetor)hbasePutData()  {
	for j := uint32(0); j < connetor.putRuntineNum; j++ {
		go func() {
			for exit := false; exit != true; {
				select {
				case sourceData, ok := <- connetor.HbasePut:
					if !ok {
						exit = true
						break
					}
					monitor, err := TransMonitorData(sourceData)
					if err != nil {
						log.Warn(err)
						continue
					}
					err = putDataToHbase(monitor,
						connetor.clientPool, connetor.hbaseCfg)
					if err != nil {
						log.Warn(err)
						continue
					}
				}
			}
		}()
	}
}

//Start multiple scan hbase table goroutine
func (connetor *Connetor)hbaseScanTable() {
	for j := uint32(0); j < connetor.scanRuntineNum; j++ {
		go func() {
			for exit := false; exit != true; {
				select {
				case scanCfg, ok := <- connetor.HbaseScan:
					if !ok {
						exit = true
						break
					}
					if scanCfg.Table == "" {
						scanCfg.Table = connetor.hbaseCfg.Table
					}

					err := scanHbaseTable(scanCfg, connetor.clientPool, connetor.hbaseCfg)
					if err != nil {
						log.Warn(err)
					}
				}
			}
		}()
	}
}

//Receive kafka data is written to the hbase
func (connetor *Connetor)ConnetKafkatoHbase() {
	for exit := false; exit != true; {
		select {
		case consumerData, ok := <- connetor.KafkaConsumer:
			if !ok {
				exit = true
			}
			connetor.HbasePut <- consumerData.Value
		}
	}
}

//Receive hbase data is written to the kafka
func (connetor *Connetor)ConnetHbasetoKafka(table string, pointName string,
	minTime time.Time, maxTime time.Time) {
	ScanCfg := NewScanConfig()

	filters := NewScanFilters(table, pointName,
		minTime, maxTime)

	ScanCfg.SetScanFilters(filters)

	connetor.HbaseScan <- ScanCfg

	go func() {
		ScanCfg.marshalResults()
	}()

	for exit := false; exit != true; {
		select {
		case scanDatas, ok := <- ScanCfg.MonitorDatas:
			if !ok {
				exit = true
				break
			}
			for _, scanData := range scanDatas {
				connetor.KafkaProducer <- scanData
			}
		default:
			if ScanCfg.endMarshaler && len(ScanCfg.MonitorDatas) == 0 {
				exit = true
				ScanCfg.endPrintMonitorDatas = true
			}
		}
	}
}

//scan hbase table
func (connetor *Connetor)ConnetScanHbase(table string, pointName string,
	minTime time.Time, maxTime time.Time) *ScanConfig {
	ScanCfg := NewScanConfig()

	filters := NewScanFilters(table, pointName,
		minTime, maxTime)

	ScanCfg.SetScanFilters(filters)

	connetor.HbaseScan <- ScanCfg

	go func() {
		ScanCfg.marshalResults()
	}()

	return ScanCfg
}

//close connector
func (connetor *Connetor)Close() {
	connetor.kafkaCfg.closeConsumer()
	connetor.kafkaCfg.closeProducer()
	close(connetor.HbasePut)
	close(connetor.HbaseScan)
}