package main

import (
	"connetor"

	"os"
	"os/signal"
	"time"
)

func main() {
	//设置kafka配置
	kafkaCfg := connetor.NewKafkaConfig()
	kafkaCfg.Brokers = "192.168.1.45:9092,192.168.1.32:9092"
	kafkaCfg.ConsumerTopic = "test11"
	kafkaCfg.ProducerTopic = "test0"

	//设置hbase配置
	hbaseCfg := connetor.NewHbaseConfig()
	hbaseCfg.Table = "tb_test"
	hbaseCfg.Hosts = "192.168.1.45:9090,192.168.1.32:9090"
	//hbaseCfg.MaxIdle = 10
	//hbaseCfg.MaxOpen = 10

	//创建连接器，之后再启动连接器。
	connet := connetor.CreateConnetor(kafkaCfg, hbaseCfg)
	connet.StartConnetor()
	defer connet.Close()
	//接收kafka test11 topic 中的测点数据并写入到hbase tb_test表中
	go func() {
		connet.ConnetKafkatoHbase()
	}()

	var t0, t1 time.Time
	//将从hbase中查询到的数据格式化成json字符串后再写入到kafka中
	go func() {
		connet.ConnetHbasetoKafka("tb_test", "Group19", t0, t1)
	}()

	//按设置好的查询条件来查询hbase数据，可通过返回的ScanCfg中的通道变量读取查询到的数据。
	ScanCfg := connet.ConnetScanHbase("tb_test", "Group19", t0, t1)
	//将查询到的数据打印到屏幕
	ScanCfg.PrintMonitorDatas()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	select {
	case <-signals:
		//结束此次的查询
		ScanCfg.ScanClose()
		return
	}
}
