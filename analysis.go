package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	_ "fmt"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

const HANDLE_DIG = " /dig?"

type cmdParams struct {
	logFilePath string
	routineNum  int
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlData struct {
	data digData
	uid  string
}

type urlNode struct {
}

var log = logrus.New()

//在golang项目里面 自动回先执行init初始化函数
func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode        urlNode
}

func main() {
	//获取参数
	logFilePath := flag.String("logFilePath", "/usr/local/var/logs/nginx/dig.log", "default dig log file path")
	routineNum := flag.Int("routineNum", 5, "goroutine num")
	runLog := flag.String("runLog", "/tmp/log", "this application runtime log")
	flag.Parse()

	params := cmdParams{
		logFilePath: *logFilePath,
		routineNum:  *routineNum,
	}

	//打日志
	logFd, err := os.OpenFile(*runLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	log.Infoln("Exec start")
	log.Infoln("params: logFilePath=%s, routineNum=%d", params.logFilePath, params.routineNum)

	//初始化一些channel 用于数据传递
	var logChannel = make(chan string, 3*params.routineNum)
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	//日志消费者
	go readFileLinebyLine(params, logChannel)

	//创建一组日志处理 日志解析
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	//创建pv uv 统计器 统计需求 可扩展的xxxCounter需求 用于新增统计分析需求
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel)

	//创建存储器
	go dataStorage(storageChannel)

	time.Sleep(1000 * time.Second) //用于开发调试 防止提前退出主程序
}

func dataStorage(storageChannel chan storageBlock) {

}

func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {

}

func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock) {

}

func logConsumer(logChannel chan string, pvChannel chan urlData, uvChannel chan urlData) error {
	for logStr := range logChannel {
		//切割日志字符串 扣出打点上报的数据
		data := cutLogFetchData(logStr)

		//uid
		//说明：课程中无法收集 是因为这里面的日志都是我们自己手动创建生成的 所以需要模拟生成uid md5(refer+ua)
		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		//很多解析的工作都可以放到这里去完成 因为这里的实例比较简单
		uData := urlData{
			data: data,
			uid:  uid,
		}
		log.Infoln(uData)
		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

func cutLogFetchData(logStr string) digData {
	logStr = strings.TrimSpace(logStr)
	pos1 := str.IndexOf(logStr, HANDLE_DIG, 0)
	if pos1 == -1 {
		return digData{}
	}
	pos1 += len(HANDLE_DIG)
	pos2 := str.IndexOf(logStr, " HTTP/", pos1)
	d := str.Substr(logStr, pos1, pos2-pos1)

	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query()
	return digData{
		time:  data.Get("time"),
		url:   data.Get("refer"),
		refer: data.Get("url"),
		ua:    data.Get("ua"),
	}
}

func readFileLinebyLine(params cmdParams, logChannel chan string) error {
	fd, err := os.OpenFile(params.logFilePath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		log.Warningf("can't open this file:%s", params.logFilePath)
		return err
	}
	defer fd.Close()

	count := 0
	bufferRead := bufio.NewReader(fd)
	for {
		line, err := bufferRead.ReadString('\n')
		logChannel <- line
		log.Infof("line: " + line)
		count++

		if count%(1000*params.routineNum) == 0 {
			log.Infof("ReadFileLinebyLine line : %d", count)
		}

		if err != nil {
			if err == io.EOF {
				time.Sleep(3 * time.Second)
				log.Infof("ReadFileLinebyLine wait. readline:%d", count)
			} else {
				log.Warningf("ReadFileLinebyLine read log error")
			}
		}
	}
	return nil
}
