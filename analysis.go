package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	_ "fmt"
	"github.com/mediocregopher/radix.v2/pool"
	// "github.com/mediocregopher/radix.v2/redis"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const HANDLE_DIG = " /dig?"
const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

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
	data  digData
	uid   string
	unode urlNode
}

type urlNode struct {
	unType string // /movie/ 或者是 /list/ 首页 或者列表页
	unRid  int    // Resource Id 资源ID
	unUrl  string // 当前这个页面的url
	unTime string //当前访问这个页面的时间
}

var log = logrus.New()

//redis这种最好设置全局 在并发模型中会出问题 所以建议使用连接池
// var redisCli redis.Client

//在golang项目里面 自动回先执行init初始化函数
func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
	// redisCli, err := redis.Dial("tcp", "localhost:6379")
	// if err != nil {
	// 	log.Fatalln("Redis connect failed")
	// } else {
	// 	defer redisCli.Close()
	// }
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

	// Redis连接池
	redisPool, err := pool.New("tcp", "localhost:6379", 2*params.routineNum)
	if err != nil {
		log.Fatalln("Redis pool created failed.")
		panic(err)
	} else {
		//连接成功后 在空闲时候没有什么日志产生的时候也一直ping redis server 保持持久
		go func() {
			for {
				redisPool.Cmd("PING")
				time.Sleep(3 * time.Second)
			}
		}()
	}

	//日志消费者
	go readFileLinebyLine(params, logChannel)

	//创建一组日志处理 日志解析
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	//创建pv uv 统计器 统计需求 可扩展的xxxCounter需求 用于新增统计分析需求
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel, redisPool)

	//创建存储器
	go dataStorage(storageChannel, redisPool)

	time.Sleep(1000 * time.Second) //用于开发调试 防止提前退出主程序
}

// 对于大企业级存储使用的一般是HBase 劣势:列簇需要声明清楚(因为业务也已经固定) 对于小企业变化 使用redis也可以cover
func dataStorage(storageChannel chan storageBlock, redisPool *pool.Pool) {
	for block := range storageChannel {
		prefix := block.counterType + "_"

		// 逐层添加,加洋葱皮的过程 访问了子页面 父类页面的pv也要加1 例如访问了
		// http://localhost:8888/movie/12917.html 则对于http://localhost:8888/movie/上一层也要加pv 1 次
		// 即是对于访问的上一个分类访问pv都要加1
		// 区分维度: 天-小时-分钟
		// 层级:顶级-大分类-小分类-终极页面
		// 存储模型: Redis SortedSet 有序集合
		setKeys := []string{
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}

		rowId := block.unode.unRid
		for _, key := range setKeys {
			ret, err := redisPool.Cmd(block.storageModel, key, 1, rowId).Int()
			if ret <= 0 || err != nil {
				log.Errorln("DataStorage redis storage error.", block.storageModel, key, rowId)
			} else {

			}
		}
	}
}

//一天访问多少次
func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		sItem := storageBlock{
			counterType:  "pv",
			storageModel: "ZINCRBY",
			unode:        data.unode,
		}
		storageChannel <- sItem
	}
}

//一天访问有多少人 需要做去重处理
func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock, redisPool *pool.Pool) {
	for data := range uvChannel {
		// HyperLoglog redis 来去重
		hyperLogLogKey := "uv_hpll_" + getTime(data.data.time, "day")
		ret, err := redisPool.Cmd("PFADD", hyperLogLogKey, data.uid, "EX", 86400).Int()
		if err != nil {
			log.Warningln("UvCounter check redis hyperloglog failed ", err)
		}
		if ret != 1 {
			continue
		}

		// insert success
		sItem := storageBlock{
			counterType:  "uv",
			storageModel: "ZINCRBY",
			unode:        data.unode,
		}
		storageChannel <- sItem
	}
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
			data:  data,
			uid:   uid,
			unode: formatUrl(data.url, data.time),
		}
		// log.Infoln(uData)
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

func formatUrl(url, t string) urlNode {
	// 一定从量大的着手, 详情页>列表页>首页
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0)
	if pos1 != -1 {
		// 详情页
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		idStr := str.Substr(url, pos1, pos2-pos1)
		id, _ := strconv.Atoi(idStr)
		return urlNode{unType: "movie", unRid: id, unUrl: url, unTime: t}
	} else {
		// 列表页
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		if pos1 != -1 {
			pos1 += len(HANDLE_MOVIE)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1)
			id, _ := strconv.Atoi(idStr)
			return urlNode{unType: "list", unRid: id, unTime: t}
		} else {
			// 首页
			return urlNode{unType: "home", unRid: 1, unUrl: url, unTime: t}
		}
	}
	//如果页面url有很多种 就在这里扩展
}

func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
	case "hour":
		item = "2006-01-02 15"
	case "min":
		item = "2006-01-02 15:04"
	}
	t, _ := time.Parse(item, time.Now().Format(item))
	return strconv.FormatInt(t.Unix(), 10)
}
