// Date : 21/2/17 AM10:04
// Copyright: TradeShift.com
// Author = 'liming'
// Email = 'lim@cn.tradeshift.com'
package lib

import (
	"github.com/tpjg/goriakpbc"
  	//util "github.com/basho/taste-of-riak/go/util"
	"baiwangmaoyi.com/common"
	//"sync"
	//"fmt"
)

var logger = common.GETLOGGER("/data/logs/riak-go.log", "debug")
//func connect() (err error, c *riak.Client){
//
//	o := &riak.NewClientOptions{
//	RemoteAddresses: []string{"192.168.100.2:8087","192.168.100.11:8087","192.168.100.12:8087"},
//	}
//
//	c, err = riak.NewClient(o)
//	if err != nil {
//	return err, nil
//	}
//
//	defer func() {
//	if err := c.Stop(); err != nil {
//	return err, nil
//	}
//	}()
//	return nil, c
//
//}

//func KvGet(bucket, key string) (err error, v string){
//	var cmd riak.Command
//	cmd, err = riak.NewFetchValueCommandBuilder().
//	WithBucket(bucket).
//	WithKey(o.Key).
//	Build()
//	if err != nil {
//	util.ErrLog.Println(err)
//	continue
//	}
//	a := &riak.Async{
//	Command: cmd,
//	Wait:    wg,
//	Done:    d,
//	}
//	if err := c.ExecuteAsync(a); err != nil {
//	util.ErrLog.Println(err)
//	}
//
//}
type RiakCFG struct {
	Addr	string	`yaml:"riak_addr"`
	MaxConn	int	`yaml:"riak_max_conn"`
}
var cfg RiakCFG

func init(){

	err := common.YamlLoad("./config.yml", &cfg)
	if err != nil {
		panic("Can not read riak config")
	}
	err = riak.ConnectClientPool(cfg.Addr, cfg.MaxConn)
	//err = riak.ConnectClient(cfg.Addr)
	if err != nil {
		panic("Can not connect to riak")
	}
	//defer riak.Close()

}

func _conn() (err error){

	err = riak.ConnectClient(cfg.Addr)
	if err != nil {
		logger.Error("Bad Riak Connection!!!")
		return err
	} else {
		return nil
	}

	//defer riak.Close()
	return err
}

func GetKv(t, b, k string) (err error, r []byte){
	//err = _conn()
	//
	//if err != nil {
	//	return err, nil
	//}
	//defer riak.Close()

	bucket, err := riak.NewBucketType(t, b)
	if err != nil {
		logger.Error("Error init bucket [", t, " - ", b, "] ", err)
		return err, nil
	}

	//fmt.Println(bucket.ListKeys())
	//obj := bucket.NewObject(k)
	//obj.ContentType = "application/json"
	//obj.ContentType = "text/plain"
	obj, err := bucket.Get(k)
	if err != nil {
		logger.Error("Error get key [", k, "] ", err)
		return err, nil
	}

	//a, _ := bucket.ListKeys()
	//for i, _ := range  a{
	//	fmt.Println(string(i))
	//}

	//fmt.Println(obj.Data)
	return nil, obj.Data
}

func SetKv(t, b, k, v string) (err error){
	//err = _conn()
	//
	//if err != nil {
	//	return err
	//}
	//defer riak.Close()

	bucket, err := riak.NewBucketType(t, b)
	if err != nil {
		logger.Error("Error init bucket [", t, " - ", b, "] ", err)
	}

	obj := bucket.NewObject(k)
	//obj.ContentType = "application/json"
	obj.ContentType = "text/plain"
	obj.Data = []byte(v)
	err = obj.Store()

	return err
}