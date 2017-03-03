// Date : 21/2/17 AM10:03
// Copyright: TradeShift.com
// Author = 'liming'
// Email = 'lim@cn.tradeshift.com'
package main

import (
  	riak "github.com/lmlala/riak-go/lib"
	"baiwangmaoyi.com/common"
	"fmt"
	"strconv"
	//"time"
	"sync"
	//"time"
	"runtime"
)


type input_kv struct {
	key	string
	value	string
}

var wg sync.WaitGroup
type MainCFG struct {
	B	string	`yaml:"bucket"`
	BType	string	`yaml:"bucket_type"`
	Routines	int	`yaml:"routines"`
	TotalKey	int	`yaml:"total_key"`
}
var cfg MainCFG

func init(){
	err := common.YamlLoad("./config.yml", &cfg)
	if err != nil {
		panic("Can not read main config")
	}

	fmt.Println(cfg)

}

func datadelivery(ch chan input_kv) {
	fmt.Println("Start to delivery data")
	for i := 1; i < cfg.TotalKey + 1; i++ {
		k := "key" + strconv.Itoa(i) + ".txt"
		v := "value" + strconv.Itoa(i)
		item := input_kv{k, v}
		//fmt.Printf("Delivered %s\n", k)
		ch <- item
		wg.Add(1)
	}
	// close input chanel
	close(ch)
	fmt.Println("delivery is finish")
	fmt.Printf("still have %d data in channel\n", len(ch))
}

func handler(th string, in_ch chan input_kv, out_ch chan string){
	//fmt.Println("haha")
	for {

		item, ok := <- in_ch
		//fmt.Println(ok)
		if ok == false {
			//fmt.Println("nil key from channel")
			continue
		}
		//fmt.Printf("Routine %s struct: %s, left num %d\n", th, item, len(in_ch))

		e := riak.SetKv(cfg.BType, cfg.B, item.key, item.value)
		if e != nil {
			fmt.Printf("Routine %s Error: %s\n", th, item.key)
			fmt.Println(e)
		} else {
			//fmt.Printf("Routine %s Success: %s\n",th, item.key)
		}
		out_ch <- item.key
		wg.Done()
	}

}

func main() {

	// set cpu using
	runtime.GOMAXPROCS(runtime.NumCPU())

	// blocked input
	input := make(chan input_kv, cfg.TotalKey)
	output := make(chan string, cfg.TotalKey)


	//datadelivery(input)
	//time.Sleep(time.Second * 5)
	for i := 1; i < cfg.Routines + 1; i ++ {
		fmt.Printf("Start to store data, routine %d\n", i)
		go handler(strconv.Itoa(i), input, output)
	}

	// insert values
	datadelivery(input)
	//time.Sleep(time.Second * 3)
	//handler("10", input, output)
	//for {
	//	select {
	//	case	a := <- output:
	//		fmt.Printf("data store success: %s\n", a)
	//	default:
	//		fmt.Printf("Total data length is: %d\n", len(output))
	//	}
	//}

	wg.Wait()
	//for {
	//	l := len(output)
	//	if l != cfg.TotalKey {
	//		time.Sleep(time.Second * 1)
	//		//fmt.Println("not finished")
	//	} else {
	//	  	fmt.Printf("Total data length is: %d\n", len(output))
	//		break
	//	}
	//}
	fmt.Printf("Total data length is: %d\n", len(output))
}
