package main

import (
	"fmt"
	"time"
	"strings"
	"errors"
	zk "github.com/samuel/go-zookeeper/zk"
)

var rootNode string = "/go_zk"
type zkOpe struct {
	conn *zk.Conn
}

// 去掉末尾
func trimpSuffix(path *string, suffix byte) {
	for len(*path) > 0 && (*path)[len(*path)-1] == suffix {
		*path = (*path)[:len(*path)-1]
	}
}

// 返回错误信息
func defError(format string, args ...interface{}) (err error) {
	str := fmt.Sprintf(format, args)
	return errors.New(str)
}

// 比较，得到变化
func cmpMaps(oldM, newM map[string]string) (map[string]string, map[string]string) {
	add := make(map[string]string)
	del := make(map[string]string)
	for k := range newM {
		_, ok := oldM[k]
		if ok {
			delete(oldM, k)
		} else {
			add[k] = newM[k]
		}
	}
	
	del = oldM
	return add, del // 返回新增的和删掉的
}

// 连接
func (f *zkOpe) Connect(servers []string, t time.Duration) (err error) {
	conn, _, err := zk.Connect(servers, t)
	if err != nil {
		return err
	}
	
	f.conn = conn
	return nil
}

// 创建节点
func (f *zkOpe) CreateNode(path string, value string) (err error) {
	trimpSuffix(&path, '/')
	exists, _, err := f.conn.Exists(path)
	if err != nil || exists {
		return err
	}
	
	var sub string
	a := strings.SplitN(path, "/", -1)
	for i:= 0; i < len(a)-1; i++{
		if len(a[i]) == 0 {
			continue
		}
		
		sub += "/" + a[i]
		fmt.Println(sub)
		exists, _, err = f.conn.Exists(sub)
		if err != nil {
			return err
		}
		if !exists {
			f.conn.Create(sub, []byte("--"), int32(0), zk.WorldACL(zk.PermAll))
		}
	}
		
	_, err = f.conn.Create(path, []byte(value), int32(0), zk.WorldACL(zk.PermAll))
	
	return err
}

// 监控子节点个数变化
func (f *zkOpe) WatchChildren(path string, addC, delC chan map[string]string) (err error){
	trimpSuffix(&path, '/')
	
	oldM := make(map[string]string)
	newM := make(map[string]string)
	go func() {
		for {
			snapshot, stat, events, err := f.conn.ChildrenW(path)
			if err != nil || stat == nil {
				delC <- oldM // delete all nodes
				return
			}

			for i := range snapshot {
				newM[path+"/"+snapshot[i]] = " "
			}
			
			add, del := cmpMaps(oldM, newM)
			oldM = newM
			newM = map[string]string{}
			if len(add) > 0 {
				addC <- add
			}
			if len(del) > 0 {
				delC <- del
			}
			
			evt := <- events
			if evt.Err != nil {
				fmt.Println(evt.Err, path)
				delC <- oldM
				return
			}
		}
	} ()
	
	return nil
}

// 收集子节点的个数变化
func (f *zkOpe) CollectChildren(addC, delC chan map[string]string) (err error){
	go func() {
		for {
		select {
			case add:= <- addC: {
				fmt.Println("add", add)
			}
			case del:= <- delC: {
				fmt.Println("del", del)
				}
			}
		}
	}()
	return nil
}


type KVStruct struct {
	K string
	V string
}
// 监控节点值的变化
func (f *zkOpe) WatchNode(node string, addC, delC chan KVStruct) (err error) {
	trimpSuffix(&node, '/')
	
	go func() {
		for {
			value, stat, events, err := f.conn.GetW(node)
			if err != nil || stat == nil {
				delC <- KVStruct{node, ""}
				return
			}
			
			addC <- KVStruct{node, string(value)}
			
			evt := <- events
			if evt.Err != nil {
				delC <- KVStruct{node, ""}
				fmt.Println(evt.Err, node)
				return
			}
		}
	} ()
	return nil
}

// 收集值的变化
func (f *zkOpe) CollectNode(addC, delC chan KVStruct) (err error){
	go func() {
		for {
			select {
				case add := <- addC: {
					fmt.Println("addV", add)
				}
				case del := <- delC: {
					fmt.Println("delV", del)
				}
			}
		}
	}()
	
	return nil
}

func main() {
	servers := []string{"10.15.144.71:2181", "10.15.144.72:2181", "10.15.144.73:2181"}

	f := zkOpe{}
	f.Connect(servers, time.Second*10)
	defer f.conn.Close()

	f.CreateNode("/go_zk/kk/", "256")

	addC := make(chan map[string]string)
	delC := make(chan map[string]string)
	f.WatchChildren("/go_zk/kk", addC, delC)
	f.WatchChildren("/go_zk/vv", addC, delC)
	f.CollectChildren(addC, delC)
	
	
	addV := make(chan KVStruct)
	delV := make(chan KVStruct)
	f.WatchNode("/go_zk/kk/", addV, delV)
	f.WatchNode("/go_zk/vv/", addV, delV)
	f.CollectNode(addV, delV)

	
	
	time.Sleep(time.Second*1000)
}