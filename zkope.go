package main

import (
	"fmt"
	"time"
	"strings"
	"errors"
	smap "saferwmap"
	zk "github.com/samuel/go-zookeeper/zk"
)

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

////////////////////////////////////// base Ope
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
func (f *zkOpe) WatchChildren(path string, addC, delC chan <- map[string]string) (err error){
	trimpSuffix(&path, '/')
	oldM := make(map[string]string)
	newM := make(map[string]string)
	go func() {
		for {
			snapshot, stat, events, err := f.conn.ChildrenW(path)
			if err != nil || stat == nil {
				fmt.Println("ChildrenW:", path, err, stat)
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
//func (f *zkOpe) CollectChildren(addC, delC <- chan map[string]string) (err error){
//	go func() {
//		for {
//		select {
//			case add:= <- addC: {
//				fmt.Println("add", add)
//			}
//			case del:= <- delC: {
//				fmt.Println("del", del)
//				}
//			}
//		}
//	}()
//	return nil
//}

// 收集子节点的个数变化，并将子节点的值加入监控
func (f *zkOpe) CollectChildren2(addC, delC chan map[string]string, addV, delV chan <- KVStruct) (err error){
	go func() {
		for {
			select {
				case add:= <- addC: {
					for i := range add {
						f.WatchChildren(i, addC, delC)
						f.WatchNode(i, addV, delV)
					}
				}
				case <- delC: {
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
func (f *zkOpe) WatchNode(node string, addV, delV chan <- KVStruct) (err error) {
	trimpSuffix(&node, '/')
	
	go func() {
		for {
			value, stat, events, err := f.conn.GetW(node)
			if err != nil || stat == nil {
				delV <- KVStruct{node, ""}
				return
			}
			
			addV <- KVStruct{node, string(value)}
			
			evt := <- events
			if evt.Err != nil {
				delV <- KVStruct{node, ""}
				fmt.Println(evt.Err, node)
				return
			}
		}
	} ()
	return nil
}

// 收集值的变化
//func (f *zkOpe) CollectNode(addV, delV <- chan KVStruct) (err error){
//	go func() {
//		for {
//			select {
//				case add := <- addV: {
//					fmt.Println("addV", add)
//				}
//				case del := <- delV: {
//					fmt.Println("delV", del)
//				}
//			}
//		}
//	}()
//	
//	return nil
//}

// 收集值的变化
func (f *zkOpe) CollectNode2(addV, delV <- chan KVStruct, sm smap.SafeRWMap) (err error){
	go func() {
		for {
			select {
				case add := <- addV: {
					sm.Set(add.K, add.V)
				}
				case del := <- delV: {
					sm.Del(del.K)
				}
			}
		}
	}()
	
	return nil
}
////////////////////////////////////// 

// 获取zookeeper当前状态的镜像
func GetZookeeperImage(servers []string, path string) {
	go func() {
		sm := smap.NewsafeRWMap()
		f := zkOpe{}
		f.Connect(servers, time.Second*10)
		defer f.conn.Close()
		
		f.CreateNode(path, "--")
		addC := make(chan map[string]string, 1024)
		delC := make(chan map[string]string, 1024)
		addV := make(chan KVStruct, 1024)
		delV := make(chan KVStruct, 1024)
		f.WatchChildren(path, addC, delC)
		f.CollectChildren2(addC, delC, addV, delV)
		f.CollectNode2(addV, delV, *sm)
		time.Sleep(time.Second*1000)
	}()
}

func main() {
	servers := []string{"10.15.144.71:2181", "10.15.144.72:2181", "10.15.144.73:2181"}
	GetZookeeperImage(servers, "/go_zk")
	
	time.Sleep(time.Second*1000)
}
