/*
 * saferwmap: add read/write lock for map.
*/

package saferwmap

import (
	"sync"
	"fmt"
)

type SafeRWMap struct{
	lock *sync.RWMutex
	m    map[interface{}] interface{} 
}

func NewsafeRWMap() *SafeRWMap {
	return &SafeRWMap {
		lock: new(sync.RWMutex),
		m   : make(map[interface{}] interface{}),
	}
}

func (m *SafeRWMap) Set(k interface{}, v interface{}) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	m.m[k] = v
	fmt.Println("Set: ", k, v)
	return true
}

func (m *SafeRWMap) Get(k interface{}) interface{} {
	m.lock.RLock()
	defer m.lock.RUnlock()
	
	if v, ok := m.m[k]; ok {
		return v
	}
	
	return nil
}

func (m *SafeRWMap) Del(k interface{}) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	delete(m.m, k)
	fmt.Println("Del: ", k)
	return true
}