package workerpool

import (
	"sync"
	"time"
)

type WorkerPool struct {
	nMaxGoroutines     int
	nMaxIdleGoroutines int
	nMaxIdleTime       time.Duration
	dataCh             chan interface{}
	fHandler           func(interface{})
	nWorkingCount      int //working goroutine count
	nIDLECount         int //idle goroutine count
	pendingItems       int //items count before being handled
	locker             sync.RWMutex
}

func NewWorkerPool(channelsize int) *WorkerPool {
	if channelsize < 1 {
		channelsize = 1
	}

	obj := &WorkerPool{
		nMaxGoroutines:     32,
		nMaxIdleGoroutines: 1,
		nMaxIdleTime:       time.Second * 120,
		dataCh:             make(chan interface{}, channelsize),
		nWorkingCount:      0,
		nIDLECount:         0,
		pendingItems:       0,
	}

	return obj
}

func (t *WorkerPool) SetHandler(fn func(interface{})) {
	t.fHandler = fn
}

func (t *WorkerPool) SetMaxGoroutine(nMaxCount int) {
	if nMaxCount > 0 {
		t.nMaxGoroutines = nMaxCount
	}
}

func (t *WorkerPool) SetMaxIdleGoroutine(nMaxCount int) {
	if nMaxCount >= 0 {
		t.nMaxIdleGoroutines = nMaxCount
	}
}

func (t *WorkerPool) SetMaxIdleTime(d time.Duration) {
	t.nMaxIdleTime = d
}

func (t *WorkerPool) PushItem(a interface{}) {
	t.locker.Lock()
	t.pendingItems += 1
	t.locker.Unlock()

	t.dataCh <- a

	var bCreate bool = false
	t.locker.Lock()
	if t.nIDLECount < len(t.dataCh) && t.nWorkingCount+t.nIDLECount < t.nMaxGoroutines {
		t.nIDLECount += 1
		bCreate = true
	}
	t.locker.Unlock()

	if !bCreate {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		lastIdle := time.Now()

		for {
			select {
			case <-ticker.C:
				if time.Now().Sub(lastIdle) >= t.nMaxIdleTime {
					var bExit bool = false
					t.locker.Lock()
					if t.nIDLECount > t.nMaxIdleGoroutines {
						t.nIDLECount -= 1
						bExit = true
					}
					t.locker.Unlock()

					if bExit {
						return
					}
				}

			case data := <-t.dataCh:
				t.locker.Lock()
				t.nWorkingCount += 1
				t.nIDLECount -= 1
				t.locker.Unlock()
				t.fHandler(data)
				t.locker.Lock()
				t.nWorkingCount -= 1
				t.nIDLECount += 1
				t.pendingItems -= 1
				t.locker.Unlock()
				lastIdle = time.Now()
			}
		}
	}()
}

func (t *WorkerPool) Idle() bool {
	// if t.nWorkingCount == 0 && len(t.dataCh) == 0 {
	// 	return true
	// }

	if t.GetPendingItemCount() == 0 {
		return true
	}

	return false
}

//已经投入队列,未处理完成的任务数量
func (t *WorkerPool) GetPendingItemCount() int {
	var nCount int = 0
	t.locker.Lock()
	nCount = t.pendingItems
	t.locker.Unlock()
	return nCount
}

/*

workpool := workerpool.NewWorkerPool(16)
workpool.SetMaxGoroutine(16)
workpool.SetMaxIdleGoroutine(2)
workpool.SetMaxIdleTime(time.Second * 60)
workpool.SetHandler(func(a interface{}) {

})

workpool.PushItem("hello")

*/
