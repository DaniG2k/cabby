package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/anycable/anycable-go/utils"
)

// utilsGID is the upstream test helper getGID (utils/gopool_test.go): the current
// goroutine ID parsed from runtime.Stack.
func utilsGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// utilsWorkerRespawnThreshold copies the value of the unexported
// utils.workerRespawnThreshold (1 << 16) that the upstream test uses.
const utilsWorkerRespawnThreshold = 1 << 16

// Pointer queues for TestQueue_vs_GC: items hold *map[string]string payloads with
// finalizers recording collection, as in the upstream test.
var (
	utilsPtrQueues   = map[int]*utils.Queue[*map[string]string]{}
	utilsFinalizedMu sync.Mutex
	utilsFinalized   = map[string]bool{}
)

func utilsIsFinalized(id string) bool {
	utilsFinalizedMu.Lock()
	defer utilsFinalizedMu.Unlock()
	return utilsFinalized[id]
}

func utilsPtrQueue(id int) (*utils.Queue[*map[string]string], error) {
	if q, ok := utilsPtrQueues[id]; ok {
		return q, nil
	}
	return nil, fmt.Errorf("unknown pointer queue handle %d", id)
}

// utilsAddTracked allocates the payload in its own frame so that nothing in the
// bridge keeps a reference to it besides the queue.
//
//go:noinline
func utilsAddTracked(q *utils.Queue[*map[string]string], id string) bool {
	payload := map[string]string{"foo": id}
	item := utils.Item[*map[string]string]{Size: 1, Data: &payload}
	runtime.SetFinalizer(&payload, func(*map[string]string) {
		utilsFinalizedMu.Lock()
		defer utilsFinalizedMu.Unlock()
		utilsFinalized[id] = true
	})
	return q.Add(item)
}

//go:noinline
func utilsRemoveDiscard(q *utils.Queue[*map[string]string]) bool {
	_, ok := q.Remove()
	return ok
}

func init() {
	// The upstream TestWorkerRespawn schedule on NewGoPool("test", 1): a GID probe,
	// workerRespawnThreshold+2 data tasks, a GID probe. Returns both GIDs and
	// len(dataChan) once the second probe has run.
	register("utils.GoPool.WorkerRespawn", func(json.RawMessage) (interface{}, error) {
		pool := utils.NewGoPool("test", 1)

		var wg sync.WaitGroup
		resChan := make(chan uint64, 2)
		dataChan := make(chan struct{}, utilsWorkerRespawnThreshold+2)

		wg.Add(1)

		pool.Schedule(func() {
			resChan <- utilsGID()
		})

		for i := 0; i < utilsWorkerRespawnThreshold+2; i++ {
			pool.Schedule(func() {
				dataChan <- struct{}{}
			})
		}

		pool.Schedule(func() {
			resChan <- utilsGID()
			wg.Done()
		})

		initial := <-resChan
		current := <-resChan

		return map[string]interface{}{"Initial": initial, "Current": current, "DataLen": len(dataChan)}, nil
	})

	register("utils.NewPtrQueue", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ InitialCapacity int }](raw)
		if err != nil {
			return nil, err
		}
		id := utilsHandle()
		utilsPtrQueues[id] = utils.NewQueue[*map[string]string](a.InitialCapacity)
		return id, nil
	})
	register("utils.PtrQueue.AddTracked", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Q  int
			ID string
		}](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPtrQueue(a.Q)
		if err != nil {
			return nil, err
		}
		return utilsAddTracked(q, fmt.Sprintf("%d/%s", a.Q, a.ID)), nil
	})
	register("utils.PtrQueue.Remove", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPtrQueue(a.Q)
		if err != nil {
			return nil, err
		}
		return utilsRemoveDiscard(q), nil
	})
	register("utils.PtrQueue.Len", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPtrQueue(a.Q)
		if err != nil {
			return nil, err
		}
		return q.Len(), nil
	})
	register("utils.RuntimeGC", func(json.RawMessage) (interface{}, error) {
		runtime.GC()
		return nil, nil
	})
	// Whether the payload tracked as ID was finalized. With Eventually, polls like
	// upstream's require.Eventually(..., 2*time.Second, 100*time.Millisecond).
	register("utils.PtrQueue.Finalized", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Q          int
			ID         string
			Eventually bool
		}](raw)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%d/%s", a.Q, a.ID)
		if !a.Eventually {
			return utilsIsFinalized(key), nil
		}
		deadline := time.After(2 * time.Second)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-deadline:
				return false, nil
			case <-tick.C:
				if utilsIsFinalized(key) {
					return true, nil
				}
			}
		}
	})
}
