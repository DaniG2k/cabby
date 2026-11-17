package main

import (
	"encoding/json"
	"fmt"

	"github.com/anycable/anycable-go/utils"
)

// Stateful handles for utils objects. Requests are handled one at a time on the
// main goroutine, so plain maps suffice. Item pointers keep their identity through
// utilsPQItems (upstream tests compare items with assert.Same).
type pqItem = utils.PriorityQueueItem[interface{}, int]

var (
	utilsNextHandle = 0
	utilsPQs        = map[int]*utils.PriorityQueue[interface{}, int]{}
	utilsPQItems    = map[int]*pqItem{}
	utilsPQItemIDs  = map[*pqItem]int{}
	utilsQueues     = map[int]*utils.Queue[[]byte]{}
)

func utilsHandle() int {
	utilsNextHandle++
	return utilsNextHandle
}

func utilsItemRef(item *pqItem) interface{} {
	if item == nil {
		return nil
	}
	id, ok := utilsPQItemIDs[item]
	if !ok {
		id = utilsHandle()
		utilsPQItemIDs[item] = id
		utilsPQItems[id] = item
	}
	return map[string]interface{}{"Item": id, "Value": item.Value(), "Priority": item.Priority()}
}

func utilsPQ(id int) (*utils.PriorityQueue[interface{}, int], error) {
	if q, ok := utilsPQs[id]; ok {
		return q, nil
	}
	return nil, fmt.Errorf("unknown priority queue handle %d", id)
}

func utilsQueue(id int) (*utils.Queue[[]byte], error) {
	if q, ok := utilsQueues[id]; ok {
		return q, nil
	}
	return nil, fmt.Errorf("unknown queue handle %d", id)
}

type queueItemArgs struct {
	Q    int
	Data string
	Size uint64
}

func (a queueItemArgs) item() utils.Item[[]byte] {
	return utils.Item[[]byte]{Data: []byte(a.Data), Size: a.Size}
}

func registerQueueOp(name string, f func(q *utils.Queue[[]byte], a queueItemArgs, raw json.RawMessage) (interface{}, error)) {
	register(name, func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[queueItemArgs](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsQueue(a.Q)
		if err != nil {
			return nil, err
		}
		return f(q, a, raw)
	})
}

func init() {
	// MessageVerifier: stateless, a verifier is built from the key on every call.
	register("utils.MessageVerifier.Generate", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Key     string
			Payload interface{}
		}](raw)
		if err != nil {
			return nil, err
		}
		res, gerr := utils.NewMessageVerifier(a.Key).Generate(a.Payload)
		return parsed{res, errString(gerr)}, nil
	})
	register("utils.MessageVerifier.Verified", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Key, Msg string }](raw)
		if err != nil {
			return nil, err
		}
		res, verr := utils.NewMessageVerifier(a.Key).Verified(a.Msg)
		return parsed{res, errString(verr)}, nil
	})
	register("utils.MessageVerifier.Validate", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Key, Msg string }](raw)
		if err != nil {
			return nil, err
		}
		return parsed{nil, errString(utils.NewMessageVerifier(a.Key).Validate(a.Msg))}, nil
	})

	// PriorityQueue[any, int] handles.
	register("utils.NewPriorityQueue", func(json.RawMessage) (interface{}, error) {
		id := utilsHandle()
		utilsPQs[id] = utils.NewPriorityQueue[interface{}, int]()
		return id, nil
	})
	register("utils.PriorityQueue.PushItem", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct {
			Q        int
			Value    interface{}
			Priority int
		}](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		return utilsItemRef(q.PushItem(a.Value, a.Priority)), nil
	})
	register("utils.PriorityQueue.PopItem", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		return utilsItemRef(q.PopItem()), nil
	})
	register("utils.PriorityQueue.Peek", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		return utilsItemRef(q.Peek()), nil
	})
	register("utils.PriorityQueue.Update", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q, Item, Priority int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		q.Update(utilsPQItems[a.Item], a.Priority)
		return nil, nil
	})
	register("utils.PriorityQueue.Remove", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q, Item int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		q.Remove(utilsPQItems[a.Item])
		return nil, nil
	})
	register("utils.PriorityQueue.Len", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ Q int }](raw)
		if err != nil {
			return nil, err
		}
		q, err := utilsPQ(a.Q)
		if err != nil {
			return nil, err
		}
		return len(*q), nil
	})

	// Queue[[]byte] handles.
	register("utils.NewQueue", func(raw json.RawMessage) (interface{}, error) {
		a, err := bridgeArgs[struct{ InitialCapacity int }](raw)
		if err != nil {
			return nil, err
		}
		id := utilsHandle()
		utilsQueues[id] = utils.NewQueue[[]byte](a.InitialCapacity)
		return id, nil
	})
	registerQueueOp("utils.Queue.Add", func(q *utils.Queue[[]byte], a queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Add(a.item()), nil
	})
	// Adds from a new goroutine without waiting for it, like the upstream test does
	// while the caller blocks in Wait.
	registerQueueOp("utils.Queue.AddAsync", func(q *utils.Queue[[]byte], a queueItemArgs, _ json.RawMessage) (interface{}, error) {
		item := a.item()
		go func() { q.Add(item) }()
		return nil, nil
	})
	registerQueueOp("utils.Queue.Push", func(q *utils.Queue[[]byte], a queueItemArgs, raw json.RawMessage) (interface{}, error) {
		m, err := bridgeArgs[struct{ MaxSize uint64 }](raw)
		if err != nil {
			return nil, err
		}
		return int(q.Push(a.item(), m.MaxSize)), nil
	})
	registerQueueOp("utils.Queue.Close", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		q.Close()
		return nil, nil
	})
	registerQueueOp("utils.Queue.Closed", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Closed(), nil
	})
	registerQueueOp("utils.Queue.Clear", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		q.Clear()
		return nil, nil
	})
	registerQueueOp("utils.Queue.Wait", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Wait(), nil
	})
	registerQueueOp("utils.Queue.Remove", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		item, ok := q.Remove()
		return map[string]interface{}{"Data": string(item.Data), "Size": item.Size, "Ok": ok}, nil
	})
	registerQueueOp("utils.Queue.Cap", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Cap(), nil
	})
	registerQueueOp("utils.Queue.Len", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Len(), nil
	})
	registerQueueOp("utils.Queue.Size", func(q *utils.Queue[[]byte], _ queueItemArgs, _ json.RawMessage) (interface{}, error) {
		return q.Size(), nil
	})
}
