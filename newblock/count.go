package main

import(
    "container/heap"
    "time"
)

type Count struct{
    Block
    queryrule chan chan interface{}
    inrule  chan interface{}
    inpoll    chan interface{}
    in      chan interface{}
    out     chan interface{}
}

func (b *Count) Setup(){
    b.Kind = "Count"
    b.in = b.InRoute("in")
    b.inrule = b.InRoute("rule")
    b.queryrule = b.QueryRoute("rule")
    b.out = b.Broadcast()
}

func (b *Count) Run(){
    waitTimer := time.NewTimer(100 * time.Millisecond)   
    pq := &PriorityQueue{}
    heap.Init(pq)
    window := time.Duration(0)

    for{
        select{
        case <-waitTimer.C:
        case rule := <- b.inrule:
            window, _ = time.ParseDuration(rule.(map[string]interface{})["Window"].(string))
        case <- b.in:
            empty := make([]byte, 0)
            queueMessage := &PQMessage{
                val: &empty,
                t:   time.Now(),
            }
            heap.Push(pq, queueMessage)
        case <- b.inpoll:
            b.out <- map[string]interface{}{
                "Count": len(*pq),
            }
        case c := <- b.queryrule:
            c <- map[string]interface{}{
                "Window": window,
            }
        }
        for {
            pqMsg, diff := pq.PeekAndShift(time.Now(), window)
            if pqMsg == nil {
                // either the queue is empty, or it"s not time to emit
                if diff == 0 {
                    // then the queue is empty. Pause for 5 seconds before checking again
                    diff = time.Duration(500) * time.Millisecond
                }
                waitTimer.Reset(diff)
                break
            }
        }
    }
}
