package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/groupcache/lru"
	rng "github.com/leesper/go_rng"
)

type Request struct {
	Item      int64
	Sent      int64
	Accepted  int64
	Completed int64
	Cached    bool
}

type Slot struct {
	Full bool
	Request
}

type Server struct {
	Queue     []Request
	Slots     []Slot
	SlotsUsed int
	Cache     *lru.Cache
}

const numServers = 8
const numSlots = 8
const requestsPerTick = 0.0105 * numServers * numSlots
const itemParameter = 10000
const cachedHandleTime = 10
const uncachedHandleTime = 100
const requestLimit = 10000000
const cacheSize = 1.8 * itemParameter / numServers
const maxQueue = 50

var tm int64
var servers []*Server
var poisson *rng.PoissonGenerator

func init() {
	servers = make([]*Server, numServers)
	for i, _ := range servers {
		servers[i] = &Server{
			Queue: make([]Request, 0, 10),
			Slots: make([]Slot, numSlots),
			Cache: lru.New(cacheSize),
		}
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	poisson = rng.NewPoissonGenerator(seed)
}

func GenerateRequests() (ret []Request) {
	numRequests := poisson.Possion(requestsPerTick)
	for i := int64(0); i < numRequests; i++ {
		item := int64(rand.ExpFloat64() * itemParameter)
		r := Request{
			Item: item,
			Sent: tm,
		}
		ret = append(ret, r)
	}
	return ret
}

func ChooseServer(r Request) int {
	//	return int(r.Item % numServers)
	return int(rand.Int31n(numServers))
}

func (s *Server) AddRequest(r Request) {
	if len(s.Queue) < maxQueue {
		s.Queue = append(s.Queue, r)
	}
}

func (s *Server) FindSlot() int {
	for i, sl := range s.Slots {
		if !sl.Full {
			return i
		}
	}
	panic(fmt.Errorf("All slots used"))
}

func (s *Server) HandleRequest(r Request) {
	i := s.FindSlot()
	s.Slots[i].Full = true
	r.Accepted = tm
	_, cached := s.Cache.Get(r.Item)
	if cached {
		r.Completed = tm + cachedHandleTime
		r.Cached = true
	} else {
		r.Completed = tm + uncachedHandleTime
	}
	s.Slots[i].Request = r
	s.SlotsUsed++
	//	fmt.Printf("Enqueued request for %d in slot %d at time %d\n", r.Item, i, tm)
}

func (s *Server) CompleteRequest(i int) Request {
	r := s.Slots[i].Request
	s.Slots[i].Full = false
	s.SlotsUsed--
	s.Cache.Add(r.Item, true)
	//	fmt.Printf("Completed request for %d in slot %d at time %d\n", r.Item, i, tm)
	return r
}

func (s *Server) Outstanding() int {
	return len(s.Queue) + s.SlotsUsed
}

func (s *Server) Tick() (ret []Request) {
	for s.SlotsUsed < numSlots && len(s.Queue) > 0 {
		s.HandleRequest(s.Queue[0])
		s.Queue = s.Queue[1:]
	}

	ret = []Request{}

	for i, sl := range s.Slots {
		if sl.Full && sl.Completed <= tm {
			ret = append(ret, s.CompleteRequest(i))
		}
	}
	return
}

var totalRequests float64
var acceptedRequests float64
var totalCached float64
var totalQueued float64
var totalTime float64

func RecordStats(r Request) {
	acceptedRequests += 1
	totalQueued += float64(r.Accepted - r.Sent)
	totalTime += float64(r.Completed - r.Sent)
	if r.Cached {
		totalCached += 1
	}
}

func Tick() {
	tm = tm + 1
	if tm <= requestLimit {
		requests := GenerateRequests()
		for _, request := range requests {
			totalRequests += 1
			i := ChooseServer(request)
			servers[i].AddRequest(request)
		}
	}
	for _, server := range servers {
		done := server.Tick()
		for _, r := range done {
			RecordStats(r)
		}
	}
}

func main() {
	var prevCompletion int

	for {
		Tick()
		var total int
		for _, s := range servers {
			total += s.Outstanding()
		}
		finish := total == 0 && tm >= requestLimit
		completion := int(10 * tm / requestLimit)

		if completion != prevCompletion || finish {
			prevCompletion = completion
			fmt.Printf("tm=%d", tm)

			if acceptedRequests > 0 {
				fmt.Printf(" Requests: %.0f, Accepted %.0f (%.2f%%), Cache Hit %.2f%%, Avg Q: %.2f, Avg Tm: %.2f",
					totalRequests,
					acceptedRequests,
					100*acceptedRequests/totalRequests,
					100*totalCached/acceptedRequests,
					totalQueued/acceptedRequests,
					totalTime/acceptedRequests,
				)
			}
			fmt.Println("")
		}
		if finish {
			break
		}
	}
}
