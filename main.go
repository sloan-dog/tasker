package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	// we'll have 15 workers
	numWorkers := 15
	doneChan := make(chan *Worker, numWorkers)
	numRequests := 10

	// create a balancer
	b := Balancer{
		pool: make([]*Worker, 0, numWorkers),
		done: doneChan,
	}

	// generate the workers
	for i := 0; i < numWorkers; i++ {
		w := &Worker{
			requests: make(chan Request, 50),
			pending:  0,
			index:    i,
		}
		b.pool = append(b.pool, w)
		// go to work
		go w.work(doneChan)
	}

	workChan := make(chan Request)
	go requester(workChan, numRequests)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		b.balance(workChan, numRequests)
		wg.Done()
	}()
	wg.Wait()
}

type Request struct {
	fn func() int // operation
	c  chan int   // return the result
}

func requester(work chan<- Request, numRequests int) {
	c := make(chan int) // unbuferred
	for i := 0; i < numRequests; i++ {
		// Kill some time (fake load)
		time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
		work <- Request{workFn, c}
		result := <-c
		furtherProcess(result)
	}
}

func furtherProcess(result int) {
	fmt.Printf("The result of the work is: %v\n", result)
}

func workFn() int {
	time.Sleep(time.Duration(rand.Int31n(50)) * 2 * time.Millisecond)
	return fib(rand.Intn(6))
}

func fib(i int) int {
	if i == 0 {
		return i
	}
	if i == 1 {
		return i
	}
	if i == 2 {
		return i
	}
	return fib(i-1) + fib(i-2)
}

type Worker struct {
	requests chan Request // work to do (buffered chan)
	pending  int
	index    int
}

func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests // get req from balancer
		req.c <- req.fn()   // call fn and send result
		done <- w           // free the worker were done
	}
}

type Pool []*Worker

type Balancer struct {
	pool Pool
	done chan *Worker
}

func (b *Balancer) balance(work chan Request, requestCount int) {
	processedCount := 0
	// balance until we handle all requests
	for processedCount != requestCount {
		select {
		case req := <-work: // received a request
			b.dispatch(req) // send request to worker
		case w := <-b.done: // worker has finished
			b.completed(w)   // update its info
			processedCount++ // update count
		}
	}
}

// Less method of heap.Interface
func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

// Len method of heap.Interface
func (p Pool) Len() int {
	return len(p)
}

// Pop method of heap.Interface
func (p Pool) Pop() interface{} {
	el := p[p.Len()-1]
	p = p[:p.Len()-2]
	return el
}

// Push method of heap.Interface
func (p Pool) Push(el interface{}) {
	element, _ := el.(*Worker)
	p = append(p, element)
}

// Swap method of heap.Interface
func (p Pool) Swap(i, j int) {
	el := p[i]
	p[i] = p[j]
	p[j] = el
}

// ship a request to a worker
func (b *Balancer) dispatch(req Request) {
	// Grab the least loaded work ...
	w := heap.Pop(&b.pool).(*Worker)
	// send it the task
	w.requests <- req
	// One more in its work queue
	w.pending++
	// Put it into its place on the heap
	heap.Push(&b.pool, w)
}

// free up a worker's load
func (b *Balancer) completed(w *Worker) {
	// One fewer in queue
	w.pending--
	// Remove it from heap
	heap.Remove(&b.pool, w.index)
	// Put it into its place on the heap
	heap.Push(&b.pool, w)
}
