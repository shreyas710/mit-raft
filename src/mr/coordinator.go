package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const success = math.MaxInt32

type Coordinator struct {
	// Your definitions here.
	tasks   chan Work
	mu      sync.Mutex
	terms   []int
	wg      sync.WaitGroup
	nMap    int
	nReduce int
	done    bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CallGetWork(args *WorkArgs, reply *WorkReply) error {
	if len(c.tasks) == 0 {
		reply.HasWork = false
		return nil
	}

	reply.Work = <-c.tasks
	c.mu.Lock()
	reply.Term = c.terms[reply.Work.Fileindex]
	c.mu.Unlock()
	reply.HasWork = true

	go func() {
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.terms[reply.Work.Fileindex] == success {
			return
		}
		c.terms[reply.Work.Fileindex]++
		c.tasks <- reply.Work
	}()

	return nil
}

func (c *Coordinator) CallReportWork(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.terms[args.Work.Fileindex] != args.Term {
		reply.Success = false
		return nil
	}
	c.terms[args.Work.Fileindex] = success
	reply.Success = true
	c.wg.Done()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func StartReduceWork(c *Coordinator) {
	c.wg.Wait()
	c.terms = make([]int, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks <- Work{
			WorkType:  REDUCE,
			Fileindex: i,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		c.wg.Add(1)
	}
	go WorkDone(c)
}

func WorkDone(c *Coordinator) {
	c.wg.Wait()
	c.mu.Lock()
	c.done = true
	c.mu.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	var buflen int
	if len(files) > nReduce {
		buflen = len(files)
	} else {
		buflen = nReduce
	}

	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,
		wg:      sync.WaitGroup{},
		tasks:   make(chan Work, buflen),
		terms:   make([]int, len(files)),
		done:    false,
	}

	for idx, file := range files {
		c.tasks <- Work{
			WorkType:  MAP,
			Filename:  file,
			Fileindex: idx,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		c.wg.Add(1)
	}

	go StartReduceWork(&c)

	c.server(sockname)
	return &c
}
