package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkStatus int

const (
	IDLE WorkStatus = iota
	START
	FINISH
)

type WorkType int

const (
	MAP WorkType = iota
	REDUCE
)

type Work struct {
	WorkType  WorkType
	Filename  string
	Fileindex int
	NMapWork  int
	NReduce   int
}

type WorkArgs struct {
	WorkerId int
}

type WorkReply struct {
	HasWork bool
	Work    Work
	Term    int
}

type ReportArgs struct {
	Work Work
	Term int
}

type ReportReply struct {
	Success bool
}
