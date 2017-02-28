package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()

			/* The for loop is to avoid workers failure
			   If the call function returns success, the task is successfully taken by this worker. We just
			   break the for loop and go to next task.
			   If it returns failure or the reply is lost or RPC timeout, we just fetch a new worker from
			   channel and assign current task to the new worker.
			*/
			for {
				worker := <- registerChan
				var args DoTaskArgs

				args.JobName = jobName
				args.File = mapFiles[taskNum]
				args.Phase = phase
				args.TaskNumber = taskNum
				args.NumOtherPhase = n_other
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						// This server finished the task, and push it into register channel again to take other tasks
						registerChan <- worker
					}()
					break
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
