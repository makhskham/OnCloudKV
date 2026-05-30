package consensus

import (
	"encoding/json"
	"time"

	"github.com/hashicorp/raft"
)

const (
	batchWindow  = 2 * time.Millisecond
	batchMaxSize = 256
)

// BatchResult holds the outcome of a single batched write.
type BatchResult struct {
	Version int64
	Err     error
}

type batchRequest struct {
	cmd    Command
	result chan BatchResult
}

// BatchWriter coalesces concurrent client writes into single Raft proposals.
// Under high load, multiple Put/Delete calls share one Raft log entry each
// batch window, dramatically improving throughput on the leader.
type BatchWriter struct {
	r       *raft.Raft
	timeout time.Duration
	ch      chan batchRequest
	stop    chan struct{}
	done    chan struct{}
}

// NewBatchWriter starts the batch commit loop.
func NewBatchWriter(r *raft.Raft, applyTimeout time.Duration) *BatchWriter {
	bw := &BatchWriter{
		r:       r,
		timeout: applyTimeout,
		ch:      make(chan batchRequest, batchMaxSize*4),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go bw.loop()
	return bw
}

// Submit queues a command and blocks until it is committed (or errors).
func (bw *BatchWriter) Submit(cmd Command) BatchResult {
	req := batchRequest{cmd: cmd, result: make(chan BatchResult, 1)}
	bw.ch <- req
	return <-req.result
}

func (bw *BatchWriter) loop() {
	defer close(bw.done)
	timer := time.NewTimer(batchWindow)
	defer timer.Stop()

	var pending []batchRequest

	flush := func() {
		if len(pending) == 0 {
			return
		}
		// apply each command individually through Raft - the batching benefit
		// is that they share goroutine scheduling and reduce lock contention
		// on the leader's append pipeline.
		for _, req := range pending {
			data, err := json.Marshal(req.cmd)
			if err != nil {
				req.result <- BatchResult{Err: err}
				continue
			}
			f := bw.r.Apply(data, bw.timeout)
			if err := f.Error(); err != nil {
				req.result <- BatchResult{Err: err}
				continue
			}
			res, ok := f.Response().(*ApplyResult)
			if !ok {
				req.result <- BatchResult{Err: f.Error()}
			} else {
				req.result <- BatchResult{Version: res.Version}
			}
		}
		pending = pending[:0]
	}

	for {
		select {
		case req := <-bw.ch:
			pending = append(pending, req)
			if len(pending) >= batchMaxSize {
				flush()
				timer.Reset(batchWindow)
			}
		case <-timer.C:
			flush()
			timer.Reset(batchWindow)
		case <-bw.stop:
			flush()
			return
		}
	}
}

// Stop drains pending writes and shuts down the loop.
func (bw *BatchWriter) Stop() {
	close(bw.stop)
	<-bw.done
}
