package app

import (
	"context"
	"errors"
	"sync"
)

var ErrWorkersAlreadyStarted = errors.New("worker group already started")

type WorkerFunc func(context.Context)

type WorkerGroup struct {
	workers []WorkerFunc
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

func NewWorkerGroup(workers ...WorkerFunc) *WorkerGroup {
	return &WorkerGroup{workers: append([]WorkerFunc(nil), workers...)}
}

func (g *WorkerGroup) Start() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		return ErrWorkersAlreadyStarted
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.done = make(chan struct{})
	g.started = true
	var group sync.WaitGroup
	group.Add(len(g.workers))
	for _, worker := range g.workers {
		go func(worker WorkerFunc) {
			defer group.Done()
			worker(ctx)
		}(worker)
	}
	go func() {
		group.Wait()
		close(g.done)
	}()
	return nil
}

func (g *WorkerGroup) Stop(ctx context.Context) error {
	g.mu.Lock()
	if !g.started {
		g.mu.Unlock()
		return nil
	}
	cancel := g.cancel
	done := g.done
	g.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
