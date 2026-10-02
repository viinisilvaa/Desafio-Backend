package app

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerGroupCancelsAndWaitsForAllWorkers(t *testing.T) {
	var stopped atomic.Int32
	worker := func(ctx context.Context) {
		<-ctx.Done()
		stopped.Add(1)
	}
	group := NewWorkerGroup(worker, worker, worker)
	if err := group.Start(); err != nil {
		t.Fatal(err)
	}
	if err := group.Start(); !errors.Is(err, ErrWorkersAlreadyStarted) {
		t.Fatalf("second start error=%v, want ErrWorkersAlreadyStarted", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got := stopped.Load(); got != 3 {
		t.Fatalf("stopped workers=%d, want 3", got)
	}
	if err := group.Stop(ctx); err != nil {
		t.Fatalf("repeated stop should be safe: %v", err)
	}
}

func TestWorkerGroupReportsShutdownDeadlineAndCanFinishLater(t *testing.T) {
	release := make(chan struct{})
	group := NewWorkerGroup(func(context.Context) { <-release })
	if err := group.Start(); err != nil {
		t.Fatal(err)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := group.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop error=%v, want context.Canceled", err)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Stop(ctx); err != nil {
		t.Fatalf("worker group did not finish after release: %v", err)
	}
}
