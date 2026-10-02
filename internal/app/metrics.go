package app

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	processed       atomic.Uint64
	rejected        atomic.Uint64
	failed          atomic.Uint64
	pending         atomic.Uint64
	duplicates      atomic.Uint64
	retries         atomic.Uint64
	dlq             atomic.Uint64
	conflicts       atomic.Uint64
	processingCount atomic.Uint64
	processingNanos atomic.Uint64
	reconciliation  atomic.Uint64
	outboxLagMillis atomic.Int64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w,
		"# TYPE wager_transactions_total counter\n"+
			"wager_transactions_total{status=\"processed\"} %d\n"+
			"wager_transactions_total{status=\"rejected\"} %d\n"+
			"wager_transactions_total{status=\"failed\"} %d\n"+
			"wager_transactions_total{status=\"pending_reference\"} %d\n"+
			"wager_idempotent_replays_total %d\n"+
			"wager_retries_total %d\n"+
			"wager_dlq_total %d\n"+
			"wager_concurrency_conflicts_total %d\n"+
			"wager_processing_observations_total %d\n"+
			"wager_processing_seconds_sum %.9f\n"+
			"wager_reconciliation_divergences_total %d\n"+
			"wager_outbox_oldest_pending_milliseconds %d\n",
		m.processed.Load(), m.rejected.Load(), m.failed.Load(), m.pending.Load(), m.duplicates.Load(), m.retries.Load(),
		m.dlq.Load(), m.conflicts.Load(), m.processingCount.Load(), float64(m.processingNanos.Load())/1e9,
		m.reconciliation.Load(), m.outboxLagMillis.Load())
}

func (m *Metrics) recordDeadLetter(disposition string) {
	m.dlq.Add(1)
	if disposition == "FAILED" {
		m.failed.Add(1)
	}
}

func (m *Metrics) recordProcess(result Result, err error, elapsedNanos uint64) {
	m.processingCount.Add(1)
	m.processingNanos.Add(elapsedNanos)
	if errorsIsIdempotencyConflict(err) {
		m.conflicts.Add(1)
	}
	if err != nil {
		return
	}
	if result.IdempotentReplay {
		m.duplicates.Add(1)
	}
	switch result.Status {
	case "PROCESSED":
		m.processed.Add(1)
	case "REJECTED":
		m.rejected.Add(1)
	case "PENDING", "PENDING_REFERENCE":
		m.pending.Add(1)
	}
}

func errorsIsIdempotencyConflict(err error) bool {
	return err == ErrIdempotencyConflict || err == ErrMessageConflict
}
