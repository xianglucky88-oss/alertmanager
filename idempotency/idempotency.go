// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package idempotency implements source-level batch idempotency records for
// alert ingestion. A sending source identifies every batch with a unique
// batch id and a monotonically increasing sequence number. The recorder
// rejects re-submissions of an already accepted batch so they do not drive
// grouping or notifications a second time, and it rejects batches whose
// sequence is older than the newest state known for the source.
//
// Records are kept in memory and durably replicated to an append-only JSON
// Lines event log so that they survive process restarts. Periodic
// maintenance garbage-collects expired records and compacts the log into an
// atomic snapshot.
package idempotency

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
)

// Sentinel errors returned by the recorder. They are always wrapped with
// contextual messages and can be inspected with errors.Is.
var (
	// ErrStaleBatch is returned when a batch sequence is not greater than
	// the latest sequence already accepted for the same source.
	ErrStaleBatch = errors.New("stale batch")
	// ErrIncompleteBatchState is returned when a batch record exists but was
	// never marked completed, typically because a previous process died while
	// ingesting it. Replaying the same batch is unsafe.
	ErrIncompleteBatchState = errors.New("incomplete batch state")
	// ErrIncompleteIdentifier is returned when the batch identifier supplied
	// by the caller is missing parts or cannot be parsed.
	ErrIncompleteIdentifier = errors.New("incomplete batch identifier")
	// ErrInvalidState is returned when the persisted state cannot be decoded.
	ErrInvalidState = errors.New("invalid persisted state")
)

// Outcome describes the result of a Begin call.
type Outcome int

const (
	// OutcomeAccepted means the batch is new and the caller is allowed to
	// ingest its alerts.
	OutcomeAccepted Outcome = iota
	// OutcomeDuplicate means the same batch was already completed and the
	// caller must not ingest anything again.
	OutcomeDuplicate
)

// BatchState is the lifecycle state of a batch record.
type BatchState string

const (
	// StatePending marks a batch whose ingestion has begun but not completed.
	StatePending BatchState = "pending"
	// StateCompleted marks a batch whose ingestion finished successfully.
	StateCompleted BatchState = "completed"
)

// BatchRecord is the in-memory representation of one ingested batch.
type BatchRecord struct {
	Source     string
	BatchID    string
	Sequence   uint64
	State      BatchState
	AlertCount int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ExpiresAt  time.Time
}

// sourceState tracks the high-water mark sequence and the batches known for
// one sending source.
type sourceState struct {
	LastSequence uint64
	Batches      map[string]*BatchRecord
}

// walEvent is one JSON Lines entry in the persistent event log.
type walEvent struct {
	Kind       string    `json:"kind"`
	Source     string    `json:"source"`
	BatchID    string    `json:"batch_id"`
	Sequence   uint64    `json:"sequence"`
	AlertCount int       `json:"alert_count,omitempty"`
	At         time.Time `json:"at"`
}

const (
	eventBegin    = "begin"
	eventComplete = "complete"
)

// Options configures a new Recorder.
type Options struct {
	// SnapshotFile is the path of the durable event log. An empty value
	// disables persistence and keeps the recorder in memory only.
	SnapshotFile string
	// Retention is how long completed and pending records are kept.
	Retention time.Duration
	// Logger is used for operational messages. If nil, a no-op logger is used.
	Logger *slog.Logger
	// Metrics registers the recorder's Prometheus metrics. If nil, metrics
	// are collected but not registered.
	Metrics prometheus.Registerer
}

func (o Options) validate() error {
	if o.Retention <= 0 {
		return errors.New("idempotency: Retention must be positive")
	}
	return nil
}

// Recorder stores batch idempotency records for alert ingestion. All methods
// are goroutine-safe.
type Recorder struct {
	mtx     sync.Mutex
	sources map[string]*sourceState

	snapshotFile string
	retention    time.Duration
	logger       *slog.Logger
	metrics      *metrics

	// f is the append-only event log handle. It is nil when the recorder
	// runs in memory-only mode.
	f *os.File
}

type metrics struct {
	events  *prometheus.CounterVec
	records prometheus.Gauge
}

func newMetrics(r prometheus.Registerer) *metrics {
	m := &metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alertmanager_batch_idempotency_events_total",
			Help: "Total number of idempotent alert batch events by outcome.",
		}, []string{"outcome"}),
		records: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "alertmanager_batch_idempotency_records",
			Help: "Current number of idempotent alert batch records held in memory.",
		}),
	}
	if r != nil {
		r.MustRegister(m.events, m.records)
	}
	return m
}

// New creates a recorder and replays the event log at SnapshotFile when
// persistence is enabled.
func New(o Options) (*Recorder, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	r := &Recorder{
		sources:      map[string]*sourceState{},
		snapshotFile: o.SnapshotFile,
		retention:    o.Retention,
		logger:       promslog.NewNopLogger(),
		metrics:      newMetrics(o.Metrics),
	}
	if o.Logger != nil {
		r.logger = o.Logger
	}

	if o.SnapshotFile == "" {
		return r, nil
	}

	data, err := os.ReadFile(o.SnapshotFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("idempotency: failed to read event log %q: %w", o.SnapshotFile, err)
	}

	goodOffset, err := r.replay(data)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(o.SnapshotFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, fmt.Errorf("idempotency: failed to open event log %q: %w", o.SnapshotFile, err)
	}
	r.f = f

	// A torn trailing line from a crash mid-write is ignored during replay
	// and truncated before new events are appended.
	if goodOffset < int64(len(data)) {
		if err := f.Truncate(goodOffset); err != nil {
			return nil, fmt.Errorf("idempotency: failed to truncate torn event log: %w", err)
		}
		r.logger.Warn("Truncated torn trailing entries from batch idempotency event log", "path", o.SnapshotFile, "offset", goodOffset)
	}

	r.metrics.records.Set(float64(r.recordCount()))
	return r, nil
}

// replay rebuilds the in-memory state from persisted events. It returns the
// byte offset up to which the log contains complete lines. A malformed final
// line is treated as a torn write; malformed lines anywhere else are hard
// corruption and surface as ErrInvalidState.
func (r *Recorder) replay(data []byte) (int64, error) {
	var offset int64
	for len(data) > 0 {
		idx := bytes.IndexByte(data, '\n')
		var line []byte
		if idx < 0 {
			line = data
		} else {
			line = data[:idx]
		}

		if len(bytes.TrimSpace(line)) == 0 {
			if idx < 0 {
				return offset, nil
			}
			offset += int64(idx) + 1
			data = data[idx+1:]
			continue
		}

		var e walEvent
		if err := json.Unmarshal(line, &e); err != nil {
			if idx < 0 {
				// The last line has no terminating newline: assume a torn
				// write and drop everything from the last good offset.
				return offset, nil
			}
			return offset, fmt.Errorf("idempotency: %w at offset %d: %w", ErrInvalidState, offset, err)
		}

		if err := r.apply(e); err != nil {
			return offset, err
		}

		if idx < 0 {
			// A complete event without a trailing newline: keep it.
			return offset + int64(len(line)), nil
		}
		offset += int64(idx) + 1
		data = data[idx+1:]
	}
	return offset, nil
}

// apply folds one replayed event into the in-memory state.
func (r *Recorder) apply(e walEvent) error {
	switch e.Kind {
	case eventBegin:
		ss := r.sources[e.Source]
		if ss == nil {
			ss = &sourceState{Batches: map[string]*BatchRecord{}}
			r.sources[e.Source] = ss
		}
		if _, exists := ss.Batches[e.BatchID]; exists {
			return fmt.Errorf("idempotency: %w: duplicate begin event for source %q batch %q", ErrInvalidState, e.Source, e.BatchID)
		}
		ss.Batches[e.BatchID] = &BatchRecord{
			Source:    e.Source,
			BatchID:   e.BatchID,
			Sequence:  e.Sequence,
			State:     StatePending,
			CreatedAt: e.At,
			UpdatedAt: e.At,
			ExpiresAt: e.At.Add(r.retention),
		}
		if e.Sequence > ss.LastSequence {
			ss.LastSequence = e.Sequence
		}
	case eventComplete:
		ss := r.sources[e.Source]
		if ss == nil {
			return nil
		}
		if rec, ok := ss.Batches[e.BatchID]; ok {
			rec.State = StateCompleted
			rec.AlertCount = e.AlertCount
			rec.UpdatedAt = e.At
		}
	default:
		return fmt.Errorf("idempotency: %w: unknown event kind %q", ErrInvalidState, e.Kind)
	}
	return nil
}

// Begin registers a new batch for the given source. It returns
// OutcomeDuplicate when the batch was already completed, ErrStaleBatch when
// the sequence is not newer than the source's high-water mark and
// ErrIncompleteBatchState when an unfinished record for the batch exists.
func (r *Recorder) Begin(ctx context.Context, source, batchID string, sequence uint64) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if source == "" || batchID == "" {
		return 0, fmt.Errorf("idempotency: %w: source id and batch id must not be empty", ErrIncompleteIdentifier)
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	now := time.Now()
	ss := r.sources[source]
	if ss != nil {
		if rec, ok := ss.Batches[batchID]; ok {
			switch rec.State {
			case StateCompleted:
				r.metrics.events.WithLabelValues("duplicate").Inc()
				return OutcomeDuplicate, nil
			default:
				r.metrics.events.WithLabelValues("incomplete").Inc()
				return 0, fmt.Errorf(
					"idempotency: %w: batch %q for source %q was never completed; resubmit under a new batch id",
					ErrIncompleteBatchState, batchID, source,
				)
			}
		}
		if sequence <= ss.LastSequence {
			r.metrics.events.WithLabelValues("stale").Inc()
			return 0, fmt.Errorf(
				"idempotency: %w: sequence %d is not greater than the latest sequence %d for source %q",
				ErrStaleBatch, sequence, ss.LastSequence, source,
			)
		}
	}

	// Persist before mutating memory: a failed write must leave no
	// in-memory trace so the request can be safely retried.
	if err := r.append(walEvent{
		Kind:     eventBegin,
		Source:   source,
		BatchID:  batchID,
		Sequence: sequence,
		At:       now,
	}); err != nil {
		r.metrics.events.WithLabelValues("persist_failed").Inc()
		return 0, fmt.Errorf("idempotency: failed to persist batch state for source %q batch %q: %w", source, batchID, err)
	}

	if ss == nil {
		ss = &sourceState{Batches: map[string]*BatchRecord{}}
		r.sources[source] = ss
	}
	ss.Batches[batchID] = &BatchRecord{
		Source:    source,
		BatchID:   batchID,
		Sequence:  sequence,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(r.retention),
	}
	ss.LastSequence = sequence

	r.metrics.events.WithLabelValues("accepted").Inc()
	r.metrics.records.Set(float64(r.recordCount()))
	return OutcomeAccepted, nil
}

// Complete marks a previously begun batch as completed and durably records
// the transition.
func (r *Recorder) Complete(ctx context.Context, source, batchID string, alertCount int) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	ss := r.sources[source]
	if ss == nil {
		return fmt.Errorf("idempotency: %w: unknown source %q", ErrIncompleteBatchState, source)
	}
	rec, ok := ss.Batches[batchID]
	if !ok {
		return fmt.Errorf("idempotency: %w: unknown batch %q for source %q", ErrIncompleteBatchState, batchID, source)
	}

	now := time.Now()
	if err := r.append(walEvent{
		Kind:       eventComplete,
		Source:     source,
		BatchID:    batchID,
		Sequence:   rec.Sequence,
		AlertCount: alertCount,
		At:         now,
	}); err != nil {
		r.metrics.events.WithLabelValues("persist_failed").Inc()
		return fmt.Errorf("idempotency: failed to persist batch state for source %q batch %q: %w", source, batchID, err)
	}

	rec.State = StateCompleted
	rec.AlertCount = alertCount
	rec.UpdatedAt = now
	return nil
}

// append writes one event to the log and fsyncs it. In memory-only mode it
// is a no-op.
func (r *Recorder) append(e walEvent) error {
	if r.f == nil {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := r.f.Write(b); err != nil {
		return err
	}
	return r.f.Sync()
}

// Maintenance garbage-collects expired records on every tick and compacts
// the event log. It runs a final cycle when stopc is closed, then releases
// the log handle, so callers only need to wait on their wait group.
func (r *Recorder) Maintenance(interval time.Duration, stopc <-chan struct{}) {
	if interval <= 0 || stopc == nil {
		r.logger.Error("interval or stop signal are missing - not running batch idempotency maintenance")
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-stopc:
			if err := r.gcAndCompact(); err != nil {
				r.logger.Error("Running shutdown compaction failed", "err", err)
			}
			_ = r.Close()
			return
		case <-t.C:
			if err := r.gcAndCompact(); err != nil {
				r.logger.Error("Running batch idempotency maintenance failed", "err", err)
			}
		}
	}
}

// gcAndCompact removes expired records and rewrites the event log with the
// remaining live state.
func (r *Recorder) gcAndCompact() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	now := time.Now()
	var buf bytes.Buffer
	for source, ss := range r.sources {
		for batchID, rec := range ss.Batches {
			if !rec.ExpiresAt.After(now) {
				delete(ss.Batches, batchID)
				continue
			}
			if err := writeEvent(&buf, walEvent{
				Kind:     eventBegin,
				Source:   rec.Source,
				BatchID:  rec.BatchID,
				Sequence: rec.Sequence,
				At:       rec.CreatedAt,
			}); err != nil {
				return err
			}
			if rec.State == StateCompleted {
				if err := writeEvent(&buf, walEvent{
					Kind:       eventComplete,
					Source:     rec.Source,
					BatchID:    rec.BatchID,
					Sequence:   rec.Sequence,
					AlertCount: rec.AlertCount,
					At:         rec.UpdatedAt,
				}); err != nil {
					return err
				}
			}
		}
		if len(ss.Batches) == 0 {
			delete(r.sources, source)
		}
	}
	r.metrics.records.Set(float64(r.recordCount()))

	if r.f == nil {
		return nil
	}

	tmpFilename := fmt.Sprintf("%s.%x", r.snapshotFile, rand.Uint64())
	tmp, err := os.Create(tmpFilename)
	if err != nil {
		return err
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpFilename)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpFilename)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpFilename)
		return err
	}
	if err := os.Rename(tmpFilename, r.snapshotFile); err != nil {
		_ = os.Remove(tmpFilename)
		return err
	}

	reopened, err := os.OpenFile(r.snapshotFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return err
	}
	old := r.f
	r.f = reopened
	return old.Close()
}

func writeEvent(buf *bytes.Buffer, e walEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = buf.Write(b)
	return err
}

// recordCount returns the total number of batch records across sources. The
// caller must hold r.mtx.
func (r *Recorder) recordCount() int {
	n := 0
	for _, ss := range r.sources {
		n += len(ss.Batches)
	}
	return n
}

// Close releases the persistent log handle. In memory-only mode it is a
// no-op.
func (r *Recorder) Close() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}
