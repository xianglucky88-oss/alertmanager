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

package idempotency

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"
)

func newTestRecorder(t *testing.T, retention time.Duration) (*Recorder, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "idempotency")
	r, err := New(Options{
		SnapshotFile: path,
		Retention:    retention,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	return r, path
}

func TestBeginCompleteAndDuplicate(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	outcome, err := r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeAccepted, outcome)

	// A second begin while the batch is still pending is an incomplete
	// state, not a silent replay.
	_, err = r.Begin(ctx, "source-a", "batch-1", 1)
	require.ErrorIs(t, err, ErrIncompleteBatchState)

	require.NoError(t, r.Complete(ctx, "source-a", "batch-1", 3))

	// The completed batch is recognized as a duplicate on retry.
	outcome, err = r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeDuplicate, outcome)
}

func TestStaleBatchCannotOverwriteNewerState(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	_, err := r.Begin(ctx, "source-a", "batch-1", 5)
	require.NoError(t, err)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-1", 1))

	// A different batch with an equal or lower sequence is stale.
	_, err = r.Begin(ctx, "source-a", "batch-old", 5)
	require.ErrorIs(t, err, ErrStaleBatch)
	_, err = r.Begin(ctx, "source-a", "batch-old", 4)
	require.ErrorIs(t, err, ErrStaleBatch)

	// The next sequence is accepted and must not have been stored while the
	// stale attempts were rejected.
	_, err = r.Begin(ctx, "source-a", "batch-2", 6)
	require.NoError(t, err)

	r.mtx.Lock()
	require.Len(t, r.sources["source-a"].Batches, 2)
	r.mtx.Unlock()
}

func TestSequencesAreScopedPerSource(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	_, err := r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	// The same sequence number is valid for a different source.
	_, err = r.Begin(ctx, "source-b", "batch-1", 1)
	require.NoError(t, err)
}

func TestBeginRejectsEmptyIdentifier(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)

	_, err := r.Begin(t.Context(), "", "batch-1", 1)
	require.ErrorIs(t, err, ErrIncompleteIdentifier)
	_, err = r.Begin(t.Context(), "source-a", "", 1)
	require.ErrorIs(t, err, ErrIncompleteIdentifier)
}

func TestRecordsSurviveRestart(t *testing.T) {
	t.Parallel()

	_, path := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	r, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	_, err = r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-1", 2))
	_, err = r.Begin(ctx, "source-a", "batch-2", 2)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	// Simulate a process restart by rebuilding the recorder from the log.
	restarted, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	outcome, err := restarted.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeDuplicate, outcome)

	// The batch that was left pending surfaces as an incomplete state rather
	// than being re-driven.
	_, err = restarted.Begin(ctx, "source-a", "batch-2", 2)
	require.ErrorIs(t, err, ErrIncompleteBatchState)

	// Sequence high-water marks are restored as well.
	_, err = restarted.Begin(ctx, "source-a", "batch-old", 1)
	require.ErrorIs(t, err, ErrStaleBatch)
}

func TestGCRemovesExpiredRecordsAndCompactsLog(t *testing.T) {
	t.Parallel()

	r, path := newTestRecorder(t, 20*time.Millisecond)
	ctx := t.Context()

	_, err := r.Begin(ctx, "source-a", "batch-expired", 1)
	require.NoError(t, err)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-expired", 1))

	time.Sleep(30 * time.Millisecond)

	_, err = r.Begin(ctx, "source-a", "batch-live", 2)
	require.NoError(t, err)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-live", 1))

	require.NoError(t, r.gcAndCompact())

	r.mtx.Lock()
	require.Len(t, r.sources, 1)
	require.Contains(t, r.sources["source-a"].Batches, "batch-live")
	require.NotContains(t, r.sources["source-a"].Batches, "batch-expired")
	r.mtx.Unlock()

	// The compacted log still replays into the same live state.
	require.NoError(t, r.Close())
	restarted, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	outcome, err := restarted.Begin(ctx, "source-a", "batch-live", 2)
	require.NoError(t, err)
	require.Equal(t, OutcomeDuplicate, outcome)
}

func TestMaintenanceRunsFinalCycleAndReleasesHandle(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)

	stopc := make(chan struct{})
	done := make(chan struct{})
	go func() {
		r.Maintenance(5*time.Millisecond, stopc)
		close(done)
	}()

	close(stopc)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not stop")
	}

	r.mtx.Lock()
	require.Nil(t, r.f)
	r.mtx.Unlock()
}

func TestTornTrailingLineIsTolerated(t *testing.T) {
	t.Parallel()

	r, path := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	_, err := r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-1", 1))
	require.NoError(t, r.Close())

	// Simulate a crash in the middle of appending an event.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	_, err = f.WriteString(`{"kind":"begin","source":"source-a","batch_id":"br`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	restarted, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	outcome, err := restarted.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeDuplicate, outcome)

	// The torn line must have been truncated so the log stays parseable.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(data), "br")
}

func TestCorruptLogIsRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "idempotency")
	require.NoError(t, os.WriteFile(path, []byte("{\"kind\":\"begin\"}\nnot-json-at-all\n"), 0o666))

	_, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.ErrorIs(t, err, ErrInvalidState)
}

func TestUnwritableLogFailsOpen(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("permission checks do not apply when running as root")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "idempotency")
	r, err := New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.NoError(t, r.Close())

	// Make the existing log read-only so reopening for append fails.
	require.NoError(t, os.Chmod(path, 0o444))
	t.Cleanup(func() { _ = os.Chmod(path, 0o666) })

	_, err = New(Options{
		SnapshotFile: path,
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.Error(t, err)
}

func TestMemoryOnlyMode(t *testing.T) {
	t.Parallel()

	r, err := New(Options{
		SnapshotFile: "",
		Retention:    time.Hour,
		Logger:       promslog.NewNopLogger(),
		Metrics:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	ctx := t.Context()
	outcome, err := r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeAccepted, outcome)
	require.NoError(t, r.Complete(ctx, "source-a", "batch-1", 1))
	outcome, err = r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	require.Equal(t, OutcomeDuplicate, outcome)
	require.NoError(t, r.Close())
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	_, err := New(Options{Retention: 0})
	require.Error(t, err)
}

func TestUnknownBatchCompleteFails(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)
	err := r.Complete(t.Context(), "source-a", "missing", 1)
	require.ErrorIs(t, err, ErrIncompleteBatchState)
}

func TestSentinelErrorsAreWrapped(t *testing.T) {
	t.Parallel()

	r, _ := newTestRecorder(t, time.Hour)
	ctx := t.Context()

	_, err := r.Begin(ctx, "source-a", "batch-1", 1)
	require.NoError(t, err)
	_, err = r.Begin(ctx, "source-a", "batch-1", 1)
	require.ErrorIs(t, err, ErrIncompleteBatchState)

	_, err = r.Begin(ctx, "source-a", "batch-old", 0)
	require.ErrorIs(t, err, ErrStaleBatch)
}
