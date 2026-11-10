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

package eventrecorder

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testGauge(name string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Name: name})
}

func newTestDiskSpool(t *testing.T, dir string, maxSize, segmentSize int64) *diskSpool {
	t.Helper()
	spool, err := openDiskSpool(
		dir, maxSize, segmentSize,
		testGauge("test_spool_bytes"),
		testGauge("test_spool_events"),
		slog.Default(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.close()) })
	return spool
}

// mockSpoolSink is a controllable spoolSink whose failures can be
// toggled to simulate an unavailable destination.
type mockSpoolSink struct {
	mu        sync.Mutex
	name      string
	fail      bool
	encodeErr error
	delivered [][]byte
	closed    bool
}

func newMockSpoolSink(name string) *mockSpoolSink {
	return &mockSpoolSink{name: name}
}

func (m *mockSpoolSink) Name() string { return m.name }

func (m *mockSpoolSink) encode(event Event) ([]byte, error) {
	if m.encodeErr != nil {
		return nil, m.encodeErr
	}
	return event.MarshalJSON()
}

func (m *mockSpoolSink) write(payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("sink unavailable")
	}
	m.delivered = append(m.delivered, append([]byte(nil), payload...))
	return nil
}

func (m *mockSpoolSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockSpoolSink) deliveries() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]byte(nil), m.delivered...)
}

func (m *mockSpoolSink) setFail(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = fail
}

// failingFileSink wraps a real fileSink and fails the first failUntil
// write attempts, exercising spool retries against the real JSONL
// target file.
type failingFileSink struct {
	*fileSink
	mu        sync.Mutex
	attempts  int
	failUntil int
	written   [][]byte
}

func (f *failingFileSink) write(payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failUntil {
		return errors.New("target file temporarily unavailable")
	}
	f.written = append(f.written, append([]byte(nil), payload...))
	return f.fileSink.write(payload)
}

func (f *failingFileSink) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.written)
}

func newTestSpooledOutput(t *testing.T, spoolRoot string, sink spoolSink) *spooledOutput {
	t.Helper()
	out, err := newSpooledOutput(sink, spoolRoot, SpoolConfig{Path: spoolRoot}, newMetrics(nil), slog.Default())
	require.NoError(t, err)
	require.NoError(t, out.start())
	t.Cleanup(func() { require.NoError(t, out.Close()) })
	return out
}

func spoolPending(t *testing.T, out *spooledOutput) int64 {
	t.Helper()
	out.mu.Lock()
	spool := out.spool
	out.mu.Unlock()
	require.NotNil(t, spool)
	spool.mu.Lock()
	defer spool.mu.Unlock()
	return spool.pending
}

func testVersionEvent(version string) Event {
	return NewAlertmanagerStartupEvent(version, "").
		withMetadata(timestamppb.Now(), "test-host", 0)
}

func TestDiskSpool_EnqueueDequeueOrder(t *testing.T) {
	spool := newTestDiskSpool(t, t.TempDir(), 0, 0)
	payloads := []string{"alpha", "beta", "gamma", "delta"}
	for _, payload := range payloads {
		require.NoError(t, spool.enqueue([]byte(payload)))
	}

	var got []string
	for range payloads {
		frame, err := spool.next(context.Background())
		require.NoError(t, err)
		got = append(got, string(frame.payload))
		require.NoError(t, spool.ack(frame))
	}
	require.Equal(t, payloads, got)
	require.Zero(t, spool.pending)
}

// A confirmed event must never be redelivered after a restart; delivery
// resumes exactly at the committed position.
func TestDiskSpool_RestartResumesAtCommit(t *testing.T) {
	dir := t.TempDir()
	spool := newTestDiskSpool(t, dir, 0, 0)
	for _, payload := range []string{"one", "two", "three"} {
		require.NoError(t, spool.enqueue([]byte(payload)))
	}
	frame, err := spool.next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "one", string(frame.payload))
	require.NoError(t, spool.ack(frame))
	require.NoError(t, spool.close())

	spool = newTestDiskSpool(t, dir, 0, 0)
	for _, want := range []string{"two", "three"} {
		frame, err := spool.next(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, string(frame.payload))
		require.NoError(t, spool.ack(frame))
	}
	require.Zero(t, spool.pending)
}

// A torn tail (crash or ENOSPC mid-write) is truncated; valid frames
// survive and new appends remain readable.
func TestDiskSpool_TornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	spool := newTestDiskSpool(t, dir, 0, 0)
	require.NoError(t, spool.enqueue([]byte("good-1")))
	require.NoError(t, spool.enqueue([]byte("good-2")))
	require.NoError(t, spool.close())

	active := segmentPath(dir, 0)
	f, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	// Header claims a 9-byte payload that is not fully present.
	_, err = f.Write([]byte{spoolFrameMagic0, spoolFrameMagic1, 0, 0, 0, 9, 0, 0, 0, 0, 'x'})
	require.NoError(t, err)
	// And some non-frame garbage after it.
	_, err = f.Write([]byte("not-a-frame"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	spool = newTestDiskSpool(t, dir, 0, 0)
	for _, want := range []string{"good-1", "good-2"} {
		frame, err := spool.next(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, string(frame.payload))
		require.NoError(t, spool.ack(frame))
	}
	require.NoError(t, spool.enqueue([]byte("good-3")))
	frame, err := spool.next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "good-3", string(frame.payload))
	require.NoError(t, spool.ack(frame))
}

// When the cap is reached the event is rejected before being written,
// leaving the existing frames intact and readable after a restart.
func TestDiskSpool_FullRejectsAndStaysReadable(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("payload")
	frameSize := int64(spoolHeaderSize + len(payload))
	spool := newTestDiskSpool(t, dir, frameSize*2, frameSize*10)

	require.NoError(t, spool.enqueue(payload))
	require.NoError(t, spool.enqueue(payload))
	require.ErrorIs(t, spool.enqueue(payload), ErrSpoolFull)
	require.NoError(t, spool.close())

	spool = newTestDiskSpool(t, dir, frameSize*2, frameSize*10)
	for range 2 {
		frame, err := spool.next(context.Background())
		require.NoError(t, err)
		require.Equal(t, payload, frame.payload)
		require.NoError(t, spool.ack(frame))
	}
}

// Segments roll at the configured size and fully-acked segments are
// deleted; only unacknowledged frames survive a restart.
func TestDiskSpool_SegmentRollAndDelete(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("x")
	frameSize := int64(spoolHeaderSize + len(payload))
	spool := newTestDiskSpool(t, dir, defaultSpoolMaxSize, frameSize)

	const total = 4
	for range total {
		require.NoError(t, spool.enqueue(payload))
	}
	var segments int
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if _, ok := parseSegmentName(entry.Name()); ok {
			segments++
		}
	}
	require.Equal(t, total, segments)

	for range total - 1 {
		frame, err := spool.next(context.Background())
		require.NoError(t, err)
		require.Equal(t, payload, frame.payload)
		require.NoError(t, spool.ack(frame))
	}
	require.NoError(t, spool.close())

	spool = newTestDiskSpool(t, dir, defaultSpoolMaxSize, frameSize)
	require.Equal(t, int64(1), spool.pending)
	frame, err := spool.next(context.Background())
	require.NoError(t, err)
	require.Equal(t, payload, frame.payload)
	require.NoError(t, spool.ack(frame))
}

// End-to-end restart: events buffered while the sink is down are
// delivered exactly once, in order, after a new owner opens the spool.
func TestSpooledOutput_RestartDeliversOnceInOrder(t *testing.T) {
	dir := t.TempDir()

	first := newMockSpoolSink("webhook:primary")
	first.setFail(true)
	out := newTestSpooledOutput(t, dir, first)

	events := []Event{
		testVersionEvent("v1"),
		testVersionEvent("v2"),
		testVersionEvent("v3"),
		testVersionEvent("v4"),
		testVersionEvent("v5"),
	}
	for _, event := range events {
		_, err := out.SendEvent(event)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return spoolPending(t, out) == int64(len(events))
	}, time.Second, 10*time.Millisecond)
	require.Empty(t, first.deliveries())
	require.NoError(t, out.Close())

	// Simulate a process restart: a brand-new output over the same
	// spool directory, backed by a healthy sink.
	second := newMockSpoolSink("webhook:primary")
	restarted := newTestSpooledOutput(t, dir, second)
	require.Eventually(t, func() bool {
		return len(second.deliveries()) == len(events)
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, restarted.Close())

	delivered := second.deliveries()
	require.Len(t, delivered, len(events))
	for i, event := range events {
		want, err := event.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, want, delivered[i], "event %d redelivered out of order or duplicated", i)
		require.True(t, json.Valid(delivered[i]))
	}
	require.Zero(t, spoolPending(t, restarted))
}

// Transient file-sink failures must not change the JSONL output format:
// the target file ends up with one JSON object per line, in order.
func TestSpooledOutput_FileSinkPreservesJSONLFormat(t *testing.T) {
	base := t.TempDir()
	spoolRoot := filepath.Join(base, "spool")
	target := filepath.Join(base, "events.jsonl")

	sink, err := newFileSink(FileOutputConfig{Name: "archive", Path: target}, slog.Default())
	require.NoError(t, err)
	failing := &failingFileSink{fileSink: sink, failUntil: 3}
	out := newTestSpooledOutput(t, spoolRoot, failing)

	const total = 6
	for range total {
		_, err := out.SendEvent(sampleEvent())
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return failing.writeCount() == total
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, out.Close())

	data, err := os.ReadFile(target)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	require.Len(t, lines, total)
	for _, line := range lines {
		require.True(t, strings.HasPrefix(line, "{"))
		require.True(t, json.Valid([]byte(line)))
		require.Contains(t, line, "alertmanagerStartupEvent")
	}
}

// A real webhook sink that is unavailable at startup gets every spooled
// event exactly once after the endpoint recovers across a restart.
func TestSpooledOutput_WebhookSinkRestart(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]byte
		fail     atomic.Bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	fail.Store(true)
	firstSink, err := newWebhookSink(WebhookOutputConfig{Name: "hook", URL: mustParseURL(t, srv.URL)}, slog.Default())
	require.NoError(t, err)
	first, err := newSpooledOutput(firstSink, dir, SpoolConfig{Path: dir}, newMetrics(nil), slog.Default())
	require.NoError(t, err)
	require.NoError(t, first.start())

	events := []Event{testVersionEvent("w1"), testVersionEvent("w2"), testVersionEvent("w3")}
	for _, event := range events {
		_, err := first.SendEvent(event)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return spoolPending(t, first) == int64(len(events))
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, first.Close())

	fail.Store(false)
	secondSink, err := newWebhookSink(WebhookOutputConfig{Name: "hook", URL: mustParseURL(t, srv.URL)}, slog.Default())
	require.NoError(t, err)
	restarted := newTestSpooledOutput(t, dir, secondSink)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == len(events)
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, restarted.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, len(events))
	for i, event := range events {
		want, err := event.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, want, received[i])
	}
}

// Encoding failures surface as serializeError and never create a spool
// frame, so a bad event cannot corrupt the on-disk queue.
func TestSpooledOutput_EncodeErrorDoesNotTouchSpool(t *testing.T) {
	dir := t.TempDir()
	sink := newMockSpoolSink("webhook:broken")
	sink.encodeErr = errors.New("cannot marshal event")
	out := newTestSpooledOutput(t, dir, sink)

	_, err := out.SendEvent(sampleEvent())
	require.Error(t, err)
	var serializationErr *serializeError
	require.ErrorAs(t, err, &serializationErr)
	require.NoError(t, out.Close())

	require.Zero(t, spoolPending(t, out))
	entries, err := os.ReadDir(out.spoolDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if _, ok := parseSegmentName(entry.Name()); ok {
			info, statErr := entry.Info()
			require.NoError(t, statErr)
			require.Zero(t, info.Size(), "no frame may be written for an encoding failure")
		}
	}
}

func TestSpooledOutput_SendBeforeStartErrors(t *testing.T) {
	dir := t.TempDir()
	sink := newMockSpoolSink("webhook:primary")
	out, err := newSpooledOutput(sink, dir, SpoolConfig{Path: dir}, newMetrics(nil), slog.Default())
	require.NoError(t, err)
	_, err = out.SendEvent(sampleEvent())
	require.Error(t, err)
	require.NoError(t, out.Close())
}

func TestBuildOutputs_SpoolWrapsFileAndWebhook(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Spool: SpoolConfig{Path: dir},
		FileOutputs: []FileOutputConfig{{
			Name: "archive",
			Path: filepath.Join(dir, "events.jsonl"),
		}},
		WebhookOutputs: []WebhookOutputConfig{{
			Name: "hook",
			URL:  mustParseURL(t, "https://example.com/hook"),
		}},
	}
	outputs := buildOutputs(cfg, "test", newMetrics(nil), slog.Default())
	require.Len(t, outputs, 2)
	for _, out := range outputs {
		spooled, ok := out.(*spooledOutput)
		require.True(t, ok)
		require.Equal(t, filepath.Join(dir, spoolDirName(out.Name())), spooled.spoolDir)
		require.NoError(t, out.Close())
	}
}

func TestSpoolDirName_StableAndDistinct(t *testing.T) {
	first := spoolDirName("webhook:primary")
	second := spoolDirName("webhook:primary")
	require.Equal(t, first, second)
	require.NotEqual(t, spoolDirName("file:a"), spoolDirName("webhook:a"))
	require.NotEqual(t, spoolDirName("webhook:日本語"), spoolDirName("webhook:english"))
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name, output string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "output" && label.GetValue() == output {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

// End-to-end behavior through the public Recorder API: events buffered
// while a webhook is down survive an ApplyConfig output rebuild and are
// delivered exactly once, in order, after the endpoint recovers.
func TestRecorder_SpooledWebhookReloadNoDuplicates(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]byte
		fail     atomic.Bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	cfg := Config{
		Spool: SpoolConfig{Path: dir},
		WebhookOutputs: []WebhookOutputConfig{{
			Name: "hook",
			URL:  mustParseURL(t, srv.URL),
		}},
	}
	fail.Store(true)
	rec := NewRecorderFromConfig(cfg, "test-host", slog.Default(), reg)
	t.Cleanup(func() { require.NoError(t, rec.Close()) })

	versions := []string{"r1", "r2", "r3"}
	for _, version := range versions {
		rec.RecordEvent(recordCtx(), func() EventData {
			return NewAlertmanagerStartupEvent(version, "")
		})
	}
	const outputID = "webhook:hook"
	require.Eventually(t, func() bool {
		return gaugeValue(t, reg, "alertmanager_event_spool_events", outputID) == float64(len(versions))
	}, 2*time.Second, 20*time.Millisecond)

	// Force an output rebuild; the unchanged output name keeps the same
	// spool directory, so the old owner must release it before the new
	// one opens it and no event may be delivered twice.
	reloaded := cfg
	reloaded.WebhookOutputs[0].Timeout = 5000
	rec.ApplyConfig(reloaded)

	fail.Store(false)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == len(versions)
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return gaugeValue(t, reg, "alertmanager_event_spool_events", outputID) == 0
	}, 2*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, len(versions))
	for i, version := range versions {
		require.True(t, json.Valid(received[i]))
		require.Contains(t, string(received[i]), version)
	}
}

// --- config tests.

func TestSpoolConfig_UnmarshalYAML(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		var cfg Config
		require.NoError(t, yaml.Unmarshal([]byte("spool:\n  path: /var/spool/am\n  max_size: 1024\n"), &cfg))
		require.Equal(t, "/var/spool/am", cfg.Spool.Path)
		require.Equal(t, int64(1024), cfg.Spool.MaxSize)
	})

	t.Run("negative max size", func(t *testing.T) {
		var cfg Config
		err := yaml.Unmarshal([]byte("spool:\n  max_size: -1\n"), &cfg)
		require.ErrorContains(t, err, "max_size")
	})

	t.Run("max size without path", func(t *testing.T) {
		var cfg Config
		err := yaml.Unmarshal([]byte("spool:\n  max_size: 1024\n"), &cfg)
		require.ErrorContains(t, err, "path")
	})
}

func TestSpoolConfig_BatchWebhookRejected(t *testing.T) {
	cfg := Config{
		Spool: SpoolConfig{Path: "/tmp/spool"},
		WebhookOutputs: []WebhookOutputConfig{{
			Name:  "hook",
			URL:   mustParseURL(t, "https://example.com/hook"),
			Batch: true,
		}},
	}
	require.ErrorContains(t, cfg.validate(), "batching")
}

func TestSpoolConfig_Equal(t *testing.T) {
	a := SpoolConfig{Path: "/tmp/spool", MaxSize: 1024}
	b := SpoolConfig{Path: "/tmp/spool", MaxSize: 1024}
	require.True(t, a.equal(b))

	b.MaxSize = 2048
	require.False(t, a.equal(b))
	b.MaxSize = a.MaxSize
	b.Path = "/other"
	require.False(t, a.equal(b))
}

func TestConfigEqual_Spool(t *testing.T) {
	a := Config{Spool: SpoolConfig{Path: "/tmp/spool"}}
	b := Config{}
	require.False(t, configEqual(a, b))
}
