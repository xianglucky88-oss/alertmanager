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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	spoolFrameMagic0 byte = 0x53 // 'S'
	spoolFrameMagic1 byte = 0x51 // 'Q'
	// Frame header size: magic (2) + payload length (4) + CRC32 (4).
	spoolHeaderSize = 10
	commitFileName  = "commit.idx"

	segmentFilePrefix = "spool-"
	segmentFileSuffix = ".log"

	// Default cap for one output's spool directory: 100 MiB.
	defaultSpoolMaxSize int64 = 100 << 20
	// Default segment size; a new segment is rolled after 8 MiB so that
	// fully-acked segments can be deleted without scanning a huge file.
	defaultSpoolSegmentSize int64 = 8 << 20

	// Bounds the time a flusher waits when the non-blocking notify
	// signal was missed.
	spoolPollInterval = 200 * time.Millisecond
	// First retry delay when a sink is unavailable; it doubles up to
	// spoolMaxBackoff.
	spoolInitialBackoff = 100 * time.Millisecond
	spoolMaxBackoff     = 10 * time.Second
)

var (
	// ErrSpoolFull is returned when an event would make the spool
	// exceed its configured size limit. The event is not written.
	ErrSpoolFull = errors.New("event recorder spool is full")
	// ErrSpoolClosed is returned when the spool is used after Close.
	ErrSpoolClosed = errors.New("event recorder spool is closed")
	// Marks an unreadable frame (bad magic, length or checksum, or a
	// torn write). The flusher truncates the tail and resumes at the
	// last valid frame boundary.
	errSpoolCorrupt = errors.New("corrupt event recorder spool frame")
)

// SpoolConfig configures durable on-disk buffering for file and webhook
// outputs. A zero value (empty Path) disables disk buffering, leaving
// the bounded in-memory queue behavior unchanged.
type SpoolConfig struct {
	// Path is the root directory under which one subdirectory is
	// created per buffered output, keyed by the stable output
	// identifier (e.g. "webhook:primary").
	Path string `yaml:"path,omitempty" json:"path,omitempty"`
	// MaxSize is the maximum size in bytes a single output's spool
	// directory may occupy. Events that would exceed it are dropped
	// and counted before anything is written. Zero means the default.
	MaxSize int64 `yaml:"max_size,omitempty" json:"max_size,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface, validating
// the spool configuration.
func (c *SpoolConfig) UnmarshalYAML(unmarshal func(any) error) error {
	type plain SpoolConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return errors.New("invalid event_recorder spool configuration")
	}
	return c.validate()
}

func (c SpoolConfig) validate() error {
	if c.MaxSize < 0 {
		return errors.New("event_recorder spool max_size cannot be negative")
	}
	if c.Path == "" && c.MaxSize != 0 {
		return errors.New("event_recorder spool max_size requires a path")
	}
	return nil
}

func (c SpoolConfig) enabled() bool {
	return c.Path != ""
}

func (c SpoolConfig) sizeLimit() int64 {
	if c.MaxSize > 0 {
		return c.MaxSize
	}
	return defaultSpoolMaxSize
}

// equal reports whether two spool configs are semantically equal.
func (c SpoolConfig) equal(o SpoolConfig) bool {
	return c.Path == o.Path && c.MaxSize == o.MaxSize
}

// spoolPosition points at the first unacknowledged frame: a segment
// sequence number and a byte offset within that segment.
type spoolPosition struct {
	segment int64
	offset  int64
}

// spoolFrame is one payload read from the spool, together with the
// position needed to acknowledge it.
type spoolFrame struct {
	payload    []byte
	pos        spoolPosition
	nextOffset int64
}

// diskSpool is a durable, ordered, append-only queue of length-prefixed
// frames stored in rolling segment files. A single committed position
// (persisted atomically in commit.idx) records the first frame that has
// not yet been confirmed as delivered, so a restart resumes exactly
// after the last acknowledged event.
type diskSpool struct {
	dir            string
	maxSize        int64
	segmentMaxSize int64
	logger         *slog.Logger
	bytesGauge     prometheus.Gauge
	eventsGauge    prometheus.Gauge

	mu        sync.Mutex
	active    *os.File
	activeSeq int64
	firstSeq  int64
	size      int64
	pending   int64
	commit    spoolPosition
	readFile  *os.File
	readSeq   int64
	notify    chan struct{}
	closed    bool
}

// openDiskSpool opens (or creates) the spool in dir, recovering any
// torn tail from a previous crash and restoring the committed
// position. The segment size is exposed as a parameter for tests.
func openDiskSpool(dir string, maxSize, segmentMaxSize int64, bytesGauge, eventsGauge prometheus.Gauge, logger *slog.Logger) (_ *diskSpool, retErr error) {
	if maxSize <= 0 {
		maxSize = defaultSpoolMaxSize
	}
	if segmentMaxSize <= 0 {
		segmentMaxSize = defaultSpoolSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating event recorder spool directory %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading event recorder spool directory %s: %w", dir, err)
	}
	var seqs []int64
	for _, entry := range entries {
		if seq, ok := parseSegmentName(entry.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	if len(seqs) == 0 {
		seqs = []int64{0}
		if err := os.WriteFile(segmentPath(dir, 0), nil, 0o644); err != nil {
			return nil, fmt.Errorf("creating event recorder spool segment: %w", err)
		}
	}

	// Only the last segment can contain a torn write: rolled segments
	// are never appended to again. Validate it and truncate an invalid
	// tail so the on-disk queue stays readable.
	lastSeq := seqs[len(seqs)-1]
	if _, err := recoverSegment(segmentPath(dir, lastSeq), maxSize); err != nil {
		return nil, err
	}

	commit, err := loadCommit(dir)
	if err != nil {
		return nil, err
	}
	firstSeq := seqs[0]
	if commit.segment < firstSeq || commit.segment > lastSeq {
		commit = spoolPosition{segment: firstSeq, offset: 0}
	}
	if commit.segment == lastSeq {
		if info, statErr := os.Stat(segmentPath(dir, lastSeq)); statErr == nil && commit.offset > info.Size() {
			// The committed frame was lost with a torn tail; resume at
			// the end of the recovered segment.
			commit.offset = info.Size()
		}
	}

	spool := &diskSpool{
		dir:            dir,
		maxSize:        maxSize,
		segmentMaxSize: segmentMaxSize,
		logger:         logger,
		bytesGauge:     bytesGauge,
		eventsGauge:    eventsGauge,
		activeSeq:      lastSeq,
		firstSeq:       firstSeq,
		commit:         commit,
		notify:         make(chan struct{}, 1),
	}
	for _, seq := range seqs {
		info, statErr := os.Stat(segmentPath(dir, seq))
		if statErr != nil {
			return nil, fmt.Errorf("statting event recorder spool segment: %w", statErr)
		}
		spool.size += info.Size()
	}
	spool.pending, err = spool.countPending(seqs, commit)
	if err != nil {
		return nil, err
	}
	active, err := os.OpenFile(segmentPath(dir, lastSeq), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening event recorder spool segment for append: %w", err)
	}
	spool.active = active
	spool.bytesGauge.Set(float64(spool.size))
	spool.eventsGauge.Set(float64(spool.pending))
	return spool, nil
}

// enqueue appends one payload as a framed record. Recording order is
// the append order because the recorder calls enqueue from a single
// goroutine. A failed write never leaves a valid-but-partial frame
// behind: the partial bytes are truncated before returning, and a crash
// mid-write is repaired on the next open.
func (s *diskSpool) enqueue(payload []byte) error {
	if len(payload) > math.MaxUint32 {
		return fmt.Errorf("event recorder spool payload exceeds maximum frame size")
	}
	frameSize := int64(spoolHeaderSize + len(payload))
	if frameSize > s.maxSize {
		// A single oversized event can never fit, so dropping it is
		// the only possible outcome.
		return ErrSpoolFull
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSpoolClosed
	}
	if s.size+frameSize > s.maxSize {
		return ErrSpoolFull
	}
	if info, err := s.active.Stat(); err == nil && info.Size() >= s.segmentMaxSize {
		if err := s.rollLocked(); err != nil {
			return err
		}
	}
	end, err := s.active.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("positioning event recorder spool segment: %w", err)
	}
	if _, err := s.active.Write(encodeFrame(payload)); err != nil {
		// Best-effort rollback so a disk-full error cannot leave a
		// partial frame that would be mistaken for valid data.
		if truncErr := s.active.Truncate(end); truncErr != nil {
			s.logger.Error("Failed to truncate partial spool frame", "output_dir", s.dir, "err", truncErr)
		}
		return fmt.Errorf("writing event recorder spool frame: %w", err)
	}
	if err := s.active.Sync(); err != nil {
		return fmt.Errorf("syncing event recorder spool frame: %w", err)
	}
	s.size += frameSize
	s.pending++
	s.bytesGauge.Set(float64(s.size))
	s.eventsGauge.Set(float64(s.pending))
	s.signalLocked()
	return nil
}

// next blocks until the frame at the committed position is available
// and returns it. The caller must acknowledge it with ack before the
// next frame is delivered, so at most one frame is in flight at a time.
func (s *diskSpool) next(ctx context.Context) (spoolFrame, error) {
	for {
		frame, err := s.tryNext()
		if err == nil {
			return frame, nil
		}
		switch {
		case errors.Is(err, ErrSpoolClosed):
			return spoolFrame{}, err
		case errors.Is(err, errSpoolCorrupt):
			if recoverErr := s.recoverTail(); recoverErr != nil {
				return spoolFrame{}, recoverErr
			}
		case !errors.Is(err, io.EOF):
			return spoolFrame{}, err
		}
		select {
		case <-s.notify:
		case <-time.After(spoolPollInterval):
		case <-ctx.Done():
			return spoolFrame{}, ctx.Err()
		}
	}
}

// ack durably records that frame has been delivered and must never be
// redelivered. Segments that lie entirely before the new position are
// deleted.
func (s *diskSpool) ack(frame spoolFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSpoolClosed
	}
	// Ignore stale acknowledgements (e.g. a flush that completed while
	// the output was being replaced).
	if frame.pos.segment < s.commit.segment ||
		(frame.pos.segment == s.commit.segment && frame.pos.offset < s.commit.offset) {
		return nil
	}
	nextCommit := spoolPosition{segment: frame.pos.segment, offset: frame.nextOffset}
	if err := s.storeCommitLocked(nextCommit); err != nil {
		return err
	}
	s.commit = nextCommit
	// A frame whose fsync failed was still physically appended and
	// therefore was not counted in pending; never report a negative
	// gauge in that edge case.
	if s.pending > 0 {
		s.pending--
	}
	s.eventsGauge.Set(float64(s.pending))

	for seq := s.firstSeq; seq < nextCommit.segment; seq++ {
		path := segmentPath(s.dir, seq)
		info, statErr := os.Stat(path)
		if statErr != nil {
			continue
		}
		if removeErr := os.Remove(path); removeErr == nil {
			s.size -= info.Size()
		}
	}
	for s.firstSeq < nextCommit.segment {
		if _, err := os.Stat(segmentPath(s.dir, s.firstSeq)); err == nil {
			// Removal failed; retry after the next acknowledgement.
			break
		}
		s.firstSeq++
	}
	if s.readSeq < s.firstSeq && s.readFile != nil {
		s.readFile.Close()
		s.readFile = nil
	}
	s.bytesGauge.Set(float64(s.size))
	return nil
}

// close flushes and releases all spool files. Frames that have not been
// acknowledged remain on disk and are redelivered on the next open.
func (s *diskSpool) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.notify)
	var errs []error
	if s.readFile != nil {
		errs = append(errs, s.readFile.Close())
	}
	if s.active != nil {
		errs = append(errs, s.active.Sync())
		errs = append(errs, s.active.Close())
	}
	return errors.Join(errs...)
}

func (s *diskSpool) tryNext() (spoolFrame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return spoolFrame{}, ErrSpoolClosed
	}
	// The committed position may sit at the end of a fully-read
	// segment; walk forward across rolled segments until a frame is
	// found or the active segment reaches EOF.
	seq := s.commit.segment
	offset := s.commit.offset
	for {
		file, err := s.readerLocked(seq)
		if err != nil {
			return spoolFrame{}, err
		}
		payload, nextOffset, readErr := readFrameAt(file, offset, s.maxSize)
		if readErr == nil {
			return spoolFrame{
				payload:    payload,
				pos:        spoolPosition{segment: seq, offset: offset},
				nextOffset: nextOffset,
			}, nil
		}
		if !errors.Is(readErr, io.EOF) {
			return spoolFrame{}, readErr
		}
		if seq >= s.activeSeq {
			return spoolFrame{}, io.EOF
		}
		seq++
		offset = 0
	}
}

func (s *diskSpool) readerLocked(seq int64) (*os.File, error) {
	if s.readFile != nil && s.readSeq == seq {
		return s.readFile, nil
	}
	if s.readFile != nil {
		s.readFile.Close()
		s.readFile = nil
	}
	file, err := os.Open(segmentPath(s.dir, seq))
	if err != nil {
		return nil, fmt.Errorf("opening event recorder spool segment: %w", err)
	}
	s.readFile = file
	s.readSeq = seq
	return file, nil
}

func (s *diskSpool) rollLocked() error {
	previous := s.active
	previousSeq := s.activeSeq
	if err := previous.Sync(); err != nil {
		return fmt.Errorf("syncing event recorder spool segment: %w", err)
	}
	if err := previous.Close(); err != nil {
		return fmt.Errorf("closing event recorder spool segment: %w", err)
	}
	if s.readFile != nil && s.readSeq == previousSeq {
		s.readFile.Close()
		s.readFile = nil
	}
	nextSeq := previousSeq + 1
	file, err := os.OpenFile(segmentPath(s.dir, nextSeq), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		// Keep the spool usable by reopening the previous segment.
		reopened, reopenErr := os.OpenFile(segmentPath(s.dir, previousSeq), os.O_APPEND|os.O_WRONLY, 0o644)
		if reopenErr != nil {
			return fmt.Errorf("rolling event recorder spool segment: %w", err)
		}
		s.active = reopened
		return fmt.Errorf("rolling event recorder spool segment: %w", err)
	}
	s.active = file
	s.activeSeq = nextSeq
	return nil
}

// recoverTail truncates an invalid frame from the active segment and
// rewinds the committed position if it pointed past the valid tail.
func (s *diskSpool) recoverTail() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := segmentPath(s.dir, s.activeSeq)
	validSize, err := recoverSegment(path, s.maxSize)
	if err != nil {
		return err
	}
	if s.commit.segment == s.activeSeq && s.commit.offset > validSize {
		s.commit.offset = validSize
	}
	if s.readFile != nil {
		s.readFile.Close()
		s.readFile = nil
	}
	return nil
}

func (s *diskSpool) countPending(seqs []int64, commit spoolPosition) (int64, error) {
	var count int64
	for _, seq := range seqs {
		if seq < commit.segment {
			continue
		}
		file, err := os.Open(segmentPath(s.dir, seq))
		if err != nil {
			return 0, fmt.Errorf("opening event recorder spool segment: %w", err)
		}
		offset := int64(0)
		if seq == commit.segment {
			offset = commit.offset
		}
		for {
			_, next, readErr := readFrameAt(file, offset, s.maxSize)
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				return 0, readErr
			}
			count++
			offset = next
		}
		file.Close()
	}
	return count, nil
}

func (s *diskSpool) storeCommitLocked(position spoolPosition) error {
	tmpPath := filepath.Join(s.dir, commitFileName+".tmp")
	finalPath := filepath.Join(s.dir, commitFileName)
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("writing event recorder spool commit index: %w", err)
	}
	if _, err := fmt.Fprintf(file, "%d %d\n", position.segment, position.offset); err != nil {
		file.Close()
		return fmt.Errorf("writing event recorder spool commit index: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("syncing event recorder spool commit index: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing event recorder spool commit index: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("committing event recorder spool position: %w", err)
	}
	// Best-effort directory fsync so the rename is durable.
	if dir, err := os.Open(s.dir); err == nil {
		if err := dir.Sync(); err != nil {
			s.logger.Debug("Failed to fsync event recorder spool directory", "err", err)
		}
		dir.Close()
	}
	return nil
}

func (s *diskSpool) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// spoolSink delivers one pre-encoded event payload to a concrete
// destination (a JSONL file or an HTTP webhook).
type spoolSink interface {
	// Name returns the stable output identifier.
	Name() string
	// encode serializes an event into the exact on-the-wire bytes the
	// destination expects; the spool stores those bytes verbatim, so
	// buffering never changes the output format.
	encode(event Event) ([]byte, error)
	// write delivers one payload. It is invoked by a single flusher
	// goroutine, one frame at a time. A non-nil error leaves the frame
	// spooled so it is retried in order.
	write(payload []byte) error
	io.Closer
}

// spooledOutput is a Destination that durably spools encoded events to
// disk before delivering them through a spoolSink. Delivery is
// single-flight and strictly ordered; the committed position advances
// only after a successful write, so a restart or a configuration reload
// resumes after the last confirmed event without replaying confirmed
// events.
type spooledOutput struct {
	name        string
	spoolDir    string
	cfg         SpoolConfig
	sink        spoolSink
	metrics     *metrics
	logger      *slog.Logger
	segmentSize int64

	mu        sync.Mutex
	spool     *diskSpool
	started   bool
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// newSpooledOutput builds a spooling destination around sink. The spool
// is opened lazily by start so that, during a configuration reload, the
// previous owner releases the directory before a new one opens it.
func newSpooledOutput(sink spoolSink, spoolRoot string, cfg SpoolConfig, m *metrics, logger *slog.Logger) (*spooledOutput, error) {
	if cfg.Path == "" {
		return nil, errors.New("spool output requires a spool path")
	}
	return &spooledOutput{
		name:        sink.Name(),
		spoolDir:    filepath.Join(spoolRoot, spoolDirName(sink.Name())),
		cfg:         cfg,
		sink:        sink,
		metrics:     m,
		logger:      logger,
		segmentSize: defaultSpoolSegmentSize,
		done:        make(chan struct{}),
	}, nil
}

// Name returns the stable identifier of the wrapped destination.
func (o *spooledOutput) Name() string { return o.name }

// start opens the on-disk spool and launches the delivery flusher.
func (o *spooledOutput) start() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return nil
	}
	spool, err := openDiskSpool(
		o.spoolDir,
		o.cfg.sizeLimit(),
		o.segmentSize,
		o.metrics.spoolBytes.WithLabelValues(o.name),
		o.metrics.spoolEvents.WithLabelValues(o.name),
		o.logger,
	)
	if err != nil {
		return fmt.Errorf("opening event recorder spool for output %s: %w", o.name, err)
	}
	o.spool = spool
	o.started = true
	o.wg.Add(1)
	go o.flushLoop()
	return nil
}

// SendEvent encodes the event with the destination's own encoder and
// durably enqueues the payload. Serialization failures are returned
// before the spool is touched, so an encoding error can never leave a
// partial or malformed record on disk.
func (o *spooledOutput) SendEvent(event Event) (int, error) {
	payload, err := o.sink.encode(event)
	if err != nil {
		return 0, &serializeError{err: err}
	}
	o.mu.Lock()
	spool := o.spool
	o.mu.Unlock()
	if spool == nil {
		return 0, fmt.Errorf("event recorder output %s spool has not started", o.name)
	}
	if err := spool.enqueue(payload); err != nil {
		switch {
		case errors.Is(err, ErrSpoolFull):
			o.metrics.spoolEnqueueErrors.WithLabelValues(o.name, "full").Inc()
		case errors.Is(err, ErrSpoolClosed):
			// Output is shutting down; nothing durable to record.
		default:
			o.metrics.spoolEnqueueErrors.WithLabelValues(o.name, "disk").Inc()
		}
		return 0, err
	}
	return len(payload), nil
}

// Close stops the flusher, closes the spool and releases the sink.
// Unacknowledged frames stay on disk and are delivered by the next
// owner of the spool directory.
func (o *spooledOutput) Close() error {
	o.closeOnce.Do(func() { close(o.done) })
	o.wg.Wait()
	o.mu.Lock()
	spool := o.spool
	o.mu.Unlock()
	var errs []error
	if spool != nil {
		errs = append(errs, spool.close())
	}
	errs = append(errs, o.sink.Close())
	return errors.Join(errs...)
}

func (o *spooledOutput) flushLoop() {
	defer o.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-o.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	backoff := spoolInitialBackoff
	for {
		frame, err := o.spool.next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, ErrSpoolClosed) {
				return
			}
			o.logger.Error("Failed to read from event recorder spool", "output", o.name, "err", err)
			if !o.wait(backoff) {
				return
			}
			backoff = nextSpoolBackoff(backoff)
			continue
		}
		if err := o.sink.write(frame.payload); err != nil {
			o.metrics.spoolDeliveryErrors.WithLabelValues(o.name).Inc()
			o.logger.Error("Event recorder delivery failed; event retained in spool for retry", "output", o.name, "err", err)
			if !o.wait(backoff) {
				return
			}
			backoff = nextSpoolBackoff(backoff)
			continue
		}
		if err := o.spool.ack(frame); err != nil {
			o.logger.Error("Failed to commit event recorder spool position", "output", o.name, "err", err)
		}
		backoff = spoolInitialBackoff
	}
}

func (o *spooledOutput) wait(delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-o.done:
		return false
	}
}

func nextSpoolBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > spoolMaxBackoff || next <= 0 {
		return spoolMaxBackoff
	}
	return next
}

// spoolDirName derives a deterministic, filesystem-safe directory name
// from the stable output identifier. The FNV suffix keeps distinct
// names whose sanitized forms would collide.
func spoolDirName(id string) string {
	var builder strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			builder.WriteRune(r)
		default:
			builder.WriteRune('_')
		}
	}
	hash := fnv.New32a()
	hash.Write([]byte(id))
	return fmt.Sprintf("%s-%08x", builder.String(), hash.Sum32())
}

func segmentPath(dir string, seq int64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%08d%s", segmentFilePrefix, seq, segmentFileSuffix))
}

func parseSegmentName(name string) (int64, bool) {
	if !strings.HasPrefix(name, segmentFilePrefix) || !strings.HasSuffix(name, segmentFileSuffix) {
		return 0, false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(name, segmentFilePrefix), segmentFileSuffix)
	seq, err := strconv.ParseInt(middle, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func loadCommit(dir string) (spoolPosition, error) {
	data, err := os.ReadFile(filepath.Join(dir, commitFileName))
	if errors.Is(err, os.ErrNotExist) {
		return spoolPosition{}, nil
	}
	if err != nil {
		return spoolPosition{}, fmt.Errorf("reading event recorder spool commit index: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return spoolPosition{}, fmt.Errorf("event recorder spool commit index is malformed")
	}
	segment, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return spoolPosition{}, fmt.Errorf("event recorder spool commit index is malformed: %w", err)
	}
	offset, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || offset < 0 {
		return spoolPosition{}, fmt.Errorf("event recorder spool commit index is malformed")
	}
	return spoolPosition{segment: segment, offset: offset}, nil
}

// encodeFrame builds the on-disk representation of a payload:
// magic bytes, big-endian length, CRC32 checksum and the payload.
func encodeFrame(payload []byte) []byte {
	frame := make([]byte, 0, spoolHeaderSize+len(payload))
	frame = append(frame, spoolFrameMagic0, spoolFrameMagic1)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	frame = append(frame, length[:]...)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
	frame = append(frame, checksum[:]...)
	return append(frame, payload...)
}

// readFrameAt reads one frame at offset from file. A clean end of file
// returns io.EOF; a truncated or invalid frame returns errSpoolCorrupt.
func readFrameAt(file *os.File, offset, maxPayload int64) ([]byte, int64, error) {
	header := make([]byte, spoolHeaderSize)
	n, err := file.ReadAt(header, offset)
	if errors.Is(err, io.EOF) {
		if n == 0 {
			return nil, offset, io.EOF
		}
		return nil, offset, errSpoolCorrupt
	}
	if err != nil {
		return nil, offset, fmt.Errorf("reading event recorder spool frame header: %w", err)
	}
	if header[0] != spoolFrameMagic0 || header[1] != spoolFrameMagic1 {
		return nil, offset, errSpoolCorrupt
	}
	length := int64(binary.BigEndian.Uint32(header[2:6]))
	if length <= 0 || length > maxPayload {
		return nil, offset, errSpoolCorrupt
	}
	checksum := binary.BigEndian.Uint32(header[6:10])
	payload := make([]byte, length)
	if _, err := file.ReadAt(payload, offset+spoolHeaderSize); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, offset, errSpoolCorrupt
		}
		return nil, offset, fmt.Errorf("reading event recorder spool frame payload: %w", err)
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, offset, errSpoolCorrupt
	}
	return payload, offset + spoolHeaderSize + length, nil
}

// recoverSegment scans a segment from the beginning, validating every
// frame, and truncates it at the first invalid or partial frame. It
// returns the valid byte size.
func recoverSegment(path string, maxPayload int64) (int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, fmt.Errorf("opening event recorder spool segment for recovery: %w", err)
	}
	defer file.Close()

	var offset int64
	for {
		_, next, readErr := readFrameAt(file, offset, maxPayload)
		if errors.Is(readErr, io.EOF) {
			return offset, nil
		}
		if readErr != nil {
			if truncErr := file.Truncate(offset); truncErr != nil {
				return 0, fmt.Errorf("truncating torn event recorder spool frame: %w", truncErr)
			}
			return offset, nil
		}
		offset = next
	}
}
