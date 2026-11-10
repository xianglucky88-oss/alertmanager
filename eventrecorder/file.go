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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// FileOutputConfig configures a JSONL file event recorder output.
type FileOutputConfig struct {
	// Name identifies this output in metrics and logs.
	Name string `yaml:"name" json:"name"`
	// Path is the JSONL file to append events to.  Created if absent.
	Path string `yaml:"path" json:"path"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface, validating
// the file output configuration.
func (c *FileOutputConfig) UnmarshalYAML(unmarshal func(any) error) error {
	type plain FileOutputConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return errors.New("invalid event_recorder file output configuration")
	}
	if _, err := outputIdentifier("file", c.Name); err != nil {
		return err
	}
	if c.Path == "" {
		return errors.New("event_recorder file output requires a path")
	}
	return nil
}

// equal reports whether two file output configs are semantically equal.
func (c FileOutputConfig) equal(o FileOutputConfig) bool {
	return c.Name == o.Name && c.Path == o.Path
}

// fileSink owns the JSONL target file and its logrotation watcher. It
// implements spoolSink so both the direct FileOutput and the durable
// spooled output write the exact same bytes to the same file format.
type fileSink struct {
	path      string
	name      string
	mu        sync.Mutex
	f         *os.File
	closed    bool
	logger    *slog.Logger
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// newFileSink opens the target file (creating it if absent) and starts
// watching its parent directory for external rotation.
func newFileSink(cfg FileOutputConfig, logger *slog.Logger) (*fileSink, error) {
	name, err := outputIdentifier("file", cfg.Name)
	if err != nil {
		return nil, err
	}
	if cfg.Path == "" {
		return nil, errors.New("file output requires a path")
	}
	f, err := openAppend(cfg.Path)
	if err != nil {
		return nil, err
	}

	sink := &fileSink{
		path:   cfg.Path,
		name:   name,
		f:      f,
		logger: logger,
		done:   make(chan struct{}),
	}

	ready := make(chan error, 1)
	sink.wg.Add(1)
	go sink.watchLoop(ready)
	if err := <-ready; err != nil {
		f.Close()
		return nil, fmt.Errorf("starting file watcher: %w", err)
	}
	return sink, nil
}

// Name returns a stable identifier for this sink.
func (s *fileSink) Name() string {
	return s.name
}

// encode serializes the event as the JSONL bytes written to the target
// file: one protojson object followed by a trailing newline.
func (s *fileSink) encode(event Event) ([]byte, error) {
	data, err := event.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// write appends pre-encoded JSONL bytes to the target file.
func (s *fileSink) write(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("file event recorder output is closed")
	}
	_, err := s.f.Write(payload)
	return err
}

// Close stops the watcher goroutine, waits for it to exit, and closes
// the file. It is safe to call more than once.
func (s *fileSink) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.f.Close()
}

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
}

func (s *fileSink) reopen() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	if s.f != nil {
		s.f.Close()
	}
	f, err := openAppend(s.path)
	if err != nil {
		s.logger.Error("Failed to reopen event recorder file", "output", s.name, "path", s.path, "err", err)
		return
	}
	s.f = f
}

func (s *fileSink) watchLoop(ready chan<- error) {
	defer s.wg.Done()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		ready <- fmt.Errorf("creating fsnotify watcher: %w", err)
		return
	}
	defer watcher.Close()

	// Watch the parent directory rather than the file itself.
	// When logrotate renames the file, an inode-level watch is
	// lost with the old inode.  A directory watch reliably
	// delivers Rename/Remove/Create events for the target path.
	dir := filepath.Dir(s.path)
	if err := watcher.Add(dir); err != nil {
		ready <- fmt.Errorf("watching directory %s: %w", dir, err)
		return
	}

	absPath, err := filepath.Abs(s.path)
	if err != nil {
		ready <- fmt.Errorf("resolving absolute path for %s: %w", s.path, err)
		return
	}

	ready <- nil

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Only react to events on our target file.
			evPath, _ := filepath.Abs(event.Name)
			if evPath != absPath {
				continue
			}
			if event.Op&(fsnotify.Rename|fsnotify.Remove|fsnotify.Create) != 0 {
				s.reopen()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			s.logger.Error("fsnotify error on event recorder directory", "output", s.name, "path", s.path, "err", err)
		case <-s.done:
			return
		}
	}
}

// FileOutput writes pre-serialized JSON event bytes to a JSONL file.
// The file is reopened when fsnotify detects a rename or remove (e.g.
// logrotate).
type FileOutput struct {
	sink *fileSink
}

// NewFileOutput creates a new file-based event recorder output. The file is
// watched with fsnotify so that external log
// rotation tools (e.g., logrotate) trigger an immediate reopen.
func NewFileOutput(cfg FileOutputConfig, logger *slog.Logger) (*FileOutput, error) {
	sink, err := newFileSink(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &FileOutput{sink: sink}, nil
}

// Name returns a stable identifier for this output.
func (fo *FileOutput) Name() string {
	return fo.sink.Name()
}

// SendEvent serializes the event as a JSON line and appends it to the
// file.  It returns the number of bytes written (including the trailing
// newline) for the bytes-written metric.
func (fo *FileOutput) SendEvent(event Event) (int, error) {
	data, err := fo.sink.encode(event)
	if err != nil {
		return 0, &serializeError{err: err}
	}
	if err := fo.sink.write(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

// Close stops the watcher goroutine, waits for it to exit, and closes
// the file.
func (fo *FileOutput) Close() error {
	return fo.sink.Close()
}
