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

package v2

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/alertmanager/idempotency"
)

// HTTP headers carrying the optional source-level batch idempotency
// identifier. They are only honored when all three are provided together.
const (
	// HeaderSourceID identifies the sending source.
	HeaderSourceID = "X-Alertmanager-Source-Id"
	// HeaderBatchID identifies a batch uniquely within one source.
	HeaderBatchID = "X-Alertmanager-Batch-Id"
	// HeaderBatchSequence is the monotonically increasing batch sequence
	// within one source.
	HeaderBatchSequence = "X-Alertmanager-Batch-Sequence"
)

// BatchRecorder persists source-level batch idempotency records. A nil
// recorder disables the feature and preserves the legacy ingest semantics.
type BatchRecorder interface {
	// Begin registers the batch. It returns idempotency.OutcomeDuplicate
	// when the batch was already completed and an error when the batch is
	// stale, incomplete or cannot be persisted.
	Begin(ctx context.Context, source, batchID string, sequence uint64) (idempotency.Outcome, error)
	// Complete marks the batch as completed after its alerts were ingested.
	Complete(ctx context.Context, source, batchID string, alertCount int) error
}

// batchIdentifier is the parsed representation of the idempotency request
// headers.
type batchIdentifier struct {
	source   string
	batchID  string
	sequence uint64
}

// parseBatchIdentifier parses the optional idempotency headers. It returns
// enabled=false when none of the headers is set, preserving the legacy
// receive semantics. A partial identifier or an unparsable sequence returns
// an error wrapping idempotency.ErrIncompleteIdentifier.
func parseBatchIdentifier(r *http.Request) (id batchIdentifier, enabled bool, err error) {
	source := r.Header.Get(HeaderSourceID)
	batchID := r.Header.Get(HeaderBatchID)
	sequenceRaw := r.Header.Get(HeaderBatchSequence)

	if source == "" && batchID == "" && sequenceRaw == "" {
		return batchIdentifier{}, false, nil
	}
	if source == "" || batchID == "" || sequenceRaw == "" {
		return batchIdentifier{}, false, fmt.Errorf(
			"idempotency: %w: headers %s, %s and %s must be provided together",
			idempotency.ErrIncompleteIdentifier, HeaderSourceID, HeaderBatchID, HeaderBatchSequence,
		)
	}

	sequence, err := strconv.ParseUint(sequenceRaw, 10, 64)
	if err != nil {
		return batchIdentifier{}, false, fmt.Errorf(
			"idempotency: %w: header %s must be a non-negative integer: %w",
			idempotency.ErrIncompleteIdentifier, HeaderBatchSequence, err,
		)
	}

	return batchIdentifier{
		source:   source,
		batchID:  batchID,
		sequence: sequence,
	}, true, nil
}

// ensureIdempotencyPrefix guarantees that persistence failures carry the
// recognizable "idempotency:" prefix on the wire even when the recorder
// returns an unwrapped error. Errors produced by the idempotency package are
// left untouched.
func ensureIdempotencyPrefix(err error) error {
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "idempotency:") {
		return err
	}
	return fmt.Errorf("idempotency: %w", err)
}
