// Copyright 2018 Prometheus Team
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

package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/alert"
	"github.com/prometheus/alertmanager/limit"
)

// ErrLimited is returned if a Store has reached the per-alert limit.
var ErrLimited = errors.New("alert limited")

// ErrNotFound is returned if a Store cannot find the Alert.
var ErrNotFound = errors.New("alert not found")

// ErrDestroyed is returned if a Store has been destroyed.
var ErrDestroyed = errors.New("alert store destroyed")

// pendingAlert retains the latest known state of an alert instance which could
// not be admitted because the per-alert-name limit was reached. The firstSeen
// timestamp preserves a stable first-in-first-out queue position across
// repeated reports of the same fingerprint.
type pendingAlert struct {
	alert     *alert.Alert
	firstSeen time.Time
}

// Alerts provides lock-coordinated to an in-memory map of alerts, keyed by
// their fingerprint. Resolved alerts are removed from the map based on
// gcInterval. An optional callback can be set which receives a slice of all
// resolved alerts that have been removed.
type Alerts struct {
	sync.Mutex
	alerts        map[model.Fingerprint]*alert.Alert
	gcCallback    func([]*alert.Alert)
	limits        map[string]*limit.Bucket[model.Fingerprint]
	pending       map[string]map[model.Fingerprint]*pendingAlert
	perAlertLimit int
	destroyed     bool
}

// NewAlerts returns a new Alerts struct.
func NewAlerts() *Alerts {
	a := &Alerts{
		alerts:        make(map[model.Fingerprint]*alert.Alert),
		gcCallback:    func(_ []*alert.Alert) {},
		perAlertLimit: 0,
	}

	return a
}

// WithPerAlertLimit sets the per-alert limit for the Alerts struct.
func (a *Alerts) WithPerAlertLimit(lim int) *Alerts {
	a.Lock()
	defer a.Unlock()

	a.limits = make(map[string]*limit.Bucket[model.Fingerprint])
	a.perAlertLimit = lim

	return a
}

// SetGCCallback sets a GC callback to be executed after each GC.
func (a *Alerts) SetGCCallback(cb func([]*alert.Alert)) {
	a.Lock()
	defer a.Unlock()

	a.gcCallback = cb
}

// Run starts the GC loop. The interval must be greater than zero; if not, the function will panic.
// Note: This is only used by inhibitor currently and potentially can be removed later.
func (a *Alerts) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.GC()
		}
	}
}

// GC deletes resolved alerts and returns them.
func (a *Alerts) GC() (deleted []*alert.Alert) {
	// Remove stale alert limit buckets.
	a.gcLimitBuckets()

	// Drop overflow entries that expired while waiting for capacity.
	a.gcPending()

	// Delete resolved alerts.
	deleted = a.gcAlerts()

	// Execute GC callback if needed.
	if len(deleted) > 0 {
		a.gcCallback(deleted)
	}

	return deleted
}

// gcAlerts deletes resolved alerts and returns a copy of them.
func (a *Alerts) gcAlerts() (deleted []*alert.Alert) {
	a.Lock()
	defer a.Unlock()
	for fp, alrt := range a.alerts {
		if alrt.Resolved() {
			deleted = append(deleted, alrt)
			delete(a.alerts, fp)
		}
	}
	return deleted
}

// gcLimitBuckets removes stale alert limit buckets.
func (a *Alerts) gcLimitBuckets() {
	a.Lock()
	defer a.Unlock()

	for alertName, bucket := range a.limits {
		if bucket.IsStale() {
			delete(a.limits, alertName)
		}
	}
}

// gcPending drops overflow alerts whose latest state has expired. Such an
// instance was never admitted while firing, so it has no notification or
// marker state to clean up.
func (a *Alerts) gcPending() {
	a.Lock()
	defer a.Unlock()

	a.gcPendingLocked(time.Now())
}

// gcPendingLocked drops expired overflow alerts. The caller must hold the
// store lock.
func (a *Alerts) gcPendingLocked(now time.Time) {
	for name, queue := range a.pending {
		for fp, p := range queue {
			if isExpired(p.alert, now) {
				delete(queue, fp)
			}
		}
		if len(queue) == 0 {
			delete(a.pending, name)
		}
	}
}

// isExpired reports whether the alert has reached its end timestamp at the
// given time, mirroring model.Alert.Resolved without re-reading the clock.
func isExpired(alrt *alert.Alert, now time.Time) bool {
	return !alrt.EndsAt.IsZero() && !alrt.EndsAt.After(now)
}

// Get returns the Alert with the matching fingerprint, or an error if it is
// not found.
func (a *Alerts) Get(fp model.Fingerprint) (*alert.Alert, error) {
	a.Lock()
	defer a.Unlock()

	alrt, prs := a.alerts[fp]
	if !prs {
		return nil, ErrNotFound
	}
	return alrt, nil
}

// Set unconditionally sets the alert in memory.
func (a *Alerts) Set(alrt *alert.Alert) error {
	a.Lock()
	defer a.Unlock()

	return a.setLocked(alrt)
}

// setLocked admits the alert into the store, enforcing the per-alert-name
// limit. The caller must hold the store lock.
func (a *Alerts) setLocked(alrt *alert.Alert) error {
	if a.destroyed {
		return ErrDestroyed
	}

	fp := alrt.Fingerprint()
	name := alrt.Name()

	// Apply per alert limits if necessary
	if a.perAlertLimit > 0 {
		bucket, ok := a.limits[name]
		if !ok {
			bucket = limit.NewBucket[model.Fingerprint](a.perAlertLimit)
			a.limits[name] = bucket
		}
		if !bucket.Upsert(fp, alrt.EndsAt) {
			return ErrLimited
		}
	}

	a.alerts[fp] = alrt

	// Maintain the invariant that a fingerprint is never held in both the
	// store and the overflow queue: a direct admission (for example after
	// bucket eviction freed capacity) cancels its retained state.
	if queue, ok := a.pending[name]; ok {
		delete(queue, fp)
		if len(queue) == 0 {
			delete(a.pending, name)
		}
	}
	return nil
}

// SetPending retains the latest state of an alert that could not be admitted
// because the per-alert-name limit was reached. Repeated reports of an
// already pending fingerprint replace its state but keep its original queue
// position, so the instance is neither duplicated nor reordered.
func (a *Alerts) SetPending(alrt *alert.Alert, at time.Time) {
	a.Lock()
	defer a.Unlock()

	if a.pending == nil {
		a.pending = make(map[string]map[model.Fingerprint]*pendingAlert)
	}
	name := alrt.Name()
	queue, ok := a.pending[name]
	if !ok {
		queue = make(map[model.Fingerprint]*pendingAlert)
		a.pending[name] = queue
	}
	fp := alrt.Fingerprint()
	if p, exists := queue[fp]; exists {
		p.alert = alrt
		return
	}
	queue[fp] = &pendingAlert{
		alert:     alrt,
		firstSeen: at,
	}
}

// GetPending returns the latest state of the fingerprint while it is held in
// overflow, or false if it is not pending.
func (a *Alerts) GetPending(name string, fp model.Fingerprint) (*alert.Alert, bool) {
	a.Lock()
	defer a.Unlock()

	queue, ok := a.pending[name]
	if !ok {
		return nil, false
	}
	p, ok := queue[fp]
	if !ok {
		return nil, false
	}
	return p.alert, true
}

// PendingLen returns the number of alert instances currently held in
// overflow.
func (a *Alerts) PendingLen() int {
	a.Lock()
	defer a.Unlock()

	n := 0
	for _, queue := range a.pending {
		n += len(queue)
	}
	return n
}

// PromotePending tries to admit overflow alerts into the store using capacity
// that has been freed. Each alert name is reconciled independently in stable
// first-in-first-out order (firstSeen, then fingerprint); overflow alerts that
// have expired while waiting are dropped. Admitted alerts are returned exactly
// once so the caller can run its regular post-admission processing.
func (a *Alerts) PromotePending(now time.Time) []*alert.Alert {
	a.Lock()
	defer a.Unlock()

	if len(a.pending) == 0 {
		return nil
	}

	names := make([]string, 0, len(a.pending))
	for name := range a.pending {
		names = append(names, name)
	}
	slices.Sort(names)

	var admitted []*alert.Alert
	for _, name := range names {
		queue := a.pending[name]

		entries := make([]*pendingAlert, 0, len(queue))
		for _, p := range queue {
			entries = append(entries, p)
		}
		slices.SortFunc(entries, func(x, y *pendingAlert) int {
			if x.firstSeen.Before(y.firstSeen) {
				return -1
			}
			if x.firstSeen.After(y.firstSeen) {
				return 1
			}
			switch {
			case x.alert.Fingerprint() < y.alert.Fingerprint():
				return -1
			case x.alert.Fingerprint() > y.alert.Fingerprint():
				return 1
			default:
				return 0
			}
		})

		for _, p := range entries {
			fp := p.alert.Fingerprint()

			// The instance resolved before capacity freed up: it was never
			// admitted while firing, so drop it instead of churning the
			// notification pipeline.
			if isExpired(p.alert, now) {
				delete(queue, fp)
				continue
			}

			if err := a.setLocked(p.alert); err != nil {
				// The name is still full (or the store was destroyed): the
				// remaining entries keep their positions for a later attempt.
				break
			}

			delete(queue, fp)
			admitted = append(admitted, p.alert)
		}

		if len(queue) == 0 {
			delete(a.pending, name)
		}
	}

	if len(a.pending) == 0 {
		a.pending = nil
	}

	return admitted
}

// DeleteIfNotModified deletes the slice of Alerts from the store if not
// modified.
func (a *Alerts) DeleteIfNotModified(alerts alert.AlertSlice, destroyIfEmpty bool) error {
	a.Lock()
	defer a.Unlock()
	for _, alrt := range alerts {
		fp := alrt.Fingerprint()
		if other, ok := a.alerts[fp]; ok && alrt.UpdatedAt.Equal(other.UpdatedAt) {
			delete(a.alerts, fp)
		}
		// Explicit deletion also cancels any retained overflow state.
		if queue, ok := a.pending[alrt.Name()]; ok {
			delete(queue, fp)
			if len(queue) == 0 {
				delete(a.pending, alrt.Name())
			}
		}
	}

	// If the store is now empty, mark it as destroyed
	if len(a.alerts) == 0 && destroyIfEmpty {
		a.destroyed = true
	}

	return nil
}

// List returns a slice of Alerts currently held in memory.
func (a *Alerts) List() []*alert.Alert {
	a.Lock()
	defer a.Unlock()

	alerts := make([]*alert.Alert, 0, len(a.alerts))
	for _, alrt := range a.alerts {
		alerts = append(alerts, alrt)
	}

	return alerts
}

// Empty returns true if the store is empty.
func (a *Alerts) Empty() bool {
	a.Lock()
	defer a.Unlock()

	return len(a.alerts) == 0
}

// Empty returns true if the store is empty.
func (a *Alerts) Destroyed() bool {
	a.Lock()
	defer a.Unlock()

	return a.destroyed
}

// Len returns the number of alerts in the store.
func (a *Alerts) Len() int {
	a.Lock()
	defer a.Unlock()

	return len(a.alerts)
}
