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
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/alertmanager/alert"
)

func TestSetGet(t *testing.T) {
	a := NewAlerts()
	alrt := alert.New(model.Alert{}, time.Now(), false)
	require.NoError(t, a.Set(alrt))
	want := alrt.Fingerprint()
	got, err := a.Get(want)

	require.NoError(t, err)
	require.Equal(t, want, got.Fingerprint())
}

func TestDeleteIfNotModified(t *testing.T) {
	t.Run("unmodified alert should be deleted", func(t *testing.T) {
		a := NewAlerts()
		a1 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"foo": "bar",
			},
		}, time.Now().Add(-time.Second), false)
		require.NoError(t, a.Set(a1))

		// a1 should be deleted as it has not been modified.
		a.DeleteIfNotModified(alert.AlertSlice{a1}, false)
		got, err := a.Get(a1.Fingerprint())
		require.Equal(t, ErrNotFound, err)
		require.Nil(t, got)
	})

	t.Run("modified alert should not be deleted", func(t *testing.T) {
		a := NewAlerts()
		a1 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"foo": "bar",
			},
		}, time.Now(), false)
		require.NoError(t, a.Set(a1))

		// Make a copy of a1 that is older, but do not put it.
		// We want to make sure a1 is not deleted.
		a2 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"foo": "bar",
			},
		}, time.Now().Add(-time.Second), false)
		require.True(t, a2.UpdatedAt.Before(a1.UpdatedAt))
		a.DeleteIfNotModified(alert.AlertSlice{a2}, false)
		// a1 should not be deleted.
		got, err := a.Get(a1.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a1, got)

		// Make another copy of a1 that is older, but do not put it.
		// We want to make sure a2 is not deleted here either.
		a3 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"foo": "bar",
			},
		}, time.Now().Add(time.Second), false)
		require.True(t, a3.UpdatedAt.After(a1.UpdatedAt))
		a.DeleteIfNotModified(alert.AlertSlice{a3}, false)
		// a1 should not be deleted.
		got, err = a.Get(a1.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a1, got)
	})

	t.Run("should not delete other alerts", func(t *testing.T) {
		a := NewAlerts()
		a1 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"foo": "bar",
			},
		}, time.Now(), false)
		a2 := alert.New(model.Alert{
			Labels: model.LabelSet{
				"bar": "baz",
			},
		}, time.Now(), false)
		require.NoError(t, a.Set(a1))
		require.NoError(t, a.Set(a2))

		// Deleting a1 should not delete a2.
		require.NoError(t, a.DeleteIfNotModified(alert.AlertSlice{a1}, true))
		// a1 should be deleted.
		got, err := a.Get(a1.Fingerprint())
		require.Equal(t, ErrNotFound, err)
		require.False(t, a.Destroyed())
		require.Nil(t, got)
		// a2 should not be deleted.
		got, err = a.Get(a2.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a2, got)
	})
}

func TestGC(t *testing.T) {
	now := time.Now()
	newAlert := func(key string, start, end time.Duration) *alert.Alert {
		return alert.New(model.Alert{
			Labels:   model.LabelSet{model.LabelName(key): "b"},
			StartsAt: now.Add(start * time.Minute),
			EndsAt:   now.Add(end * time.Minute),
		}, time.Time{}, false)
	}
	active := []*alert.Alert{
		newAlert("b", 10, 20),
		newAlert("c", -10, 10),
	}
	resolved := []*alert.Alert{
		newAlert("a", -10, -5),
		newAlert("d", -10, -1),
	}
	s := NewAlerts()
	var (
		n           int
		done        = make(chan struct{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	s.SetGCCallback(func(a []*alert.Alert) {
		n += len(a)
		if n >= len(resolved) {
			cancel()
		}
	})
	for _, alrt := range append(active, resolved...) {
		require.NoError(t, s.Set(alrt))
	}
	go func() {
		s.Run(ctx, 10*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("garbage collection didn't complete in time")
	}

	for _, alrt := range active {
		if _, err := s.Get(alrt.Fingerprint()); err != nil {
			t.Errorf("alert %v should not have been gc'd", alrt)
		}
	}
	for _, alrt := range resolved {
		if _, err := s.Get(alrt.Fingerprint()); err == nil {
			t.Errorf("alert %v should have been gc'd", alrt)
		}
	}
	require.Len(t, resolved, n)
}

func newLimitedAlert(t *testing.T, instance string, start, end time.Time) *alert.Alert {
	t.Helper()
	return alert.New(model.Alert{
		Labels: model.LabelSet{
			model.AlertNameLabel: "TestAlert",
			"instance":           model.LabelValue(instance),
		},
		StartsAt: start,
		EndsAt:   end,
	}, start, false)
}

func TestSetPendingWithoutLimit(t *testing.T) {
	// With no limit configured every alert is admitted, so the overflow
	// queue stays empty and the legacy behavior is preserved.
	s := NewAlerts()
	now := time.Now()
	alrt := newLimitedAlert(t, "a", now, now.Add(time.Hour))

	require.NoError(t, s.Set(alrt))
	require.Equal(t, 0, s.PendingLen())

	s.SetPending(alrt, now)
	require.Equal(t, 1, s.PendingLen())
	admitted := s.PromotePending(now)
	require.Len(t, admitted, 1)
	require.Equal(t, alrt.Fingerprint(), admitted[0].Fingerprint())
	require.Equal(t, 0, s.PendingLen())
}

func TestPendingReconciliationStableOrder(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(2)

	// The two admitted instances occupy the full capacity and expire shortly
	// after the test starts.
	active1 := newLimitedAlert(t, "active1", now, now.Add(300*time.Millisecond))
	active2 := newLimitedAlert(t, "active2", now, now.Add(300*time.Millisecond))
	require.NoError(t, s.Set(active1))
	require.NoError(t, s.Set(active2))

	// The first two queued instances outlive the first reconciliation round;
	// the third stays fresh for the second round.
	first := newLimitedAlert(t, "first", now, now.Add(1200*time.Millisecond))
	second := newLimitedAlert(t, "second", now, now.Add(1200*time.Millisecond))
	third := newLimitedAlert(t, "third", now, now.Add(time.Hour))

	require.ErrorIs(t, s.Set(first), ErrLimited)
	require.ErrorIs(t, s.Set(second), ErrLimited)
	require.ErrorIs(t, s.Set(third), ErrLimited)

	// Queue entries in reverse arrival order to prove that ordering follows
	// the firstSeen timestamp rather than map iteration or insertion order.
	s.SetPending(third, now.Add(3*time.Second))
	s.SetPending(second, now.Add(2*time.Second))
	s.SetPending(first, now.Add(1*time.Second))
	require.Equal(t, 3, s.PendingLen())

	// While the capacity is occupied nothing can be admitted.
	require.Empty(t, s.PromotePending(now))
	require.Equal(t, 3, s.PendingLen())

	// After the active instances expire, the two oldest queued instances are
	// admitted in first-in-first-out order; the third must keep waiting.
	time.Sleep(400 * time.Millisecond)
	require.Equal(t, []model.Fingerprint{first.Fingerprint(), second.Fingerprint()}, fps(s.PromotePending(time.Now())))
	require.Equal(t, 1, s.PendingLen())
	if _, err := s.Get(first.Fingerprint()); err != nil {
		t.Fatalf("first alert should have been admitted: %v", err)
	}
	if _, err := s.Get(second.Fingerprint()); err != nil {
		t.Fatalf("second alert should have been admitted: %v", err)
	}

	// Once the previously admitted instances expire too, the last queued
	// instance is admitted rather than being lost.
	time.Sleep(1100 * time.Millisecond)
	require.Equal(t, []model.Fingerprint{third.Fingerprint()}, fps(s.PromotePending(time.Now())))
	require.Equal(t, 0, s.PendingLen())
}

func TestPendingRepeatedReportsKeepPositionAndState(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(1)

	active := newLimitedAlert(t, "active", now, now.Add(time.Hour))
	require.NoError(t, s.Set(active))

	queued := newLimitedAlert(t, "queued", now, now.Add(time.Hour))
	require.ErrorIs(t, s.Set(queued), ErrLimited)
	s.SetPending(queued, now)
	require.Equal(t, 1, s.PendingLen())

	// A repeated report replaces the latest state but must not create a
	// duplicate entry or change the queue position.
	updated := newLimitedAlert(t, "queued", now, now.Add(2*time.Hour))
	updated.UpdatedAt = now.Add(time.Second)
	s.SetPending(updated, now.Add(time.Second))
	require.Equal(t, 1, s.PendingLen())

	got, ok := s.GetPending(queued.Name(), queued.Fingerprint())
	require.True(t, ok)
	require.Equal(t, updated.EndsAt, got.EndsAt)
}

func TestPendingExpiredEntriesAreDropped(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(1)

	active := newLimitedAlert(t, "active", now, now.Add(time.Hour))
	require.NoError(t, s.Set(active))

	expired := newLimitedAlert(t, "expired", now.Add(-2*time.Hour), now.Add(-time.Hour))
	firing := newLimitedAlert(t, "firing", now, now.Add(time.Hour))
	s.SetPending(expired, now.Add(-2*time.Hour))
	s.SetPending(firing, now)
	require.Equal(t, 2, s.PendingLen())

	// Reconciliation drops the expired instance without admitting it and
	// keeps the firing instance queued because the slot is still occupied.
	require.Empty(t, s.PromotePending(now))
	require.Equal(t, 1, s.PendingLen())
	_, ok := s.GetPending(expired.Name(), expired.Fingerprint())
	require.False(t, ok)
}

func TestGCPrunesExpiredPendingEntries(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(1)

	expired := newLimitedAlert(t, "expired", now.Add(-2*time.Hour), now.Add(-time.Hour))
	firing := newLimitedAlert(t, "firing", now, now.Add(time.Hour))
	s.SetPending(expired, now.Add(-2*time.Hour))
	s.SetPending(firing, now)

	s.GC()

	require.Equal(t, 1, s.PendingLen())
	_, ok := s.GetPending(firing.Name(), firing.Fingerprint())
	require.True(t, ok)
	_, ok = s.GetPending(expired.Name(), expired.Fingerprint())
	require.False(t, ok)
}

func TestSetAdmissionClearsPendingState(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(1)

	queued := newLimitedAlert(t, "queued", now, now.Add(time.Hour))
	s.SetPending(queued, now)
	require.Equal(t, 1, s.PendingLen())

	// With no competing entry the same fingerprint is admitted directly and
	// must never exist in both the store and the overflow queue.
	require.NoError(t, s.Set(queued))
	require.Equal(t, 0, s.PendingLen())
	if _, err := s.Get(queued.Fingerprint()); err != nil {
		t.Fatalf("queued alert should have been admitted: %v", err)
	}
}

func TestDeleteIfNotModifiedRemovesPendingState(t *testing.T) {
	now := time.Now()
	s := NewAlerts().WithPerAlertLimit(1)

	queued := newLimitedAlert(t, "queued", now, now.Add(time.Hour))
	s.SetPending(queued, now)
	require.Equal(t, 1, s.PendingLen())

	require.NoError(t, s.DeleteIfNotModified(alert.AlertSlice{queued}, false))
	require.Equal(t, 0, s.PendingLen())
}

func fps(alerts []*alert.Alert) []model.Fingerprint {
	out := make([]model.Fingerprint, 0, len(alerts))
	for _, alrt := range alerts {
		out = append(out, alrt.Fingerprint())
	}
	return out
}
