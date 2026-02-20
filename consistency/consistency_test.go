package consistency

import (
	"context"
	"testing"
	"time"
)

func TestTrackFolderAutoEvicts(t *testing.T) {
	uuid := "evict-test"
	TrackFolder(uuid)

	if _, ok := recentFolders.Load(uuid); !ok {
		t.Fatal("expected folder to be tracked immediately after TrackFolder")
	}

	time.Sleep(window + 50*time.Millisecond)

	if _, ok := recentFolders.Load(uuid); ok {
		t.Error("expected folder to be evicted after consistency window")
	}
}

func TestAwaitFolder(t *testing.T) {
	t.Run("returns immediately for unknown folder", func(t *testing.T) {
		start := time.Now()
		err := AwaitFolder(context.Background(), "unknown-uuid")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Since(start) > 10*time.Millisecond {
			t.Error("expected immediate return for unknown folder")
		}
	})

	t.Run("returns immediately when window has elapsed", func(t *testing.T) {
		uuid := "elapsed-uuid"
		recentFolders.Store(uuid, time.Now().Add(-window))
		defer recentFolders.Delete(uuid)

		start := time.Now()
		err := AwaitFolder(context.Background(), uuid)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Since(start) > 10*time.Millisecond {
			t.Error("expected immediate return when window has elapsed")
		}
	})

	t.Run("waits remaining time for recent folder", func(t *testing.T) {
		uuid := "recent-uuid"
		recentFolders.Store(uuid, time.Now())
		defer recentFolders.Delete(uuid)

		start := time.Now()
		err := AwaitFolder(context.Background(), uuid)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed < 400*time.Millisecond {
			t.Errorf("expected to wait ~500ms, only waited %v", elapsed)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		uuid := "cancel-uuid"
		recentFolders.Store(uuid, time.Now())
		defer recentFolders.Delete(uuid)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := AwaitFolder(ctx, uuid)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
