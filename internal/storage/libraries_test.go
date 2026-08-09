package storage

import (
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLibraryPersistenceIsolationAndStableOrdering(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	for _, user := range []User{
		{ID: "owner-a", Username: "alice", PasswordHash: "hash"},
		{ID: "owner-b", Username: "bob", PasswordHash: "hash"},
	} {
		if err := store.CreateUser(t.Context(), user, now); err != nil {
			t.Fatalf("CreateUser(%s): %v", user.ID, err)
		}
	}

	first, created, err := store.CreateLibrary(t.Context(), Library{
		ID: "00000000-0000-4000-8000-000000000002", OwnerUserID: "owner-a", Name: "A\u030Angstrom",
	}, now)
	if err != nil || !created || first.Name != "Ångstrom" || first.HeadCommitID != nil || first.HeadVersion != 0 {
		t.Fatalf("first CreateLibrary = %+v, %v, %v", first, created, err)
	}
	replayed, created, err := store.CreateLibrary(t.Context(), Library{
		ID: first.ID, OwnerUserID: "owner-a", Name: "Ångstrom",
	}, now.Add(time.Hour))
	if err != nil || created || replayed != first {
		t.Fatalf("replayed CreateLibrary = %+v, %v, %v; want %+v, false, nil", replayed, created, err, first)
	}
	if _, _, err := store.CreateLibrary(t.Context(), Library{
		ID: first.ID, OwnerUserID: "owner-a", Name: "different",
	}, now); !errors.Is(err, ErrLibraryObjectConflict) {
		t.Fatalf("same ID different name error = %v, want ErrLibraryObjectConflict", err)
	}
	if _, _, err := store.CreateLibrary(t.Context(), Library{
		ID: "00000000-0000-4000-8000-000000000003", OwnerUserID: "owner-a", Name: "A\u030Angstrom",
	}, now); !errors.Is(err, ErrLibraryExists) {
		t.Fatalf("canonical duplicate name error = %v, want ErrLibraryExists", err)
	}
	if _, _, err := store.CreateLibrary(t.Context(), Library{
		ID: "00000000-0000-4000-8000-000000000001", OwnerUserID: "owner-a", Name: "first",
	}, now); err != nil {
		t.Fatalf("CreateLibrary ordered first: %v", err)
	}
	other, created, err := store.CreateLibrary(t.Context(), Library{
		ID: "00000000-0000-4000-8000-000000000001", OwnerUserID: "owner-b", Name: "private",
	}, now)
	if err != nil || !created || other.OwnerUserID != "owner-b" {
		t.Fatalf("cross-owner CreateLibrary = %+v, %v, %v; want independent library", other, created, err)
	}

	libraries, err := store.ListLibraries(t.Context(), "owner-a", time.Time{}, "", 10)
	if err != nil {
		t.Fatalf("ListLibraries: %v", err)
	}
	if len(libraries) != 2 || libraries[0].ID != "00000000-0000-4000-8000-000000000001" || libraries[1].ID != first.ID {
		t.Fatalf("ordered libraries = %+v", libraries)
	}
	page, err := store.ListLibraries(t.Context(), "owner-a", libraries[0].CreatedAt, libraries[0].ID, 10)
	if err != nil || len(page) != 1 || page[0].ID != first.ID {
		t.Fatalf("cursor page = %+v, %v", page, err)
	}
	if _, err := store.GetLibrary(t.Context(), "owner-b", first.ID); !errors.Is(err, ErrLibraryNotFound) {
		t.Fatalf("cross-owner GetLibrary error = %v, want ErrLibraryNotFound", err)
	}
}

func TestUpdateLibraryHeadUsesAtomicCompareAndSwap(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "owner", Username: "alice", PasswordHash: "hash"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	library, _, err := store.CreateLibrary(t.Context(), Library{ID: "library", OwnerUserID: "owner", Name: "library"}, now)
	if err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}

	start := make(chan struct{})
	var successes atomic.Int32
	var conflicts atomic.Int32
	var wait sync.WaitGroup
	for _, commitID := range []string{"commit-a", "commit-b"} {
		wait.Go(func() {
			<-start
			_, err := store.UpdateLibraryHead(t.Context(), "owner", "library", nil, library.HeadVersion, commitID, nil, now.Add(time.Second))
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, ErrHeadConflict):
				conflicts.Add(1)
			default:
				t.Errorf("UpdateLibraryHead: %v", err)
			}
		})
	}
	close(start)
	wait.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("CAS results = %d successes/%d conflicts", successes.Load(), conflicts.Load())
	}
	current, err := store.GetLibrary(t.Context(), "owner", "library")
	if err != nil || current.HeadVersion != 1 || current.HeadCommitID == nil {
		t.Fatalf("current library = %+v, %v", current, err)
	}
	if _, err := store.UpdateLibraryHead(t.Context(), "owner", "library", current.HeadCommitID, 0, "commit-c", nil, now); !errors.Is(err, ErrHeadConflict) {
		t.Fatalf("stale version update error = %v, want ErrHeadConflict", err)
	}
}

func TestUpdateLibraryHeadMaintainsPublishedCommitsAtomically(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	for _, user := range []User{
		{ID: "owner-a", Username: "alice", PasswordHash: "hash"},
		{ID: "owner-b", Username: "bob", PasswordHash: "hash"},
	} {
		if err := store.CreateUser(t.Context(), user, now); err != nil {
			t.Fatalf("CreateUser(%s): %v", user.ID, err)
		}
		if _, _, err := store.CreateLibrary(t.Context(), Library{ID: "library", OwnerUserID: user.ID, Name: "library"}, now); err != nil {
			t.Fatalf("CreateLibrary(%s): %v", user.ID, err)
		}
	}

	updated, err := store.UpdateLibraryHead(t.Context(), "owner-a", "library", nil, 0, "candidate", []string{"branch-a", "branch-b"}, now)
	if err != nil {
		t.Fatalf("UpdateLibraryHead: %v", err)
	}
	for _, commitID := range []string{"candidate", "branch-a", "branch-b"} {
		published, err := store.IsCommitPublished(t.Context(), "owner-a", "library", commitID)
		if err != nil || !published {
			t.Fatalf("IsCommitPublished(%s) = %v, %v; want true, nil", commitID, published, err)
		}
		otherPublished, err := store.IsCommitPublished(t.Context(), "owner-b", "library", commitID)
		if err != nil || otherPublished {
			t.Fatalf("other owner IsCommitPublished(%s) = %v, %v; want false, nil", commitID, otherPublished, err)
		}
	}

	if _, err := store.UpdateLibraryHead(t.Context(), "owner-a", "library", updated.HeadCommitID, 0, "stale", []string{"stale-branch"}, now); !errors.Is(err, ErrHeadConflict) {
		t.Fatalf("stale UpdateLibraryHead error = %v, want ErrHeadConflict", err)
	}
	for _, commitID := range []string{"stale", "stale-branch"} {
		published, err := store.IsCommitPublished(t.Context(), "owner-a", "library", commitID)
		if err != nil || published {
			t.Fatalf("CAS failure published %s = %v, %v", commitID, published, err)
		}
	}

	if _, err := store.DB().ExecContext(t.Context(), `
		CREATE TRIGGER fail_published_commit BEFORE INSERT ON published_commits
		WHEN NEW.commit_id = 'fail-insert'
		BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	if _, err := store.UpdateLibraryHead(t.Context(), "owner-a", "library", updated.HeadCommitID, updated.HeadVersion, "rolled-back", []string{"fail-insert"}, now); err == nil {
		t.Fatal("published commit insert failure unexpectedly succeeded")
	}
	current, err := store.GetLibrary(t.Context(), "owner-a", "library")
	if err != nil || current.HeadVersion != updated.HeadVersion || current.HeadCommitID == nil || *current.HeadCommitID != "candidate" {
		t.Fatalf("Head after insert rollback = %+v, %v", current, err)
	}
	published, err := store.IsCommitPublished(t.Context(), "owner-a", "library", "rolled-back")
	if err != nil || published {
		t.Fatalf("rolled-back candidate published = %v, %v", published, err)
	}
}

func TestFindActiveSession(t *testing.T) {
	store := openAccountStore(t)
	defer closeAccountStore(t, store)
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := store.CreateUser(t.Context(), User{ID: "owner", Username: "alice", PasswordHash: "hash"}, now); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	active := sha256.Sum256([]byte("active"))
	if err := store.CreateSession(t.Context(), "owner", active, "device", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if userID, err := store.FindActiveSession(t.Context(), active, now); err != nil || userID != "owner" {
		t.Fatalf("FindActiveSession = %q, %v", userID, err)
	}
	if _, err := store.FindActiveSession(t.Context(), active, now.Add(2*time.Hour)); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expired FindActiveSession error = %v, want ErrSessionNotFound", err)
	}
}
