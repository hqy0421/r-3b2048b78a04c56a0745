package bolttable

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// positionAt is the file's Position read in one View.
func positionAt(t *testing.T, db *DB) Position {
	t.Helper()
	var p Position
	view(t, db, func(r *ReadTx) error {
		var err error
		p, err = r.Position()
		return err
	})
	return p
}

// originOf is the origin stored in the closed file at path.
func originOf(t *testing.T, path string) [16]byte {
	t.Helper()
	bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bdb.Close() }()
	var o [16]byte
	if err := bdb.View(func(btx *bbolt.Tx) error {
		var err error
		o, err = readOrigin(btx.Bucket([]byte(libBucket)))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return o
}

// commitKeys commits one revision per key in [from, to), each a distinct row
// so every write records a change.
func commitKeys(t *testing.T, db *DB, f *fixture, from, to uint64) {
	t.Helper()
	for k := from; k < to; k++ {
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, k, &item{N: int64(k)}) })
	}
}

func TestPositionIsStableAcrossReopenAndPagesForward(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 10})
	commitKeys(t, db, f, 1, 4)
	p := positionAt(t, db)
	if p.Origin == ([16]byte{}) {
		t.Fatal("Position has a zero Origin")
	}
	if p.Rev != 3 {
		t.Fatalf("Position.Rev = %d, want 3", p.Rev)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{Retention: 10})
	commitKeys(t, db, f, 4, 6)
	if got := positionAt(t, db); got.Origin != p.Origin {
		t.Errorf("Origin after reopen = %x, want %x", got.Origin, p.Origin)
	}
	view(t, db, func(r *ReadTx) error {
		page, err := r.SinceAt(p, 100)
		if err != nil || page.Full || len(page.Changes) != 2 || page.Changes[0].Rev != 4 || page.More {
			t.Errorf("SinceAt after reopen = %+v, %v; want revisions 4 and 5", page, err)
		}
		return nil
	})
}

func TestPositionSnapshotAndDeltaDoNotOverlap(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	// Snapshot: read the rows and the Position in one transaction. A commit
	// lands while the snapshot is open, between reading the rows and reading
	// the position. The snapshot does not see its row, and the position does
	// not include its revision: the delta holds exactly the commits after
	// the snapshot, so applying them never repeats or drops one.
	var p Position
	committed := make(chan error, 1)
	ctx := t.Context()
	view(t, db, func(r *ReadTx) error {
		var rows int
		if err := f.items.Scan(r, Range{}, func(uint64, *item) (bool, error) {
			rows++
			return true, nil
		}); err != nil {
			return err
		}
		if rows != 0 {
			t.Errorf("snapshot saw %d rows, want 0", rows)
		}
		go func() {
			committed <- db.Update(ctx, func(tx *Tx) error {
				return f.items.Put(tx, 99, &item{})
			})
		}()
		if err := <-committed; err != nil {
			t.Fatalf("concurrent Update: %v", err)
		}
		var err error
		p, err = r.Position()
		return err
	})
	if p.Rev != 0 {
		t.Fatalf("Position.Rev = %d, want 0: the snapshot predates the commit", p.Rev)
	}
	// A later commit is the only delta.
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	view(t, db, func(r *ReadTx) error {
		page, err := r.SinceAt(p, 100)
		if err != nil || page.Full {
			t.Fatalf("SinceAt = %+v, %v", page, err)
		}
		var revs []uint64
		for _, c := range page.Changes {
			revs = append(revs, c.Rev)
		}
		if want := []uint64{1, 2}; !slices.Equal(revs, want) {
			t.Errorf("deltas after the snapshot = %v, want %v", revs, want)
		}
		return nil
	})
}

func TestSinceAtRefusesAnotherFilesPosition(t *testing.T) {
	f1 := newFixture(t)
	first := mustOpen(t, tempPath(t), f1.schema, Options{Retention: 100})
	commitKeys(t, first, f1, 1, 6)
	foreign := positionAt(t, first)

	// The other file is ahead of the foreign position's revision, so only its
	// origin tells the two apart.
	f2 := newFixture(t)
	second := mustOpen(t, tempPath(t), f2.schema, Options{Retention: 100})
	commitKeys(t, second, f2, 1, 10)
	if headRev(t, second) <= foreign.Rev {
		t.Fatalf("the second file's head %d does not exceed %d", headRev(t, second), foreign.Rev)
	}
	view(t, second, func(r *ReadTx) error {
		page, err := r.SinceAt(foreign, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !page.Full || page.Changes != nil || page.More {
			t.Errorf("SinceAt from another file = %+v, want a Full page", page)
		}
		// The revision after foreign.Rev on this file is answerable when
		// asked by revision, which does not compare origins.
		if page, err := r.Since(foreign.Rev, 100); err != nil || page.Full || len(page.Changes) != 4 {
			t.Errorf("Since(%d) = %+v, %v; want 4 changes", foreign.Rev, page, err)
		}
		return nil
	})
}

func TestSinceAtRefusesZeroAndAboveHead(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	commitKeys(t, db, f, 1, 4)
	p := positionAt(t, db)
	view(t, db, func(r *ReadTx) error {
		for name, bad := range map[string]Position{
			"zero":       {},
			"above head": {Origin: p.Origin, Rev: p.Rev + 1},
		} {
			page, err := r.SinceAt(bad, 100)
			if err != nil {
				t.Fatalf("%s: SinceAt: %v", name, err)
			}
			if !page.Full || page.Changes != nil {
				t.Errorf("%s: SinceAt = %+v, want a Full page", name, page)
			}
		}
		return nil
	})
}

func TestSinceAtRefusesTrimmedPosition(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 3})
	commitKeys(t, db, f, 1, 3)
	p := positionAt(t, db)
	// Trim past p: head 6, retention 3 keeps 4 to 6, so p.Rev 2 is gone.
	commitKeys(t, db, f, 3, 7)
	view(t, db, func(r *ReadTx) error {
		page, err := r.SinceAt(p, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !page.Full || page.Changes != nil {
			t.Errorf("SinceAt from a trimmed position = %+v, want a Full page", page)
		}
		return nil
	})
}

func TestSinceAtPagesWithinRetention(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	commitKeys(t, db, f, 1, 11)
	var p Position
	view(t, db, func(r *ReadTx) error {
		var err error
		if p, err = r.Position(); err != nil {
			return err
		}
		p.Rev = 2
		return nil
	})
	view(t, db, func(r *ReadTx) error {
		var revs []uint64
		for {
			page, err := r.SinceAt(p, 3)
			if err != nil || page.Full {
				t.Fatalf("SinceAt(%+v, 3) = %+v, %v", p, page, err)
			}
			for _, c := range page.Changes {
				revs = append(revs, c.Rev)
			}
			if !page.More {
				break
			}
			p.Rev = page.Changes[len(page.Changes)-1].Rev
		}
		if want := []uint64{3, 4, 5, 6, 7, 8, 9, 10}; !slices.Equal(revs, want) {
			t.Errorf("pages = %v, want %v", revs, want)
		}
		return nil
	})
}

func TestPositionAfterTransactionFails(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	var kept *ReadTx
	view(t, db, func(r *ReadTx) error { kept = r; return nil })
	if p, err := kept.Position(); !errors.Is(err, errTxClosed) {
		t.Errorf("Position after View returned = %+v, %v; want the transaction-ended error", p, err)
	}
}

func TestPositionReportsCorruptOrigin(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	undo := errors.New("undo")
	err := db.Update(t.Context(), func(tx *Tx) error {
		if err := tx.read.btx.Bucket([]byte(libBucket)).Put(keyOrigin, []byte{1, 2, 3}); err != nil {
			return err
		}
		if p, err := tx.Read().Position(); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Position over a 3-byte origin = %+v, %v; want ErrCorrupt", p, err)
		}
		return undo
	})
	if err != undo {
		t.Fatalf("Update = %v, want the rollback error", err)
	}
}

func TestPositionInsideInspectSeesNoOrigin(t *testing.T) {
	// A file Open will stamp: an origin-less file created by an earlier
	// build. Inspect reads it before Open completes the library bucket.
	path := tempPath(t)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte(libBucket)).Delete(keyOrigin)
	})
	var zero bool
	db = mustOpen(t, path, f.schema, Options{
		Inspect: func(r *ReadTx) error {
			p, err := r.Position()
			if err != nil {
				return err
			}
			zero = p.Origin == ([16]byte{})
			return nil
		},
	})
	if !zero {
		t.Error("Position inside Inspect of an origin-less file had a non-zero Origin")
	}
	if p := positionAt(t, db); p.Origin == ([16]byte{}) {
		t.Error("the opened file has a zero Origin after Open stamped it")
	}
}

func TestBackupCarriesOriginAndRebuildsResume(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 100})
	commitKeys(t, db, f, 1, 5)
	p := positionAt(t, db)
	copyPath := filepath.Join(t.TempDir(), "copy.bolt")
	if err := db.Backup(t.Context(), copyPath); err != nil {
		t.Fatal(err)
	}
	if got := originOf(t, copyPath); got != p.Origin {
		t.Errorf("backup origin = %x, want the original's %x", got, p.Origin)
	}
	// A restore opened from the backup resumes from p and sees the changes
	// the original commits after it.
	commitKeys(t, db, f, 5, 7)
	restored := mustOpen(t, copyPath, newFixture(t).schema, Options{Retention: 100})
	view(t, restored, func(r *ReadTx) error {
		page, err := r.SinceAt(p, 100)
		if err != nil || page.Full || len(page.Changes) != 0 {
			t.Errorf("SinceAt on the restore at p = %+v, %v; want an empty page", page, err)
		}
		return nil
	})
}

func TestCompactKeepsOrigin(t *testing.T) {
	path := sparseFile(t)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 100})
	before := positionAt(t, db).Origin
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, path, newFixture(t).schema, Options{Retention: 100, Compact: compactAt})
	if !db.OpenReport().Compacted {
		t.Fatal("the second Open did not compact")
	}
	if after := positionAt(t, db).Origin; after != before {
		t.Errorf("origin after compaction = %x, want %x", after, before)
	}
}

func TestResetMintsNewOrigin(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 100})
	commitKeys(t, db, f, 1, 4)
	before := positionAt(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A format change to namespace a, allowed to reset, empties it and the
	// revision head; the file names no revision before the reset.
	db = mustOpen(t, path, fixtureAt(t, 2).schema, Options{
		Retention: 100,
		Reset:     ResetPolicy{Allow: func(NamespaceDigest) bool { return true }},
	})
	if len(db.OpenReport().Reset) == 0 {
		t.Fatal("Open reset nothing")
	}
	after := positionAt(t, db)
	if after.Origin == before.Origin {
		t.Error("the reset file kept the old origin")
	}
	view(t, db, func(r *ReadTx) error {
		page, err := r.SinceAt(before, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !page.Full || page.Changes != nil {
			t.Errorf("SinceAt from before the reset = %+v, want a Full page", page)
		}
		return nil
	})
}

func TestOpenStampsOriginOnOlderFile(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 100})
	commitKeys(t, db, f, 1, 3)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte(libBucket)).Delete(keyOrigin)
	})
	db = mustOpen(t, path, newFixture(t).schema, Options{Retention: 100})
	p := positionAt(t, db)
	if p.Origin == ([16]byte{}) {
		t.Fatal("Open left an origin-less file without an origin")
	}
	if p.Rev != 2 {
		t.Errorf("Position.Rev = %d, want the preserved head 2", p.Rev)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// The stamped origin survives the next reopen.
	db = mustOpen(t, path, newFixture(t).schema, Options{Retention: 100})
	if got := positionAt(t, db); got.Origin != p.Origin {
		t.Errorf("Origin after a reopen = %x, want the stamped %x", got.Origin, p.Origin)
	}
}

// swapIn replaces live's file with the closed file at from, through a
// staged rename, the way a restore lands.
func swapIn(t *testing.T, live *Live, from, path string) {
	t.Helper()
	staged := path + ".staged"
	if err := os.Rename(from, staged); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
	defer cancel()
	if err := live.Swap(ctx, moveIn(staged, path)); err != nil {
		t.Fatalf("Swap: %v", err)
	}
}

func TestLiveSwapToAnotherFilesPositionRequiresRebuild(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	live := openLive(t, path, f.schema, Options{Retention: 100})
	if err := putName(t.Context(), live, f, "a"); err != nil {
		t.Fatal(err)
	}
	var p Position
	if err := live.View(t.Context(), func(r *ReadTx) error {
		var err error
		p, err = r.Position()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Another file with its own origin. Its revisions reach p.Rev and beyond,
	// so only the origin tells it apart from the file p came from.
	other := storedWith(t, newFixture(t), "b")
	swapIn(t, live, other, path)
	if err := live.View(t.Context(), func(r *ReadTx) error {
		if head, err := r.Rev(); err != nil || head < p.Rev {
			t.Fatalf("the swapped-in file's head = %d, %v; want at least %d", head, err, p.Rev)
		}
		page, err := r.SinceAt(p, 100)
		if err != nil {
			return err
		}
		if !page.Full {
			t.Errorf("SinceAt across a swap to another file = %+v, want Full", page)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLiveSwapSameFileResumes(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	live := openLive(t, path, f.schema, Options{Retention: 100})
	var p Position
	if err := live.Update(t.Context(), func(tx *Tx) error {
		return f.items.Put(tx, 1, &item{Name: "a"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := live.View(t.Context(), func(r *ReadTx) error {
		var err error
		p, err = r.Position()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A consistent copy of the same file lands under the same origin, so the
	// position resumes rather than demanding a rebuild.
	staged := path + ".staged2"
	if err := live.Backup(t.Context(), staged); err != nil {
		t.Fatal(err)
	}
	swapIn(t, live, staged, path)
	if err := live.View(t.Context(), func(r *ReadTx) error {
		page, err := r.SinceAt(p, 100)
		if err != nil {
			return err
		}
		if page.Full {
			t.Errorf("SinceAt across a swap of the same file = %+v, want a page", page)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEpochAndOriginAreDistinct(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 100})
	p := positionAt(t, db)
	if db.Epoch() == p.Origin {
		t.Error("a fresh open's Epoch equals its origin")
	}
}

func TestOpenReadOnlyOfOriginlessFile(t *testing.T) {
	// An older build's file carries no origin. OpenReadOnly writes nothing, so
	// it leaves the file unstamped and its Position answers a zero Origin,
	// which no SinceAt accepts: a reader resyncs until a writer's Open stamps
	// it.
	path := tempPath(t)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 10})
	commitKeys(t, db, f, 1, 3)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte(libBucket)).Delete(keyOrigin)
	})
	rdb := mustOpenReadOnly(t, path, newFixture(t).schema, ReadOptions{Retention: 10})
	if err := rdb.View(t.Context(), func(r *ReadTx) error {
		p, err := r.Position()
		if err != nil {
			return err
		}
		if p.Origin != ([16]byte{}) || p.Rev != 2 {
			t.Errorf("Position = %+v, want a zero Origin and Rev 2", p)
		}
		if page, err := r.SinceAt(p, 100); err != nil || !page.Full {
			t.Errorf("SinceAt at a zero Origin = %+v, %v; want Full", page, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if got := originOf(t, path); got != ([16]byte{}) {
		t.Errorf("OpenReadOnly stamped an origin %x", got)
	}
}
