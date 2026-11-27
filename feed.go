package bolttable

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

// ChangePage is one page of the change log.
type ChangePage struct {
	// Changes are in revision order; empty, never nil, unless Full.
	Changes []Change
	// Full reports that the feed cannot answer from what was asked: Since was
	// asked after a change this file has trimmed (after+1 is below the oldest
	// retained revision) or after one above the head, and SinceAt from a
	// Position of another file, one this file has trimmed past, or one above
	// the head. Changes is then nil and More false, and a caller holding a
	// projection reloads it rather than applying a page.
	Full bool
	// More reports that retained changes follow the last one returned.
	More bool
}

// Position names a place in one file's change feed: the file's origin and a
// revision in it. Origin is a random identity Open stamps into the file, and
// a backup or a compaction carries it; a file moved aside as invalid, reset
// or created starts over with a new one. A Position from one file names
// nothing in another, even when the other's revisions reach its Rev, so a
// reader holding one across a Live's Swap learns to reload its projection
// instead of applying deltas that describe another file's rows. Its fields
// are its whole state, so a caller may store and compare it. The zero
// Position names no file, and every SinceAt refuses it.
type Position struct {
	Origin [16]byte
	Rev    uint64
}

// Since returns at most limit retained changes with Rev above after, in
// order. limit below 1 is an error. To continue, call again with after set
// to the last returned Rev; pages read in one transaction agree, and a later
// transaction may find the log trimmed past after, which it reports as Full.
// Since compares revisions only: a caller resuming across a break or a Swap
// holds a Position and calls SinceAt instead, which also checks the file.
func (r *ReadTx) Since(after uint64, limit int) (ChangePage, error) {
	if limit < 1 {
		return ChangePage{}, fmt.Errorf("bolttable: Since limit %d is below 1", limit)
	}
	if err := r.usable(); err != nil {
		return ChangePage{}, err
	}
	r.op.enter()
	w, err := r.window()
	r.op.leave()
	if err != nil {
		return ChangePage{}, err
	}
	if after > w.head || after+1 < w.oldest {
		return ChangePage{Full: true}, nil
	}
	return r.page(w.log, after, limit)
}

// SinceAt returns the page Since returns after p.Rev when p names this file,
// and a Full page otherwise: when p came from another file (another Origin),
// when this file's retention has trimmed the changes after p.Rev, or when
// p.Rev is above the head. Full tells a caller holding a projection to reload
// it, since no page can bring it forward. Pages read in later transactions
// may find the log trimmed further, which answers Full as well. limit below 1
// is an error.
func (r *ReadTx) SinceAt(p Position, limit int) (ChangePage, error) {
	if limit < 1 {
		return ChangePage{}, fmt.Errorf("bolttable: SinceAt limit %d is below 1", limit)
	}
	if err := r.usable(); err != nil {
		return ChangePage{}, err
	}
	r.op.enter()
	w, err := r.window()
	r.op.leave()
	if err != nil {
		return ChangePage{}, err
	}
	if p.Origin == ([16]byte{}) || p.Origin != w.origin || p.Rev > w.head || p.Rev+1 < w.oldest {
		return ChangePage{Full: true}, nil
	}
	return r.page(w.log, p.Rev, limit)
}

// Position returns where this snapshot stands in the file's change feed: the
// file's origin and its head revision, read in this transaction. Work that
// builds a projection from this snapshot and then pages SinceAt from the
// returned Position sees every commit after the snapshot exactly once,
// including the commits made while it read: the rows and the position come
// from one read. It fails after the transaction ended, and with ErrCorrupt
// when the library bucket, the head or the origin is damaged. Inside
// Options.Inspect it reads the stored file before Open completes it, so a
// file that has no origin yet answers a zero Origin, which no SinceAt
// accepts.
func (r *ReadTx) Position() (Position, error) {
	if err := r.usable(); err != nil {
		return Position{}, err
	}
	r.op.enter()
	lib, _, err := r.feed()
	var p Position
	if err == nil {
		if p.Rev, err = readHead(lib); err == nil {
			p.Origin, err = readOrigin(lib)
		}
	}
	r.op.leave()
	if err != nil {
		return Position{}, err
	}
	return p, nil
}

// page is the changes with Rev above after, at most limit, read from log.
func (r *ReadTx) page(log *bbolt.Bucket, after uint64, limit int) (ChangePage, error) {
	page := ChangePage{Changes: []Change{}}
	if log == nil {
		return page, nil
	}
	c := log.Cursor()
	r.op.enter()
	k, v := c.Seek(be64(after + 1))
	r.op.leave()
	for ; k != nil; k, v = r.next(c) {
		if len(page.Changes) == limit {
			page.More = true
			break
		}
		ch, err := decodeChange(k, v)
		if err != nil {
			return ChangePage{}, err
		}
		page.Changes = append(page.Changes, ch)
	}
	return page, nil
}

// window is what one read of the feed's metadata tells: the change log, the
// head, the oldest revision Since and SinceAt may return and the file's
// origin.
type window struct {
	log          *bbolt.Bucket
	head, oldest uint64
	origin       [16]byte
}

// window reads the window in this transaction.
func (r *ReadTx) window() (window, error) {
	lib, log, err := r.feed()
	if err != nil {
		return window{}, err
	}
	var w window
	w.log = log
	if w.head, err = readHead(lib); err != nil {
		return window{}, err
	}
	if w.origin, err = readOrigin(lib); err != nil {
		return window{}, err
	}
	w.oldest, err = oldestRetained(log, w.head, r.db.retention)
	return w, err
}

func (r *ReadTx) head() (head uint64, log *bbolt.Bucket, err error) {
	lib, log, err := r.feed()
	if err != nil {
		return 0, nil, err
	}
	head, err = readHead(lib)
	return head, log, err
}

func (r *ReadTx) next(c *bbolt.Cursor) (k, v []byte) {
	r.op.enter()
	k, v = c.Next()
	r.op.leave()
	return k, v
}

// Rev returns the head revision: the newest committed one, 0 before any. It
// fails after the transaction ended, and with ErrCorrupt when the library
// bucket, the change log or the head is damaged.
func (r *ReadTx) Rev() (uint64, error) {
	if err := r.usable(); err != nil {
		return 0, err
	}
	r.op.enter()
	head, _, err := r.head()
	r.op.leave()
	return head, err
}

func (r *ReadTx) feed() (lib, log *bbolt.Bucket, err error) {
	lib = r.btx.Bucket([]byte(libBucket))
	if r.beforeOpen && (lib == nil || lib.Bucket(keyChanges) == nil) {
		return lib, nil, nil
	}
	if lib == nil {
		return nil, nil, fmt.Errorf("%w: no library bucket", ErrCorrupt)
	}
	log = lib.Bucket(keyChanges)
	if log == nil {
		return nil, nil, fmt.Errorf("%w: no change log", ErrCorrupt)
	}
	return lib, log, nil
}

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func readHead(lib *bbolt.Bucket) (uint64, error) {
	if lib == nil {
		return 0, nil
	}
	raw := lib.Get(keyHead)
	switch len(raw) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(raw), nil
	}
	return 0, fmt.Errorf("%w: revision head", ErrCorrupt)
}

// originLength is the bytes keyOrigin holds: a random 16-byte identity.
const originLength = 16

// newOrigin mints a file's origin. crypto/rand.Read never returns an error
// (Go 1.24+).
func newOrigin() [16]byte {
	var o [16]byte
	_, _ = rand.Read(o[:])
	return o
}

// readOrigin is the file's origin, zero for a file Open has not stamped yet,
// as inside Options.Inspect before the library bucket is completed. A bucket
// Open created holds one; any other length is corrupt.
func readOrigin(lib *bbolt.Bucket) ([16]byte, error) {
	var o [16]byte
	if lib == nil {
		return o, nil
	}
	raw := lib.Get(keyOrigin)
	switch len(raw) {
	case 0:
		return o, nil
	case originLength:
		copy(o[:], raw)
		return o, nil
	}
	return o, fmt.Errorf("%w: revision origin", ErrCorrupt)
}

// putOrigin stamps o as the file's origin.
func putOrigin(lib *bbolt.Bucket, o [16]byte) error {
	if err := lib.Put(keyOrigin, o[:]); err != nil {
		return fmt.Errorf("bolttable: revision origin: %w", err)
	}
	return nil
}

// oldestRetained is the lowest revision Since may return: the first stored
// one within the newest retention revisions, or head+1 when none is.
// A nil log holds none.
func oldestRetained(log *bbolt.Bucket, head, retention uint64) (uint64, error) {
	if retention == 0 || log == nil {
		return head + 1, nil
	}
	lo := uint64(1)
	if head >= retention {
		lo = head - retention + 1
	}
	k, _ := log.Cursor().Seek(be64(lo))
	if k == nil {
		return head + 1, nil
	}
	if len(k) != 8 {
		return 0, fmt.Errorf("%w: change log key", ErrCorrupt)
	}
	return binary.BigEndian.Uint64(k), nil
}

// record advances the head to c.Rev, logs c when retention keeps any change,
// and trims every logged revision beyond retention, including entries an
// open with a larger retention left behind.
func record(lib *bbolt.Bucket, c Change, retention uint64) error {
	if err := lib.Put(keyHead, be64(c.Rev)); err != nil {
		return fmt.Errorf("bolttable: revision head: %w", err)
	}
	log := lib.Bucket(keyChanges)
	if retention > 0 {
		if err := log.Put(be64(c.Rev), encodeChange(c)); err != nil {
			return fmt.Errorf("bolttable: change log: %w", err)
		}
	}
	if c.Rev < retention {
		return nil
	}
	cut := be64(c.Rev - retention + 1)
	var stale [][]byte
	cur := log.Cursor()
	for k, _ := cur.First(); k != nil && bytes.Compare(k, cut) < 0; k, _ = cur.Next() {
		stale = append(stale, bytes.Clone(k))
	}
	for _, k := range stale {
		if err := log.Delete(k); err != nil {
			return fmt.Errorf("bolttable: trim change log: %w", err)
		}
	}
	return nil
}

func encodeChange(c Change) []byte {
	b := binary.AppendUvarint(nil, uint64(len(c.Subjects)))
	for _, s := range c.Subjects {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	b = binary.AppendUvarint(b, uint64(len(c.Marks)))
	for _, m := range c.Marks {
		b = binary.AppendUvarint(b, uint64(len(m)))
		b = append(b, m...)
	}
	return b
}

func decodeChange(k, v []byte) (Change, error) {
	bad := fmt.Errorf("%w: change log entry", ErrCorrupt)
	if len(k) != 8 {
		return Change{}, bad
	}
	c := Change{Rev: binary.BigEndian.Uint64(k)}
	d := decoder{b: v}
	n := d.uvarint()
	for range n {
		c.Subjects = append(c.Subjects, Subject(d.field()))
	}
	n = d.uvarint()
	for range n {
		c.Marks = append(c.Marks, string(d.field()))
	}
	if d.bad || len(d.b) != 0 {
		return Change{}, bad
	}
	return c, nil
}

// decoder reads uvarint-delimited fields, latching bad on the first
// malformed one so a corrupt count cannot loop or allocate unbounded.
type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) uvarint() uint64 {
	if d.bad {
		return 0
	}
	n, w := binary.Uvarint(d.b)
	if w <= 0 || n > uint64(len(d.b)) {
		d.bad = true
		return 0
	}
	d.b = d.b[w:]
	return n
}

func (d *decoder) field() []byte {
	n := d.uvarint()
	if d.bad || n > uint64(len(d.b)) {
		d.bad = true
		return nil
	}
	f := bytes.Clone(d.b[:n])
	d.b = d.b[n:]
	return f
}
