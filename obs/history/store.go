// Package history persists payload-free Observation snapshots outside the
// Pipeflow execution path.
package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bbernier33/pipeflow/obs"
)

const SchemaVersion = "pipeflow.obs.history/v1"

type Options struct {
	MaxSnapshots int
	MaxAge       time.Duration
	Sync         bool
}

type Query struct {
	From, To    time.Time
	Limit       int
	NewestFirst bool
}

type Stats struct {
	Snapshots int
	Bytes     int64
	Oldest    time.Time
	Newest    time.Time
}

type record struct {
	Schema   string       `json:"schema"`
	Snapshot obs.Snapshot `json:"snapshot"`
}

// Store is a concurrency-safe directory of immutable snapshot records.
type Store struct {
	mu      sync.Mutex
	dir     string
	options Options
	next    uint64
}

func Open(dir string, options Options) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("pipeflow obs history: directory cannot be empty")
	}
	if options.MaxSnapshots < 0 {
		return nil, errors.New("pipeflow obs history: max snapshots cannot be negative")
	}
	if options.MaxAge < 0 {
		return nil, errors.New("pipeflow obs history: max age cannot be negative")
	}
	if options.MaxSnapshots == 0 && options.MaxAge == 0 {
		options.MaxSnapshots = 1000
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("pipeflow obs history: resolve directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, fmt.Errorf("pipeflow obs history: create directory: %w", err)
	}
	entries, err := snapshotEntries(abs)
	if err != nil {
		return nil, err
	}
	return &Store{dir: abs, options: options, next: uint64(len(entries))}, nil
}

func (s *Store) Append(snapshot obs.Snapshot) error {
	if s == nil {
		return errors.New("pipeflow obs history: nil Store")
	}
	if snapshot.CapturedAt.IsZero() {
		return errors.New("pipeflow obs history: snapshot has zero capture time")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	temporary, err := os.CreateTemp(s.dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("pipeflow obs history: create temporary record: %w", err)
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryName)
		}
	}()
	encoder := json.NewEncoder(temporary)
	if err := encoder.Encode(record{Schema: SchemaVersion, Snapshot: snapshot}); err != nil {
		return fmt.Errorf("pipeflow obs history: encode snapshot: %w", err)
	}
	if s.options.Sync {
		if err := temporary.Sync(); err != nil {
			return fmt.Errorf("pipeflow obs history: sync snapshot: %w", err)
		}
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("pipeflow obs history: close snapshot: %w", err)
	}
	var target string
	for {
		s.next++
		target = filepath.Join(s.dir, fmt.Sprintf("%020d-%020d.json", snapshot.CapturedAt.UnixNano(), s.next))
		if _, err := os.Stat(target); errors.Is(err, fs.ErrNotExist) {
			break
		} else if err != nil {
			return fmt.Errorf("pipeflow obs history: inspect target: %w", err)
		}
	}
	if err := os.Rename(temporaryName, target); err != nil {
		return fmt.Errorf("pipeflow obs history: commit snapshot: %w", err)
	}
	committed = true
	return s.pruneLocked(snapshot.CapturedAt)
}

func (s *Store) Query(query Query) ([]obs.Snapshot, error) {
	if s == nil {
		return nil, errors.New("pipeflow obs history: nil Store")
	}
	if query.Limit < 0 {
		return nil, errors.New("pipeflow obs history: query limit cannot be negative")
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.From.After(query.To) {
		return nil, errors.New("pipeflow obs history: query start is after end")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := snapshotEntries(s.dir)
	if err != nil {
		return nil, err
	}
	if query.NewestFirst {
		reverse(entries)
	}
	result := make([]obs.Snapshot, 0, len(entries))
	for _, entry := range entries {
		snapshot, err := readSnapshot(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if !query.From.IsZero() && snapshot.CapturedAt.Before(query.From) || !query.To.IsZero() && snapshot.CapturedAt.After(query.To) {
			continue
		}
		result = append(result, snapshot)
		if query.Limit > 0 && len(result) == query.Limit {
			break
		}
	}
	return result, nil
}

func (s *Store) Latest() (obs.Snapshot, bool, error) {
	values, err := s.Query(Query{Limit: 1, NewestFirst: true})
	if err != nil || len(values) == 0 {
		return obs.Snapshot{}, false, err
	}
	return values[0], true, nil
}

func (s *Store) Stats() (Stats, error) {
	if s == nil {
		return Stats{}, errors.New("pipeflow obs history: nil Store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := snapshotEntries(s.dir)
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{Snapshots: len(entries)}
	for i, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return Stats{}, fmt.Errorf("pipeflow obs history: inspect record %q: %w", entry.Name(), err)
		}
		stats.Bytes += info.Size()
		captured, err := timestamp(entry.Name())
		if err != nil {
			return Stats{}, err
		}
		if i == 0 {
			stats.Oldest = captured
		}
		stats.Newest = captured
	}
	return stats, nil
}

func (s *Store) pruneLocked(now time.Time) error {
	entries, err := snapshotEntries(s.dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		latest, err := timestamp(entries[len(entries)-1].Name())
		if err != nil {
			return err
		}
		if latest.After(now) {
			now = latest
		}
	}
	remove := 0
	if s.options.MaxSnapshots > 0 && len(entries) > s.options.MaxSnapshots {
		remove = len(entries) - s.options.MaxSnapshots
	}
	cutoff := now.Add(-s.options.MaxAge)
	for remove < len(entries) && s.options.MaxAge > 0 {
		captured, err := timestamp(entries[remove].Name())
		if err != nil {
			return err
		}
		if !captured.Before(cutoff) {
			break
		}
		remove++
	}
	for _, entry := range entries[:remove] {
		if err := os.Remove(filepath.Join(s.dir, entry.Name())); err != nil {
			return fmt.Errorf("pipeflow obs history: prune record %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func snapshotEntries(dir string) ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("pipeflow obs history: read directory: %w", err)
	}
	result := entries[:0]
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			result = append(result, entry)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name() < result[j].Name() })
	return result, nil
}

func timestamp(name string) (time.Time, error) {
	separator := strings.IndexByte(name, '-')
	if separator < 1 {
		return time.Time{}, fmt.Errorf("pipeflow obs history: invalid record name %q", name)
	}
	value, err := strconv.ParseInt(name[:separator], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("pipeflow obs history: invalid record name %q: %w", name, err)
	}
	return time.Unix(0, value), nil
}

func readSnapshot(path string) (obs.Snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return obs.Snapshot{}, fmt.Errorf("pipeflow obs history: open record %q: %w", filepath.Base(path), err)
	}
	defer file.Close()
	var value record
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return obs.Snapshot{}, fmt.Errorf("pipeflow obs history: decode record %q: %w", filepath.Base(path), err)
	}
	if value.Schema != SchemaVersion {
		return obs.Snapshot{}, fmt.Errorf("pipeflow obs history: record %q has unsupported schema %q", filepath.Base(path), value.Schema)
	}
	return value.Snapshot, nil
}

func reverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
