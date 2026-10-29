package migrate

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang-migrate/migrate/v4/database"
	dStub "github.com/golang-migrate/migrate/v4/database/stub"
	"github.com/golang-migrate/migrate/v4/source"
	sStub "github.com/golang-migrate/migrate/v4/source/stub"
)

type trackedBody struct {
	io.ReadCloser
	closes atomic.Int32
}

func (r *trackedBody) Close() error {
	r.closes.Add(1)
	return r.ReadCloser.Close()
}

type trackedSource struct {
	source.Driver
	mu     sync.Mutex
	bodies []*trackedBody
}

func (s *trackedSource) track(reader io.ReadCloser) *trackedBody {
	body := &trackedBody{ReadCloser: reader}
	s.mu.Lock()
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()
	return body
}

func (s *trackedSource) ReadUp(version uint) (io.ReadCloser, string, error) {
	reader, name, err := s.Driver.ReadUp(version)
	if err != nil {
		return nil, name, err
	}
	return s.track(reader), name, nil
}

func (s *trackedSource) ReadDown(version uint) (io.ReadCloser, string, error) {
	reader, name, err := s.Driver.ReadDown(version)
	if err != nil {
		return nil, name, err
	}
	return s.track(reader), name, nil
}

func (s *trackedSource) verifyClosed(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.bodies) == 0 {
		t.Fatal("no migration bodies opened")
	}
	for i, body := range s.bodies {
		if got := body.closes.Load(); got != 1 {
			t.Errorf("body %d closed %d times, want exactly once", i, got)
		}
	}
}

type failingDatabase struct {
	*dStub.Stub
	mode     string
	failure  error
	runCalls int
}

func (d *failingDatabase) SetVersion(version int, dirty bool) error {
	if (d.mode == "dirty_version" && dirty) || (d.mode == "clean_version" && !dirty) {
		return d.failure
	}
	return d.Stub.SetVersion(version, dirty)
}

func (d *failingDatabase) Run(body io.Reader) error {
	d.runCalls++
	if d.mode == "before_read" {
		return d.failure
	}
	if d.mode == "partial_read" {
		var prefix [1]byte
		if _, err := io.ReadFull(body, prefix[:]); err != nil {
			return err
		}
		return d.failure
	}
	return d.Stub.Run(body)
}

func TestMigrationFailureClosesPrefetchedBodies(t *testing.T) {
	const count = 24 // Exceed the prefetch queue and force producer backpressure.
	for _, operation := range []string{"up", "down", "steps", "migrate", "run"} {
		for _, mode := range []string{"dirty_version", "before_read", "partial_read", "clean_version"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				migrations := source.NewMigrations()
				for version := uint(1); version <= count; version++ {
					migrations.Append(&source.Migration{Version: version, Direction: source.Up, Identifier: "SELECT 1;"})
					migrations.Append(&source.Migration{Version: version, Direction: source.Down, Identifier: "SELECT 0;"})
				}
				src := &trackedSource{Driver: &sStub.Stub{Migrations: migrations}}
				db := &failingDatabase{Stub: &dStub.Stub{CurrentVersion: database.NilVersion}, mode: mode, failure: errors.New("expected database failure")}
				if operation == "down" {
					db.CurrentVersion = count
				}
				m, err := NewWithInstance("stub", src, "stub", db)
				if err != nil {
					t.Fatal(err)
				}
				defer m.Close()
				switch operation {
				case "up":
					err = m.Up()
				case "down":
					err = m.Down()
				case "steps":
					err = m.Steps(count)
				case "migrate":
					err = m.Migrate(count)
				case "run":
					var batch []*Migration
					for version := uint(1); version <= count; version++ {
						body := src.track(io.NopCloser(strings.NewReader("SELECT 1;")))
						migration, makeErr := NewMigration(body, "inline", version, int(version))
						if makeErr != nil {
							t.Fatal(makeErr)
						}
						batch = append(batch, migration)
					}
					err = m.Run(batch...)
				}
				if !errors.Is(err, db.failure) {
					t.Fatalf("%s failure = %v, want original %v", operation, err, db.failure)
				}
				src.verifyClosed(t)
				wantRuns := 1
				if mode == "dirty_version" {
					wantRuns = 0
				}
				if db.runCalls != wantRuns {
					t.Fatalf("SQL executions = %d, want %d; prefetched SQL must not execute", db.runCalls, wantRuns)
				}
				if mode != "dirty_version" && !db.IsDirty {
					t.Fatal("failed migration lost its dirty flag")
				}
			})
		}
	}
}

func TestMigrationFailureClosesSourceBlockedOnRead(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	body := &trackedBody{ReadCloser: reader}
	db := &failingDatabase{Stub: &dStub.Stub{CurrentVersion: database.NilVersion}, mode: "dirty_version", failure: errors.New("expected version failure")}
	m, err := NewWithInstance("stub", &sStub.Stub{}, "stub", db)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	migration, err := NewMigration(body, "blocked", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Run(migration); !errors.Is(err, db.failure) {
		t.Fatalf("blocked source migration = %v, want %v", err, db.failure)
	}
	if got := body.closes.Load(); got != 1 {
		t.Fatalf("blocked source closed %d times, want 1", got)
	}
}

func TestMigrationStopClosesReceivedAndQueuedBodies(t *testing.T) {
	m := newCommon()
	m.GracefulStop <- true
	ret := make(chan interface{}, 3)
	var bodies []*trackedBody
	for version := uint(1); version <= 3; version++ {
		body := &trackedBody{ReadCloser: io.NopCloser(strings.NewReader("SELECT 1;"))}
		bodies = append(bodies, body)
		migration, err := NewMigration(body, "stop", version, int(version))
		if err != nil {
			t.Fatal(err)
		}
		ret <- migration
		go migration.Buffer()
	}
	close(ret)
	if err := m.runMigrations(ret); err != nil {
		t.Fatal(err)
	}
	for i, body := range bodies {
		if got := body.closes.Load(); got != 1 {
			t.Errorf("stopped body %d closed %d times, want 1", i, got)
		}
	}
}
