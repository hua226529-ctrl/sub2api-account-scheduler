package testsupport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"sync/atomic"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"modernc.org/sqlite"
)

type SQLCounts struct {
	Queries int64
	Execs   int64
}

func (c SQLCounts) Total() int64 { return c.Queries + c.Execs }

type SQLCounter struct {
	queries atomic.Int64
	execs   atomic.Int64
}

func (c *SQLCounter) Snapshot() SQLCounts {
	return SQLCounts{Queries: c.queries.Load(), Execs: c.execs.Load()}
}

func (c *SQLCounter) Reset() { c.queries.Store(0); c.execs.Store(0) }

type TestingT interface {
	Helper()
	TempDir() string
	Cleanup(func())
	Fatalf(string, ...any)
}

type TempDatabase struct {
	Store      *store.Store
	Path       string
	SQLCounter *SQLCounter
	defaults   model.Settings
	connector  driver.Connector
}

// OpenTempDatabase creates an isolated database using the production schema.
func OpenTempDatabase(t TestingT, defaults model.Settings) *TempDatabase {
	t.Helper()
	counter := &SQLCounter{}
	database := &TempDatabase{
		Path:       filepath.Join(t.TempDir(), "scheduler.db"),
		SQLCounter: counter,
		defaults:   defaults,
	}
	database.connector = &countingConnector{
		driver: &countingDriver{inner: &sqlite.Driver{}, counter: counter},
		dsn:    database.Path,
	}
	var err error
	database.Store, err = store.OpenDatabaseForTesting(sql.OpenDB(database.connector), defaults)
	if err != nil {
		t.Fatalf("open temporary scheduler database: %v", err)
	}
	t.Cleanup(func() {
		if database.Store != nil {
			_ = database.Store.Close()
		}
	})
	counter.Reset()
	return database
}

func (d *TempDatabase) Reopen() error {
	if d.Store != nil {
		if err := d.Store.Close(); err != nil {
			return err
		}
	}
	opened, err := store.OpenDatabaseForTesting(sql.OpenDB(d.connector), d.defaults)
	if err != nil {
		return err
	}
	d.Store = opened
	d.SQLCounter.Reset()
	return nil
}

type countingConnector struct {
	driver driver.Driver
	dsn    string
}

func (c *countingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.driver.Open(c.dsn)
}

func (c *countingConnector) Driver() driver.Driver { return c.driver }

type countingDriver struct {
	inner   driver.Driver
	counter *SQLCounter
}

func (d *countingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, counter: d.counter}, nil
}

type countingConn struct {
	driver.Conn
	counter *SQLCounter
}

func (c *countingConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &countingStmt{Stmt: stmt, counter: c.counter}, nil
}

func (c *countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	preparer, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &countingStmt{Stmt: stmt, counter: c.counter}, nil
}

func (c *countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := execer.ExecContext(ctx, query, args)
	if err != driver.ErrSkip {
		c.counter.execs.Add(1)
	}
	return result, err
}

func (c *countingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != driver.ErrSkip {
		c.counter.queries.Add(1)
	}
	return rows, err
}

func (c *countingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	return beginner.BeginTx(ctx, opts)
}

func (c *countingConn) Ping(ctx context.Context) error {
	pinger, ok := c.Conn.(driver.Pinger)
	if !ok {
		return driver.ErrSkip
	}
	return pinger.Ping(ctx)
}

func (c *countingConn) CheckNamedValue(value *driver.NamedValue) error {
	checker, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(value)
}

func (c *countingConn) ResetSession(ctx context.Context) error {
	resetter, ok := c.Conn.(driver.SessionResetter)
	if !ok {
		return nil
	}
	return resetter.ResetSession(ctx)
}

func (c *countingConn) IsValid() bool {
	validator, ok := c.Conn.(driver.Validator)
	return !ok || validator.IsValid()
}

type countingStmt struct {
	driver.Stmt
	counter *SQLCounter
}

func (s *countingStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.Stmt.Exec(args)
	s.counter.execs.Add(1)
	return result, err
}

func (s *countingStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.Stmt.Query(args)
	s.counter.queries.Add(1)
	return rows, err
}

func (s *countingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := execer.ExecContext(ctx, args)
	if err != driver.ErrSkip {
		s.counter.execs.Add(1)
	}
	return result, err
}

func (s *countingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, args)
	if err != driver.ErrSkip {
		s.counter.queries.Add(1)
	}
	return rows, err
}
