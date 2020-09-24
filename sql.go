package telemetry

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
)

type wrappedDriver struct {
	tracer trace.Tracer
	parent driver.Driver
}

type wrappedConn struct {
	tracer trace.Tracer
	parent driver.Conn
}

type wrappedTx struct {
	tracer trace.Tracer
	ctx    context.Context
	parent driver.Tx
}

type wrappedStmt struct {
	tracer trace.Tracer
	ctx    context.Context
	query  string
	parent driver.Stmt
}

type wrappedResult struct {
	tracer trace.Tracer
	ctx    context.Context
	parent driver.Result
}

type wrappedRows struct {
	tracer trace.Tracer
	ctx    context.Context
	parent driver.Rows
}

func WrapDriver(nameSuffix string, driver driver.Driver, tracer trace.Tracer) string {
	name := "traced-" + nameSuffix
	d := wrappedDriver{parent: driver, tracer: tracer}
	sql.Register(name, d)
	return name
}

func (d wrappedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return wrappedConn{tracer: d.tracer, parent: conn}, nil
}

func (c wrappedConn) Prepare(query string) (driver.Stmt, error) {
	parent, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}

	return wrappedStmt{tracer: c.tracer, query: query, parent: parent}, nil
}

func (c wrappedConn) Close() error {
	return c.parent.Close()
}

func (c wrappedConn) Begin() (driver.Tx, error) {
	tx, err := c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return wrappedTx{tracer: c.tracer, parent: tx}, nil
}

func (c wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	ctx, span := c.tracer.Start(ctx, "sql-tx-begin")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return wrappedTx{tracer: c.tracer, ctx: ctx, parent: tx}, nil
	}

	tx, err = c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return wrappedTx{tracer: c.tracer, ctx: ctx, parent: tx}, nil
}

var errLbl = label.Key("err")

func (c wrappedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	ctx, span := c.tracer.Start(ctx, "sql-prepare")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(query))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if connPrepareCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return wrappedStmt{tracer: c.tracer, ctx: ctx, parent: stmt}, nil
	}

	return c.Prepare(query)
}

func (c wrappedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.parent.(driver.Execer); ok {
		res, err := execer.Exec(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{tracer: c.tracer, parent: res}, nil
	}

	return nil, driver.ErrSkip
}

var (
	queryLbl = label.Key("query")
	argsLbl  = label.Key("args")
)

func (c wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	ctx, span := c.tracer.Start(ctx, "sql-conn-exec")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(query), argsLbl.String(pretty.Sprint(args)))

	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if execContext, ok := c.parent.(driver.ExecerContext); ok {
		res, err := execContext.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{tracer: c.tracer, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Exec(query, dargs)
}

func (c wrappedConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		ctx, span := c.tracer.Start(ctx, "sql-ping")
		span.SetAttribute("component", "database/sql")
		defer func() {
			if err != nil {
				span.RecordError(ctx, err)
			}
			span.End()
		}()

		return pinger.Ping(ctx)
	}
	return nil
}

func (c wrappedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		rows, err := queryer.Query(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{tracer: c.tracer, parent: rows}, nil
	}

	return nil, driver.ErrSkip
}

func (c wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	ctx, span := c.tracer.Start(ctx, "sql-conn-query")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(query), argsLbl.String(pretty.Sprint(args)))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if queryerContext, ok := c.parent.(driver.QueryerContext); ok {
		rows, err := queryerContext.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{tracer: c.tracer, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Query(query, dargs)
}

func (t wrappedTx) Commit() (err error) {
	ctx, span := t.tracer.Start(t.ctx, "sql-tx-commit")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return t.parent.Commit()
}

func (t wrappedTx) Rollback() (err error) {
	ctx, span := t.tracer.Start(t.ctx, "sql-tx-rollback")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return t.parent.Rollback()
}

func (s wrappedStmt) Close() (err error) {
	ctx, span := s.tracer.Start(s.ctx, "sql-stmt-close")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return s.parent.Close()
}

func (s wrappedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s wrappedStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	ctx, span := s.tracer.Start(s.ctx, "sql-stmt-exec")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(s.query), argsLbl.String(pretty.Sprint(args)))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	res, err = s.parent.Exec(args)
	if err != nil {
		return nil, err
	}

	return wrappedResult{tracer: s.tracer, ctx: s.ctx, parent: res}, nil
}

func (s wrappedStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	ctx, span := s.tracer.Start(s.ctx, "sql-stmt-query")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(s.query), argsLbl.String(pretty.Sprint(args)))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	rows, err = s.parent.Query(args)
	if err != nil {
		return nil, err
	}

	return wrappedRows{tracer: s.tracer, ctx: s.ctx, parent: rows}, nil
}

func (s wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	ctx, span := s.tracer.Start(s.ctx, "sql-stmt-exec")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(s.query), argsLbl.String(pretty.Sprint(args)))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if stmtExecContext, ok := s.parent.(driver.StmtExecContext); ok {
		res, err := stmtExecContext.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{tracer: s.tracer, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Exec(dargs)
}

func (s wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	ctx, span := s.tracer.Start(s.ctx, "sql-stmt-query")
	span.SetAttribute("component", "database/sql")
	span.SetAttributes(queryLbl.String(s.query), argsLbl.String(pretty.Sprint(args)))
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	if stmtQueryContext, ok := s.parent.(driver.StmtQueryContext); ok {
		rows, err := stmtQueryContext.QueryContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{tracer: s.tracer, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Query(dargs)
}

func (r wrappedResult) LastInsertId() (id int64, err error) {
	ctx, span := r.tracer.Start(r.ctx, "sql-res-lastInsertId")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return r.parent.LastInsertId()
}

func (r wrappedResult) RowsAffected() (num int64, err error) {
	ctx, span := r.tracer.Start(r.ctx, "sql-res-rowsAffected")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return r.parent.RowsAffected()
}

func (r wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r wrappedRows) Close() error {
	return r.parent.Close()
}

func (r wrappedRows) Next(dest []driver.Value) (err error) {
	ctx, span := r.tracer.Start(r.ctx, "sql-rows-next")
	span.SetAttribute("component", "database/sql")
	defer func() {
		if err != nil {
			span.RecordError(ctx, err)
		}
		span.End()
	}()

	return r.parent.Next(dest)
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
