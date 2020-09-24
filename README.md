# otsql

OpenTelemetry *tracing* integration for the `database/sql` package.

## Usage

If using `github.com/lib/pq`:
```go
var (
    tp.Tracer("postgres")
)

db, err := sql.Open(otsql.WrapDriver("postgres", &pq.Driver{}, tracer), dsn)
if err != nil {
    return errors.Wrap(err, "opening DB")
}
```

Same thing for any other SQL library (MySQL or wtv).

## License

MIT.

## Credits

Pretty much copy-pasta from `github.com/ExpansiveWorlds/instrumentedsql`, which is also MIT licensed.