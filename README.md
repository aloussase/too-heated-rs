Extract data about issues locked as too heated from Github.

## Usage

Make sure you export an environment variable `GITHUB_TOKEN` with your auth
token.

```
cargo run -- --database_url <YOUR_URL> --iterations <YOUR_ITERATIONS>
```

The schema from `db/schema.sql` must already be created in the database you are
using.

## License

MIT
