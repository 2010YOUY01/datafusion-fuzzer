# DataFuzzer

A SQL query fuzzing tool for testing databases.

## Usage

```bash
# Run with default settings
cargo run --release

# Run with specific seed
cargo run --release -- --seed 12345

# Run with config file
cargo run --release -- --config datafuzzer.toml

# Override config file settings with command line arguments
cargo run --release -- --config datafuzzer.toml --rounds 5 --queries-per-round 20

# Full options
cargo run --release -- --help
```

## Configuration

You can configure DataFuzzer in two ways:

1. Command line arguments
2. TOML configuration file

Command line arguments take precedence over configuration file settings.

### Configuration File

See `datafuzzer.toml` for an example configuration file:

```toml
# Random seed for reproducibility
seed = 42

# Number of fuzzing rounds to run
rounds = 3  

# Number of queries to generate per round
queries_per_round = 10

# Query timeout in seconds
timeout_seconds = 30

# Path to log file (comment out to use stdout only)
# log_path = "logs/datafuzzer.log"

# Table generation parameters
max_column_count = 5
max_row_count = 100
max_expr_level = 3
```

### Command Line Options

```
Options:
  -c, --config <FILE>                    Path to config file
  -s, --seed <SEED>                      Random seed [default: 42]
  -r, --rounds <ROUNDS>                  Number of rounds to run
  -q, --queries-per-round <QUERIES>      Number of queries per round
  -t, --timeout <TIMEOUT>                Query timeout in seconds
  -l, --log-path <LOG_PATH>              Path to log file
  -h, --help                             Print help
  -V, --version                          Print version
```

## Progress Tracker
### SQL Features
- [ ] where
- [ ] sort + limit, offset
- [ ] aggregate
- [ ] having
- [ ] join
- [ ] union/union all/intersect/except

### SQL - Subqueries
- [ ] views
- [ ] scalar subquery
- [ ] 'relation-like' subquery

### Expressions
- [ ] Operators
- [ ] Scalar functions
- [ ] Aggregate Functions
- [ ] Window Functions

### Types
- [ ] Complete Primitive types
- [ ] Time-related types
- [ ] Array types
- [ ] Struct/Json

### Infrastructure
- [x] CLI
- [ ] Oracle interface

## License

[MIT](LICENSE)