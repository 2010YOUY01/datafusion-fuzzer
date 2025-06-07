# DataFusion Fuzzer

> **🚧 Work In Progress** 
> 
> This project is still under active development. The following documentation is AI-generated and requires future cleanup and validation.
>
> This is a Rust rewrite of [datafusion-sqlancer](https://github.com/apache/datafusion/issues/11030), originally implemented in Java. The rewrite aims to simplify implementation, enable better integration with existing DataFusion tooling, and make test oracles applicable to `sqllogictests`. See [this issue](https://github.com/apache/datafusion/issues/14535) for more details on the motivation behind the Rust rewrite.

A comprehensive fuzzing tool for Apache DataFusion, designed to test SQL query execution and find potential bugs, crashes, or inconsistencies in the query engine.

## Quick Start

To run the fuzzer with default settings:

```bash
cargo run --release
```

To run with a custom configuration:

```bash
cargo run --release -- --config datafusion-fuzzer.toml
```

To run with command-line options:
```bash
cargo run --release -- --config datafusion-fuzzer.toml --rounds 5 --queries-per-round 20
```

## Configuration

The fuzzer supports extensive configuration options to customize the fuzzing process.

You can configure DataFusion Fuzzer in two ways:

1. **Configuration file**: Use a TOML file to specify detailed settings
2. **Command-line arguments**: Override configuration file settings or use standalone

### Configuration File

See `datafusion-fuzzer.toml` for an example configuration file:

```toml
# Fuzzing execution settings
seed = 42
rounds = 3
queries_per_round = 10
timeout_seconds = 30

# Logging settings  
display_logs = true
enable_tui = false
# log_path = "logs/datafusion-fuzzer.log"

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
- [x] where
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
- [x] Oracle interface

## License

[MIT](LICENSE)