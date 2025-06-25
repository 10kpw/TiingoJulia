# TiingoJulia

<!-- For private repos, use shields.io with authentication -->
[![Tests](https://img.shields.io/github/actions/workflow/status/10kpw/TiingoJulia/Test.yml?branch=main&label=Tests)](https://github.com/10kpw/TiingoJulia/actions)
[![Documentation](https://img.shields.io/github/actions/workflow/status/10kpw/TiingoJulia/Docs.yml?branch=main&label=Docs)](https://10kpw.github.io/TiingoJulia/dev)
[![Lint](https://img.shields.io/github/actions/workflow/status/10kpw/TiingoJulia/Lint.yml?branch=main&label=Lint)](https://github.com/10kpw/TiingoJulia/actions)

<!-- Add note about private repository -->
> **Note**: This is a private repository. Access to links and badges requires appropriate permissions.

## How to Cite

If you use TiingoJulia.jl in your work, please cite using the reference given in [CITATION.cff](https://github.com/10kpw/TiingoJulia/blob/main/CITATION.cff).


## Contributing

If you want to make contributions of any kind, please first that a look into our [contributing guide directly on GitHub](docs/src/90-contributing.md) or the [contributing page on the website](https://10kpw.github.io/TiingoJulia/dev/90-contributing/).


---

### Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## Performance Improvements

TiingoJulia.jl now includes significant performance improvements for historical data processing:

### Parallel Processing Enhancements

The library now uses a more robust parallel processing system with the following improvements:

- Better thread utilization using Julia's native `Threads.@spawn` for true parallelism
- Job queue system with bounded buffer to manage resource usage
- Improved error handling in parallel operations
- Automatic system memory detection and optimization
- Optimized database settings for different platforms

### Usage

To take advantage of these improvements:

```julia
using TiingoJulia

# Connect to database with optimized settings
conn = connect_duckdb()
optimize_database(conn)  # Automatically configures for your system

# Get tickers to update
tickers = get_tickers_stock(conn)

# Update with parallel processing
update_historical(conn, tickers;
                  use_parallel=true,
                  batch_size=50,
                  max_concurrent=10)
```

See the [Performance Guide](PERFORMANCE.md) for more details and tuning parameters.

