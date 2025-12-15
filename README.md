# duckdb-tpcds-to-parquet

Generate TPC-DS benchmark data using DuckDB and export to Parquet format. A simple way to generate big data for testing and development.

## Requirements

- Python >= 3.14
- Poetry (for dependency management)

## Installation

```bash
poetry install
```

## Usage

Run the script:

```bash
poetry run python tpcds_generator.py
```

The script will prompt you for a scale factor:

```
Enter TPC-DS Scale Factor (default: 1):
```

### Scale Factor Reference

| Scale Factor | Size | Description |
|--------------|------|-------------|
| 1 | ~1 GB | Development/Testing |
| 10 | ~10 GB | Small benchmarks |
| 100 | ~100 GB | Medium benchmarks |
| 1000 | ~1 TB | Large benchmarks |

## Output

Generated Parquet files are saved in the `tpcds_data/` directory.

## Configuration

Edit these constants in `tpcds_generator.py`:

```python
OUTPUT_DIR = "tpcds_data"        # Output directory
COMPRESSION = "snappy"    # Compression: snappy, gzip, zstd
```

## Author

**Dionei Junior (juniorverli)**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/juniorverli)

## License

This script is provided as-is for benchmark and testing purposes.

## Resources

- [DuckDB Documentation](https://duckdb.org/)
- [TPC-DS Extension](https://duckdb.org/docs/extensions/tpcds.html)
- [Parquet Format](https://parquet.apache.org/)

---

**Note**: TPC-DS is a trademark of the Transaction Processing Performance Council (TPC). This tool is for generating test data and is not an official TPC benchmark implementation.
