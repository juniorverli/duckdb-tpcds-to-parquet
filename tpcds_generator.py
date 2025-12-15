import duckdb
import os
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = "tpcds_data"
COMPRESSION = "snappy"


def get_scale_factor() -> int:
    print("\n" + "="*70)
    print("TPC-DS DATA GENERATOR")
    print("="*70)
    print("\nScale Factor Reference:")
    print("  1    = ~1 GB    (Development/Testing)")
    print("  10   = ~10 GB   (Small benchmarks)")
    print("  100  = ~100 GB  (Medium benchmarks)")
    print("  1000 = ~1 TB    (Large benchmarks)")
    print("="*70)

    while True:
        try:
            sf = input("\nEnter TPC-DS Scale Factor (default: 1): ").strip()
            if sf == "":
                return 1
            scale = int(sf)
            if scale < 1:
                print("⚠ Scale factor must be positive. Please try again.")
                continue
            if scale > 10000:
                confirm = input(f"⚠ Scale factor {scale} will generate ~{scale} GB. Continue? (y/n): ")
                if confirm.lower() != 'y':
                    continue
            return scale
        except ValueError:
            print("⚠ Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\n\nOperation cancelled by user.")
            exit(0)

def create_directory(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
    print(f"✓ Directory '{path}' created/verified")


def log_progress(message: str, level: str = "INFO") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def install_tpcds_extension(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        log_progress("Installing TPC-DS extension...")
        conn.execute("INSTALL tpcds;")
        conn.execute("LOAD tpcds;")
        log_progress("✓ TPC-DS extension loaded successfully")
    except Exception as e:
        log_progress(f"Error loading TPC-DS extension: {e}", "ERROR")
        raise


def list_tpcds_tables(conn: duckdb.DuckDBPyConnection, scale_factor: int) -> list:
    try:
        log_progress(f"Generating TPC-DS schema with scale_factor={scale_factor}...")
        conn.execute(f"CALL dsdgen(sf={scale_factor});")

        result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name;
        """).fetchall()

        tables = [row[0] for row in result]
        log_progress(f"✓ Schema generated: {len(tables)} tables found")
        return tables

    except Exception as e:
        log_progress(f"Error listing TPC-DS tables: {e}", "ERROR")
        raise


def export_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    output_dir: str,
    compression: str
) -> dict:
    try:
        file_path = os.path.join(output_dir, f"{table}.parquet")

        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        log_progress(f"Exporting '{table}' ({count:,} records)...")

        start = datetime.now()
        conn.execute(f"""
            COPY {table}
            TO '{file_path}'
            (FORMAT PARQUET, COMPRESSION '{compression}');
        """)
        duration = (datetime.now() - start).total_seconds()

        size_mb = os.path.getsize(file_path) / (1024 * 1024)

        log_progress(
            f"✓ '{table}' exported: {size_mb:.2f} MB "
            f"in {duration:.2f}s ({count:,} records)"
        )

        return {
            "table": table,
            "records": count,
            "size_mb": size_mb,
            "duration_s": duration
        }

    except Exception as e:
        log_progress(f"✗ Error exporting '{table}': {e}", "ERROR")
        return {
            "table": table,
            "error": str(e)
        }


def generate_report(statistics: list, scale_factor: int) -> None:
    print("\n" + "="*70)
    print("FINAL REPORT - TPC-DS GENERATION")
    print("="*70)

    total_records = sum(s.get("records", 0) for s in statistics)
    total_size = sum(s.get("size_mb", 0) for s in statistics)
    total_duration = sum(s.get("duration_s", 0) for s in statistics)
    success_tables = len([s for s in statistics if "error" not in s])
    error_tables = len([s for s in statistics if "error" in s])

    print(f"Scale Factor: {scale_factor}")
    print(f"Tables processed: {len(statistics)}")
    print(f"  • Success: {success_tables}")
    print(f"  • Error: {error_tables}")
    print(f"Total records: {total_records:,}")
    print(f"Total size: {total_size:.2f} MB")
    print(f"Total time: {total_duration:.2f}s")
    print(f"Directory: {OUTPUT_DIR}/")

    if error_tables > 0:
        print("\n⚠ Tables with errors:")
        for s in statistics:
            if "error" in s:
                print(f"  • {s['table']}: {s['error']}")

    print("="*70 + "\n")


def main():
    scale_factor = get_scale_factor()

    log_progress("Starting TPC-DS data generation")
    log_progress(f"Settings: SF={scale_factor}, DIR={OUTPUT_DIR}, COMP={COMPRESSION}")

    create_directory(OUTPUT_DIR)

    conn = duckdb.connect(database=":memory:")

    try:
        install_tpcds_extension(conn)

        tables = list_tpcds_tables(conn, scale_factor)

        if not tables:
            log_progress("No tables found in TPC-DS schema", "WARNING")
            return

        log_progress(f"Starting export of {len(tables)} tables...")
        print("-" * 70)

        statistics = []
        for i, table in enumerate(tables, 1):
            log_progress(f"[{i}/{len(tables)}] Processing '{table}'...")
            result = export_table(conn, table, OUTPUT_DIR, COMPRESSION)
            statistics.append(result)
            print("-" * 70)

        generate_report(statistics, scale_factor)

        log_progress("✓ TPC-DS generation completed successfully!")

    except Exception as e:
        log_progress(f"Fatal error during execution: {e}", "ERROR")
        raise

    finally:
        conn.close()
        log_progress("DuckDB connection closed")


if __name__ == "__main__":
    main()
