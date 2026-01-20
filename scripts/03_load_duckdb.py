import duckdb
from scripts.config import CLEAN_PATH, DUCKDB_PATH


def main():
    if not CLEAN_PATH.exists():
        raise FileNotFoundError(f"Clean file not found: {CLEAN_PATH}")

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    con.execute("DROP TABLE IF EXISTS stages_clean")
    con.execute(
        "CREATE TABLE stages_clean AS SELECT * FROM read_csv_auto(?, sep=',', header=True)",
        [str(CLEAN_PATH)],
    )

    con.close()
    print(f"[OK] DuckDB loaded: {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
