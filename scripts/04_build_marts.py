import duckdb
from scripts.config import DUCKDB_PATH


def main():
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}")

    con = duckdb.connect(str(DUCKDB_PATH))
    cols = {row[1] for row in con.execute("PRAGMA table_info('stages_clean')").fetchall()}

    def pick_col(candidates):
        for col in candidates:
            if col in cols:
                return f"\"{col}\""
        return None

    entreprise_col = pick_col(["Société", "Societe"])
    pays_col = pick_col(["Pays", "pays"])
    promotion_col = pick_col(["Promotion"])
    sujet_col = pick_col(["Sujet"])

    if entreprise_col is None or pays_col is None:
        raise ValueError("Missing required columns for marts: Société/Societe or Pays/pays")

    year_parts = []
    if promotion_col:
        year_parts.append(
            f"regexp_extract(CAST({promotion_col} AS VARCHAR), '(20\\\\d{{2}})', 1)"
        )
    if sujet_col:
        year_parts.append(
            f"regexp_extract(CAST({sujet_col} AS VARCHAR), '(20\\\\d{{2}})', 1)"
        )

    if year_parts:
        year_expr = f"COALESCE({', '.join(year_parts)})"
    else:
        year_expr = "NULL"

    con.execute("DROP VIEW IF EXISTS stages_analytics")
    con.execute(
        f"""
        CREATE VIEW stages_analytics AS
        SELECT
            *,
            CAST(
                NULLIF(
                    {year_expr},
                    ''
                ) AS INTEGER
            ) AS annee,
            {entreprise_col} AS entreprise,
            {pays_col} AS pays
        FROM stages_clean
        """
    )

    con.execute("DROP VIEW IF EXISTS mart_top_companies")
    con.execute(
        """
        CREATE VIEW mart_top_companies AS
        SELECT
            annee,
            entreprise,
            COUNT(*) AS nb_stages
        FROM stages_analytics
        WHERE annee IS NOT NULL AND entreprise IS NOT NULL
        GROUP BY annee, entreprise
        """
    )

    con.execute("DROP VIEW IF EXISTS mart_geo")
    con.execute(
        """
        CREATE VIEW mart_geo AS
        SELECT
            annee,
            pays,
            COUNT(*) AS nb_stages
        FROM stages_analytics
        WHERE annee IS NOT NULL AND pays IS NOT NULL
        GROUP BY annee, pays
        """
    )

    con.execute("DROP VIEW IF EXISTS mart_trends")
    con.execute(
        """
        CREATE VIEW mart_trends AS
        SELECT
            annee,
            COUNT(*) AS nb_stages
        FROM stages_analytics
        WHERE annee IS NOT NULL
        GROUP BY annee
        """
    )

    con.close()
    print("[OK] Marts built")


if __name__ == "__main__":
    main()
