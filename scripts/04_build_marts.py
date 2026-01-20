import duckdb
from scripts.config import DUCKDB_PATH


def main():
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB not found: {DUCKDB_PATH}")

    con = duckdb.connect(str(DUCKDB_PATH))

    con.execute("DROP VIEW IF EXISTS stages_analytics")
    con.execute(
        """
        CREATE VIEW stages_analytics AS
        SELECT
            *,
            CAST(
                NULLIF(
                    COALESCE(
                        regexp_extract(Promotion, '(20\\d{2})', 1),
                        regexp_extract(Sujet, '(20\\d{2})', 1)
                    ),
                    ''
                ) AS INTEGER
            ) AS annee,
            COALESCE("Société", "Societe") AS entreprise,
            COALESCE("Pays", "pays") AS pays
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
