import pandas as pd
from datetime import datetime
from scripts.config import RAW_PATH, REPORTS_DIR, EXPECTED_COLUMNS, PII_COLUMNS


def ensure_dirs():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def load_raw() -> pd.DataFrame:
    # CSV  observe: separateur ';' et encodage latin1
    return pd.read_csv(RAW_PATH, sep=";", encoding="latin1")


def column_profile(df: pd.DataFrame) -> pd.DataFrame:
    # Profil simple: types, % manquants, nb uniques
    prof = pd.DataFrame({
        "column": df.columns,
        "dtype": [str(df[c].dtype) for c in df.columns],
        "missing_pct": [(df[c].isna().mean() * 100) for c in df.columns],
        "n_unique": [df[c].nunique(dropna=True) for c in df.columns],
    })
    prof["missing_pct"] = prof["missing_pct"].round(2)
    prof["is_expected"] = prof["column"].isin(EXPECTED_COLUMNS)
    prof["is_pii"] = prof["column"].isin(PII_COLUMNS)
    return prof.sort_values(["is_pii", "missing_pct"], ascending=[False, False])


def generate_markdown_report(df: pd.DataFrame, prof: pd.DataFrame) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append("# Profiling report - Stages (2023-2025)\n")
    lines.append(f"- Generated: **{now}**\n")
    lines.append(f"- Rows: **{len(df)}**")
    lines.append(f"- Columns: **{df.shape[1]}**\n")

    # Colonnes PII
    pii_cols = prof[prof["is_pii"]]["column"].tolist()
    lines.append("## RGPD / PII scan\n")
    if pii_cols:
        lines.append("Colonnes identifiees comme PII (a supprimer/anonymiser des le debut) :\n")
        for c in pii_cols:
            lines.append(f"- PII: {c}")
    else:
        lines.append("Aucune colonne PII explicite detectee.\n")

    # Resume qualite
    lines.append("\n## Qualite - apercu\n")
    worst = prof.sort_values("missing_pct", ascending=False).head(10)
    lines.append(worst.to_markdown(index=False))

    lines.append("\n## Notes\n")
    lines.append("- Ce rapport n'affiche pas d'exemples de valeurs pour eviter toute fuite potentielle (privacy first).")
    lines.append("- Les decisions RGPD sont appliquees dans `02_clean_anonymize.py`.")

    return "\n".join(lines) + "\n"


def main():
    ensure_dirs()
    df = load_raw()

    # Verif simple de colonnes attendues (non bloquant)
    missing_expected = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_expected:
        print(f"[WARN] Colonnes attendues manquantes: {missing_expected}")

    prof = column_profile(df)
    prof_path = REPORTS_DIR / "profiling_columns.csv"
    prof.to_csv(prof_path, index=False)

    md = generate_markdown_report(df, prof)
    md_path = REPORTS_DIR / "profiling_report.md"
    md_path.write_text(md, encoding="utf-8")

    print("[OK] Profiling termine")
    print(f" - {prof_path}")
    print(f" - {md_path}")


if __name__ == "__main__":
    main()
