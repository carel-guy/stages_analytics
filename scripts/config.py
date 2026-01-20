from pathlib import Path

# Racine du projet (un niveau au-dessus de /scripts)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
REPORTS_DIR = PROJECT_ROOT / "reports"

RAW_FILENAME = "stages_2023_2025_raw.csv"
CLEAN_FILENAME = "stages_2023_2025_clean.csv"
DUCKDB_FILENAME = "stages.duckdb"

RAW_PATH = RAW_DIR / RAW_FILENAME
CLEAN_PATH = PROCESSED_DIR / CLEAN_FILENAME
DUCKDB_PATH = WAREHOUSE_DIR / DUCKDB_FILENAME

# If the default raw filename is missing, fall back to the only CSV in data/raw.
if not RAW_PATH.exists():
    candidates = sorted(RAW_DIR.glob("*.csv"))
    if len(candidates) == 1:
        RAW_PATH = candidates[0]

# Colonnes PII (RGPD) à supprimer (safe by design)
PII_COLUMNS = {"Nom étudiant", "email étudiant"}

# Colonnes attendues (pour détecter si le CSV change)
EXPECTED_COLUMNS = [
    "Programme",
    "Promotion",
    "Sujet",
    "Domaine de stage",
    "Société",
    "Adresse société",
    "Code postal",
    "Ville",
    "Pays",
    "Nom étudiant",
    "email étudiant",
]
