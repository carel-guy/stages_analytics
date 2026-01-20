import re
import pandas as pd
from scripts.config import RAW_PATH, CLEAN_PATH, PII_COLUMNS


def clean_text(value: str) -> str:
    if pd.isna(value):
        return value
    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_company(name: str) -> str:
    if pd.isna(name):
        return name
    text = clean_text(name)
    replacements = {
        "cap gemini": "capgemini",
        "cap-gemini": "capgemini",
        "capgemini france": "capgemini",
        "darty": "Darty",
    }
    key = text.lower()
    return replacements.get(key, text)
def normalize_city(name: str) -> str:
    if pd.isna(name):
        return name
    text = clean_text(name)
    return text.title()

def normalize_country(code: str) -> str:
    if pd.isna(code):
        return code
    text = clean_text(code)
    return text.upper()


def load_raw() -> pd.DataFrame:
    return pd.read_csv(RAW_PATH, sep=";", encoding="latin1")


def main():
    df = load_raw()

    # Drop PII early (privacy by design)
    pii_present = [c for c in PII_COLUMNS if c in df.columns]
    if pii_present:
        df = df.drop(columns=pii_present)

    # Basic text cleanup
    text_cols = [c for c in df.columns if df[c].dtype == "object"]
    for col in text_cols:
        df[col] = df[col].map(clean_text)

    if "Société" in df.columns:
        df["Société"] = df["Société"].map(normalize_company)
    if "Ville" in df.columns:
        df["Ville"] = df["Ville"].map(normalize_city)
    if "Pays" in df.columns:
        df["Pays"] = df["Pays"].map(normalize_country)

    CLEAN_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False)
    print(f"[OK] Clean file written: {CLEAN_PATH}")


if __name__ == "__main__":
    main()
