# blackbox_cleaner.py
"""
Fonction fournie pour la compétition Zindi - FORCE-N Data Engineering.
Fonction principale :
    clean_data(users_path, transactions_path, output_path) -> pd.DataFrame

Objectifs :
- Nettoyage complet des fichiers JSON utilisateurs + transactions
- Fusion robuste (join automatique sur colonnes pertinentes)
- Masquage PII (emails, national_id, notes internes…)
- Uniformisation des types (tout en string)
- Arrondi des valeurs numériques à 2 décimales
- Formatage de toutes les dates en 'JJ/MM/AAAA HH:MM:SS'
- Renommage tx_id → ID (en première colonne)
- Sauvegarde CSV + retour du DataFrame final

Bibliothèques autorisées :
    pandas, numpy, re, json, dateutil.parser, pathlib
"""

from pathlib import Path
import json
import re
from datetime import datetime
from dateutil import parser as dparser

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# ------------------------- CHARGEMENT FLEXIBLE JSON -------------------------
# ---------------------------------------------------------------------------
def _load_json_flex(path):
    """
    Charge un fichier JSON de manière robuste :
    - JSON classique (liste ou dict)
    - JSON newline-delimited (un objet par ligne)
    - JSON avec structures imbriquées
    - Si tout échoue : renvoie un DataFrame vide
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8")

    # --- Cas 1 : JSON par lignes (NDJSON) ---
    try:
        if "\n" in text and text.strip().count("\n") > 0:
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            objs = []
            for ln in lines:
                try:
                    objs.append(json.loads(ln))
                except Exception:
                    pass
            if objs:
                return pd.json_normalize(objs)

        # --- Cas 2 : JSON complet ---
        obj = json.loads(text)

    except Exception:
        # --- Cas 3 : Pandas read_json en secours ---
        try:
            return pd.read_json(p, orient="records", lines=True)
        except Exception:
            return pd.DataFrame()  # dernier recours

    # Normalisation du JSON en DataFrame
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                return pd.json_normalize(v)
        return pd.json_normalize([obj])

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# ----------------------------- FORMATTAGE DATES -----------------------------
# ---------------------------------------------------------------------------
def _format_datetime(val):
    """Convertit toute date en format 'DD/MM/YYYY HH:MM:SS'. Renvoie '' si impossible."""
    if pd.isna(val):
        return ""

    # Timestamp numérique
    if isinstance(val, (int, float)):
        try:
            ts = val / 1000 if val > 1e12 else val
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return ""

    # Parsing flexible
    for dayfirst in (False, True):
        try:
            dt = dparser.parse(str(val), fuzzy=True, dayfirst=dayfirst)
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            pass

    return ""


# ---------------------------------------------------------------------------
# --------------------------- MASQUAGE DES PII ------------------------------
# ---------------------------------------------------------------------------
def _mask_email(email):
    """Masque un email : n****@domain.com"""
    if not isinstance(email, str) or "@" not in email:
        return ""
    local, domain = email.split("@", 1)
    masked = (local[0] + "*" * 4) if len(local) > 1 else "*" * len(local)
    return f"{masked}@{domain}"


def _mask_national_id(val):
    """Masque identifiant personnel : garde 3 premiers chars puis 'X'."""
    s = str(val)
    if not s:
        return ""
    return s[:3] + "X" * (len(s) - 3)


def _mask_internal_notes(val):
    """Supprime emails, remplace chiffres par X et limite à 200 chars."""
    if pd.isna(val):
        return ""
    s = str(val)
    s = re.sub(r"[0-9]", "X", s)
    s = re.sub(r"[\w\.-]+@[\w\.-]+", "<masked_email>", s)
    return s[:200] + ("..." if len(s) > 200 else "")


# ---------------------------------------------------------------------------
# ------------------ ARRONDI ET FORMATAGE DES NUMÉRIQUES --------------------
# ---------------------------------------------------------------------------
def _round_and_format_numeric(val):
    """Arrondi 2 décimales si numérique, sinon renvoie string brute."""
    if pd.isna(val):
        return ""
    try:
        f = float(val)
        return f"{np.round(f, 2):.2f}"
    except Exception:
        return str(val)


# ---------------------------------------------------------------------------
# --------------------- DÉTECTION DE TYPES DE COLONNES ----------------------
# ---------------------------------------------------------------------------
def _is_datetime_column_name(name):
    """Détecte une colonne date par son nom."""
    if not isinstance(name, str):
        return False
    return any(p in name.lower() for p in
               ("date", "time", "timestamp", "created", "updated"))


def _is_pii_column(name):
    """Détecte une colonne PII par son nom."""
    if not isinstance(name, str):
        return False
    patterns = (
        "email", "mail", "national", "nid", "ssn", "id_number",
        "phone", "msisdn", "note", "notes"
    )
    return any(p in name.lower() for p in patterns)


# ---------------------------------------------------------------------------
# ------------------------- FONCTION PRINCIPALE ------------------------------
# ---------------------------------------------------------------------------
def clean_data(users_path: str, transactions_path: str, output_path: str) -> pd.DataFrame:
    """
    Nettoie, standardise et fusionne users + transactions.
    Applique :
    - Masquage PII
    - Formatage dates
    - Arrondi numériques
    - Conversion full string
    - Renommage / repositionnement de la colonne 'ID'
    """
    # --- CHARGEMENT ---
    users_df = _load_json_flex(users_path) or pd.DataFrame()
    tx_df = _load_json_flex(transactions_path) or pd.DataFrame()

    # Normalisation noms colonnes
    users_df.columns = [str(c) for c in users_df.columns]
    tx_df.columns = [str(c) for c in tx_df.columns]

    # --- Renommage tx_id en ID ---
    for cname in ("tx_id", "TXN_ID"):
        if cname in tx_df.columns:
            tx_df = tx_df.rename(columns={cname: "ID"})

    if "tx_id" in users_df.columns:
        users_df = users_df.rename(columns={"tx_id": "ID"})

    # --- Recherche automatique de clé de jointure ---
    join_order = ["user_id", "customer_id", "client_id", "userId", "customerId"]
    common_key = next((k for k in join_order if k in tx_df.columns and k in users_df.columns), None)

    # Si rien trouvé, on regarde les colonnes communes
    if not common_key:
        inter = [c for c in tx_df.columns if c in users_df.columns and c.lower() not in ("id", "tx_id", "txn_id")]
        common_key = inter[0] if inter else None

    # --- Fusion ---
    if common_key:
        merged = pd.merge(tx_df, users_df, on=common_key, how="left", suffixes=("", "_user"))
    else:
        merged = tx_df.copy()
        for c in users_df.columns:
            if c not in merged.columns:
                merged[c] = np.nan

    # --- Normalisation de la colonne ID ---
    if "ID" not in merged.columns:
        for cand in ("txn_id", "transaction_id", "TXN", "txid"):
            if cand in merged.columns:
                merged = merged.rename(columns={cand: "ID"})
                break

    if "ID" not in merged.columns:
        merged.insert(0, "ID", [f"TXN{i:06d}" for i in range(1, len(merged)+1)])
    else:
        cols = list(merged.columns)
        cols.insert(0, cols.pop(cols.index("ID")))
        merged = merged[cols]

    # ------------------------------------------------------------------
    # ------------------- NETTOYAGE COLONNE PAR COLONNE ----------------
    # ------------------------------------------------------------------
    out = merged.copy()

    for col in out.columns:
        series = out[col]

        # 🌐 Dates
        if _is_datetime_column_name(col) or np.issubdtype(series.dtype, np.datetime64):
            out[col] = series.apply(_format_datetime).astype(str)
            continue

        # 🔐 PII
        if _is_pii_column(col):
            key = col.lower()
            if "email" in key:
                out[col] = series.apply(lambda v: _mask_email(v) if not pd.isna(v) else "")
            elif "national" in key or "nid" in key or "ssn" in key:
                out[col] = series.apply(lambda v: _mask_national_id(v) if not pd.isna(v) else "")
            elif "note" in key:
                out[col] = series.apply(_mask_internal_notes)
            elif "phone" in key or "msisdn" in key:
                out[col] = series.apply(lambda v: re.sub(r"[0-9]", "X", str(v)) if not pd.isna(v) else "")
            continue

        # 🔢 Numériques
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            out[col] = [
                f"{np.round(c, 2):.2f}" if not pd.isna(c) else (str(o) if not pd.isna(o) else "")
                for o, c in zip(series, coerced)
            ]
            continue

        # 🧹 String par défaut
        out[col] = series.fillna("").astype(str).str.strip()

    # Conversion finale 100% string
    for col in out.columns:
        out[col] = out[col].astype(str)

    # Sauvegarde CSV (sans index)
    out.to_csv(output_path, index=False, encoding="utf-8")

    return out

