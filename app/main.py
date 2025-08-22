from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import re
import json

app = FastAPI(title="S4HANA Credit Master Data Remediator (OSS Note 2706489 and 2227014)")

# === Mapping of obsolete fields/tables to replacements ===
REPLACEMENTS = {
    "KNKK-KNKLI": "UKMBP_CMS-PARTNER / UKMBP_CMS_SGM-PARTNER (Business Partner key)",
    "KNKK-KLIMK": "UKMBP_CMS_SGM-CREDIT_LIMIT (Credit Limit, per segment)",
    "KNKK-CRBLB": "UKMBP_CMS_SGM-XBLOCKED (Blocked in Credit Mgmt)",
    "KNKK-CTLPC": "UKMBP_CMS-RISK_CLASS (Risk Category)",
    "KNKK-SAUFT": "Calculated dynamically from UKM_ITEM (COMM_TYP 100, 400, 500) – See Note 2508755",
    "KNKK-NXTRV": "No direct equivalent – see KBA 2371714",
    "KNKA": "No direct equivalent – Main Segment (0000) aggregates exposure (Note 2801882)",
    "T024P": "BUT050 (RelCat TUKMSB0 – 'Credit Mgmt is managed by')",
    "T024B-SBGRP": "BUT050 (RelCat TUKMSBG – 'is in Credit Analyst Group')",
    "T691B": "Obsolete – not carried forward",
    "VBAK-SBGRP": "Obsolete – not filled in FSCM",
    "VBAK-GRUPP": "Obsolete – not filled in FSCM",
}

TABLES = ["KNKK", "KNKA", "T024B", "T024P", "T691B", "VBAK"]
FIELDS = ["KNKLI", "KLIMK", "CRBLB", "CTLPC", "SAUFT", "NXTRV", "SBGRP", "GRUPP"]

# === List of obsolete SAP-delivered programs, transactions, tables, views ===
BANNED_OBJECTS = {
    "PROG": [
        "MF01AO00", "MF02CO00", "RFCMCRCV", "RFCMDECV",
        "RFDKLI10", "RFDKLI20", "RFDKLI20_NACC",
        "RFDKLI30", "RFDKLI40", "RFDKLI40_NACC",
        "RFDKLI41", "RFDKLI41_NACC", "RFDKLI42",
        "RFDKLI43", "RFDKLI50", "RFDKLIAB",
        "RFDKLIAB_NACC", "RFDKVZ00_NACC"
    ],
    "TRAN": ["OB02", "S_ER9_11000074"],
    "TABL": ["T024P", "T024B", "T691B", "KNKK", "KNKA"],
    "VIEW": ["V_T024B"],
}

# Regex patterns
STMT_RE = re.compile(r"\b(SELECT|UPDATE|INSERT|DELETE)\b[\s\S]+?(?:\.|\Z)", re.IGNORECASE)
FIELD_REF_RE = re.compile(rf"\b({'|'.join(TABLES)})-({'|'.join(FIELDS)})\b", re.IGNORECASE)
TABLE_REF_RE = re.compile(rf"\b({'|'.join(TABLES)})\b", re.IGNORECASE)


def find_obsolete_usage(txt: str):
    matches = []

    # --- 1) SQL statement scanning ---
    for stmt in STMT_RE.finditer(txt or ""):
        snippet = stmt.group(0)
        span = stmt.span(0)

        for t in TABLES:
            if re.search(rf"\b{t}\b", snippet, re.IGNORECASE):
                field_found = False
                for f in FIELDS:
                    if re.search(rf"\b{f}\b", snippet, re.IGNORECASE):
                        key = f"{t}-{f}"
                        matches.append({
                            "full": snippet,
                            "stmt": stmt.group(1),
                            "object": key,
                            "suggested_statement": REPLACEMENTS.get(key),
                            "span": span,
                        })
                        field_found = True
                if not field_found:
                    key = t
                    matches.append({
                        "full": snippet,
                        "stmt": stmt.group(1),
                        "object": key,
                        "suggested_statement": REPLACEMENTS.get(key),
                        "span": span,
                    })

    # --- 2) Field references anywhere ---
    for m in FIELD_REF_RE.finditer(txt or ""):
        key = f"{m.group(1).upper()}-{m.group(2).upper()}"
        matches.append({
            "full": m.group(0),
            "stmt": "FIELD_REF",
            "object": key,
            "suggested_statement": REPLACEMENTS.get(key),
            "span": m.span(0),
        })

    # --- 3) Table references anywhere ---
    for m in TABLE_REF_RE.finditer(txt or ""):
        t = m.group(1).upper()
        matches.append({
            "full": m.group(0),
            "stmt": "TABLE_REF",
            "object": t,
            "suggested_statement": REPLACEMENTS.get(t),
            "span": m.span(0),
        })

    return matches


def check_banned_objects(unit_name: str, unit_type: str):
    """Check if the program/include itself is obsolete"""
    hits = []
    banned_list = BANNED_OBJECTS.get(unit_type, [])
    if unit_name in banned_list:
        hits.append({
            "full": unit_name,
            "stmt": "OBJECT_REF",
            "object": f"{unit_type}-{unit_name}",
            "suggested_statement": "Eliminate – Obsolete in S/4HANA Credit Management (2706489)",
            "span": (0, len(unit_name)),
        })
    return hits


class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str     # PROG, TABL, TRAN, VIEW, raw_code
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""


@app.post("/remediate-credit-fields")
async def remediate_credit_fields(units: List[Unit]):
    results = []
    for u in units:
        src = u.code or ""
        metadata = []

        # Step 1: scan code for obsolete field/table usage
        for m in find_obsolete_usage(src):
            metadata.append({
                "table": None,
                "target_type": "TABLE" if "-" not in m["object"] else "FIELD",
                "target_name": m["object"],
                "start_char_in_unit": m["span"][0],
                "end_char_in_unit": m["span"][1],
                "used_fields": [],
                "ambiguous": m["suggested_statement"] is None,
                "suggested_statement": m["suggested_statement"],
                "suggested_fields": None,
            })

        # Step 2: check if the object itself is obsolete (from list)
        for m in check_banned_objects(u.name or u.inc_name, u.type):
            metadata.append({
                "table": None,
                "target_type": u.type,
                "target_name": m["object"],
                "start_char_in_unit": m["span"][0],
                "end_char_in_unit": m["span"][1],
                "used_fields": [],
                "ambiguous": False,
                "suggested_statement": m["suggested_statement"],
                "suggested_fields": None,
            })

        obj = json.loads(u.model_dump_json())
        obj["mb_txn_usage"] = metadata
        results.append(obj)
    return results
