import json
import re
import tempfile
import threading
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# Local engine (must be present in the repo root)
import bank_analysis_v5_2_1 as engine


# =============================================================================
# Streamlit page config
# =============================================================================
st.set_page_config(
    page_title="Part 1 — Process Statements → Analysis JSON (v5.2.1)",
    page_icon="🏦",
    layout="wide",
)

ENGINE_LOCK = threading.Lock()


# =============================================================================
# Helpers: parsing + auto-detection
# =============================================================================

def _safe_json_loads(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "account"


def _most_common(items: List[str]) -> Optional[str]:
    if not items:
        return None
    counts: Dict[str, int] = {}
    for it in items:
        if not it:
            continue
        counts[it] = counts.get(it, 0) + 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def detect_bank_name(statement: Dict[str, Any], fallback_filename: str = "") -> str:
    """
    Best-effort bank name detection:
    1) Most common transactions[*].bank
    2) Fallback heuristics from filename
    """
    tx = statement.get("transactions", []) or []
    banks = [t.get("bank") for t in tx if isinstance(t, dict) and t.get("bank")]
    bank = _most_common([_normalize_spaces(b) for b in banks if b])
    if bank:
        return bank

    fn = (fallback_filename or "").upper()
    if "CIMB" in fn:
        return "CIMB"
    if "HLB" in fn or "HONG" in fn:
        return "Hong Leong"
    if "MUAMALAT" in fn or "BMMB" in fn:
        return "Bank Muamalat"
    return "Unknown Bank"


# --- Company name auto-detection (robust across your sample statements) ---

_PREFIX_PATTERNS = [
    r"TR\s+TO\s+C/A\s+",
    r"TR\s+FROM\s+CA\s+",
    r"TR\s+TO\s+SAVINGS\s+",
    r"OWN\s+ACC\s+TXN\s+",
    r"INTER\s+ACC\s+TXN\s+",
    r"ACC\s+TXN\s+",
    r"DUITNOW\s+TO\s+ACCOUNT\s+",
    r"DUITNOW\s+TRANSFER\s+",
    r"INSTANT\s+TRANSFER\s+AT\s+\w+\s+",
    r"INSTANT\s+TRANSFER\s+",
    r"FUND\s+TRANSFER\s+",
    r"TRANSFER\s+",
    r"CR\s+TFR/SAL/MISC\s+",
    r"DR\s+TFR/SAL/MISC\s+",
    r"CR\s+",
    r"DR\s+",
    r"DRA\s+",
    r"DEBIT\s+ADVICE\s+",
    r"CREDIT\s+ADVICE\s+",
]
_PREFIX_RE = re.compile(r"^(" + "|".join(_PREFIX_PATTERNS) + r")", re.IGNORECASE)

_BANKCODE_TAIL_RE = re.compile(r"\s+(MBB|HLBB|BIMB|AMFB|BMMB|PBB|RHB|OCBC|UOB|HSBC|SCB|CITI|BSN)\b.*$")


def _clean_candidate_name(cand: str) -> str:
    up = _normalize_spaces(str(cand).upper())

    # Iteratively strip known prefixes (some descriptions stack them)
    changed = True
    while changed:
        changed = False
        if _PREFIX_RE.search(up):
            up = _PREFIX_RE.sub("", up).strip()
            changed = True

    # Strip common "front junk"
    up = re.sub(r"^(ITB\s+TRF\s+|INTERBANK[-\s]*\w*\s+|IBG\s+|CIB\s+)", "", up).strip()
    up = re.sub(r"^(TO\s+ACCOUNT\s+)", "", up).strip()

    # Strip trailing bank code + anything after it
    up = _BANKCODE_TAIL_RE.sub("", up).strip()

    return up.strip(" .,-_/").strip()


def _base_company_name(cand: str) -> str:
    up = _clean_candidate_name(cand)

    # Remove legal suffix if present
    up = re.sub(r"\bSDN\s+BHD\b$", "", up).strip()
    up = re.sub(r"\bSDN\b$", "", up).strip()
    up = re.sub(r"\bBHD\b$", "", up).strip()

    return up.strip(" .,-_/").strip()


def _extract_candidates_from_desc(desc: str) -> List[str]:
    """
    Extract likely entity names from a transaction description.

    We focus on Malaysia-style legal suffixes and bank-code endings.
    """
    up = _normalize_spaces(str(desc or "").upper())
    cands: List[str] = []

    # Pattern A: "... SDN BHD" / "SDN" / "BHD"
    for m in re.finditer(
        r"\b([A-Z][A-Z0-9&().,'/-]{1,}(?:\s+[A-Z0-9&().,'/-]{1,}){0,12})\s+(SDN\.?\s*BHD\.?|SDN\.?|BHD\.?)\b",
        up,
    ):
        base = m.group(1).strip(" .,-")
        suffix = re.sub(r"\s+", " ", m.group(2).replace(".", "")).strip()
        full = f"{base} SDN BHD" if ("SDN" in suffix and "BHD" in suffix) else f"{base} {suffix}"
        full = _clean_candidate_name(full)

        if len(full) >= 5 and not any(x in full for x in ["PAYMENT", "DUITNOW", "INTERBANK", "TRANSFER", "TRF", "INVOICE"]):
            cands.append(full)

    # Pattern B: "<NAME> MBB/HLBB/..." (bank code at end)
    for m in re.finditer(
        r"\b([A-Z][A-Z0-9&().,'/-]{2,}(?:\s+[A-Z0-9&().,'/-]{2,}){0,12})\s+(MBB|HLBB|BIMB|AMFB|BMMB|PBB|RHB|OCBC|UOB|HSBC|SCB|CITI|BSN)\b",
        up,
    ):
        base = m.group(1).strip(" .,-")
        base = _clean_candidate_name(base)
        if len(base) >= 5 and not any(x in base for x in ["PAYMENT", "DUITNOW", "INTERBANK", "TRANSFER", "TRF", "INVOICE"]):
            cands.append(base)

    return cands


def suggest_company_name(statements: List[Dict[str, Any]]) -> Tuple[Optional[str], List[Tuple[int, int, str]]]:
    """
    Returns:
      - best_guess company name (or None)
      - ranked suggestions list: (coverage_count, hit_count, name)
    Scoring prioritizes candidates that appear in more accounts (coverage), then frequency.
    """
    if not statements:
        return None, []

    stats: Dict[str, Dict[str, Any]] = {}

    for idx, stmt in enumerate(statements):
        tx = stmt.get("transactions", []) or []
        for t in tx:
            if not isinstance(t, dict):
                continue
            desc = t.get("description") or ""
            for cand in _extract_candidates_from_desc(desc):
                base = _base_company_name(cand)
                if len(base) < 5:
                    continue
                if base not in stats:
                    stats[base] = {"count": 0, "accounts": set()}
                stats[base]["count"] += 1
                stats[base]["accounts"].add(idx)

    ranked: List[Tuple[int, int, str]] = []
    for name, info in stats.items():
        ranked.append((len(info["accounts"]), int(info["count"]), name))

    ranked.sort(key=lambda x: (-x[0], -x[1], x[2]))

    best = ranked[0][2] if ranked else None
    return best, ranked[:10]


def derive_company_keywords(company_name: str) -> List[str]:
    """
    Produce safe-ish, useful keywords for partial matching.
    Engine uses: any(keyword in desc_upper).
    Keep keywords not-too-short to reduce false positives.
    """
    name_up = _normalize_spaces(company_name).upper()
    if not name_up:
        return []

    # Base (remove SDN/BHD)
    base = re.sub(r"\bSDN\s+BHD\b$", "", name_up).strip()
    base = re.sub(r"\bSDN\b$", "", base).strip()
    base = re.sub(r"\bBHD\b$", "", base).strip()
    base = base.strip(" .,-_/").strip()

    kws = set()

    # Full forms
    kws.add(name_up)
    if base and base != name_up:
        kws.add(base)

    # Partial prefix (min 8 chars)
    if len(base) >= 9:
        kws.add(base[:9])  # e.g., "MTC ENGIN"
    elif len(base) >= 8:
        kws.add(base[:8])

    # First token if >= 3 chars (e.g., "MTC")
    first = base.split()[0] if base.split() else ""
    if len(first) >= 3:
        kws.add(first)

    # Two-word prefix if available
    parts = base.split()
    if len(parts) >= 2:
        two = " ".join(parts[:2])
        if len(two) >= 8:
            kws.add(two)

    # Sort longer → shorter
    return sorted(kws, key=lambda s: (-len(s), s))


def load_registry_from_secrets_or_upload(uploaded_registry_file) -> Optional[Dict[str, Any]]:
    # 1) UI upload (preferred)
    if uploaded_registry_file is not None:
        try:
            return json.load(uploaded_registry_file)
        except Exception:
            return None

    # 2) Streamlit secrets (optional)
    # Put this in Streamlit Cloud "Secrets":
    # ACCOUNT_REGISTRY_JSON = """{...}"""
    try:
        raw = st.secrets.get("ACCOUNT_REGISTRY_JSON", None)
        if raw:
            if isinstance(raw, str):
                return _safe_json_loads(raw)
            if isinstance(raw, dict):
                return raw  # already parsed by secrets
    except Exception:
        pass
    return None


def match_registry_entry(
    entry: Dict[str, Any], filename: str, detected_bank: str
) -> bool:
    """
    Registry entry matching rules (first match wins):
      - match.filename (exact, case-insensitive)
      - match.filename_contains
      - match.filename_regex
      - match.bank_contains
    """
    m = entry.get("match", {}) or {}
    fn = filename or ""
    fn_up = fn.upper()
    bank_up = (detected_bank or "").upper()

    exact = m.get("filename")
    if exact and str(exact).upper() == fn_up:
        return True

    contains = m.get("filename_contains")
    if contains and str(contains).upper() in fn_up:
        return True

    regex = m.get("filename_regex")
    if regex:
        try:
            if re.search(regex, fn, flags=re.IGNORECASE):
                return True
        except re.error:
            pass

    bank_contains = m.get("bank_contains")
    if bank_contains and str(bank_contains).upper() in bank_up:
        return True

    return False


def apply_registry_defaults(
    accounts_df: pd.DataFrame, registry: Dict[str, Any]
) -> Tuple[pd.DataFrame, Optional[str], Optional[List[str]], Optional[List[Dict[str, str]]]]:
    """
    Returns:
      - updated accounts_df
      - registry_company_name (optional)
      - registry_company_keywords (optional)
      - registry_related_parties (optional)
    """
    df = accounts_df.copy()

    company = registry.get("company", {}) or {}
    reg_company_name = company.get("name")
    reg_company_keywords = company.get("keywords")
    reg_related = company.get("related_parties")

    entries = registry.get("accounts", []) or []
    for i in range(len(df)):
        filename = str(df.loc[i, "filename"])
        detected_bank = str(df.loc[i, "bank_detected"])
        match = None
        for e in entries:
            if not isinstance(e, dict):
                continue
            if match_registry_entry(e, filename, detected_bank):
                match = e
                break

        if not match:
            continue

        for col in ["account_id", "bank_name", "account_number", "account_type", "classification", "is_od", "od_limit"]:
            if col in match and match[col] not in (None, ""):
                df.loc[i, col] = match[col]

    return df, reg_company_name, reg_company_keywords, reg_related


def validate_monthly_totals(statement: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Check monthly_summary totals equal sums over transactions per month.
    Your samples match perfectly; this flags if a future file is inconsistent.
    """
    tx = statement.get("transactions", []) or []
    ms = statement.get("monthly_summary", []) or []
    ms_map = {m.get("month"): m for m in ms if isinstance(m, dict) and m.get("month")}
    if not tx or not ms_map:
        return True, "No monthly_summary to validate."

    # group sums
    sums: Dict[str, Dict[str, float]] = {}
    last_balance: Dict[str, float] = {}

    # sort by date then __row_order if present
    def _key(i_t):
        i, t = i_t
        date = t.get("date", "")
        row = t.get("__row_order", i)
        try:
            row = int(row)
        except Exception:
            row = i
        return (date, row)

    for _, t in sorted(list(enumerate(tx)), key=_key):
        if not isinstance(t, dict):
            continue
        date = t.get("date", "")
        if not date or len(date) < 7:
            continue
        month = date[:7]
        sums.setdefault(month, {"debit": 0.0, "credit": 0.0, "count": 0})
        sums[month]["debit"] += float(t.get("debit") or 0.0)
        sums[month]["credit"] += float(t.get("credit") or 0.0)
        sums[month]["count"] += 1
        bal = t.get("balance")
        if isinstance(bal, (int, float)):
            last_balance[month] = float(bal)

    # compare
    for month, calc in sums.items():
        given = ms_map.get(month)
        if not given:
            continue
        # allow tiny float tolerance
        if abs(calc["debit"] - float(given.get("total_debit") or 0.0)) > 0.01:
            return False, f"Month {month}: debit mismatch."
        if abs(calc["credit"] - float(given.get("total_credit") or 0.0)) > 0.01:
            return False, f"Month {month}: credit mismatch."
        if int(calc["count"]) != int(given.get("transaction_count") or 0):
            return False, f"Month {month}: transaction count mismatch."
        if month in last_balance and isinstance(given.get("ending_balance"), (int, float)):
            if abs(last_balance[month] - float(given["ending_balance"])) > 0.01:
                return False, f"Month {month}: ending balance mismatch."

    return True, "OK"


# =============================================================================
# Engine patching (safe updates of module-level globals)
# =============================================================================

class EnginePatch:
    def __init__(
        self,
        company_name: str,
        company_keywords: List[str],
        related_parties: List[Dict[str, str]],
        account_info: Dict[str, Dict[str, Any]],
        file_paths: Dict[str, str],
        provided_bank_codes: Optional[List[str]] = None,
    ):
        self.company_name = company_name
        self.company_keywords = company_keywords
        self.related_parties = related_parties
        self.account_info = account_info
        self.file_paths = file_paths
        self.provided_bank_codes = provided_bank_codes
        self._backup: Dict[str, Any] = {}

    def __enter__(self):
        # backup
        self._backup = {
            "COMPANY_NAME": engine.COMPANY_NAME,
            "COMPANY_KEYWORDS": deepcopy(engine.COMPANY_KEYWORDS),
            "RELATED_PARTIES": deepcopy(engine.RELATED_PARTIES),
            "ACCOUNT_INFO": deepcopy(engine.ACCOUNT_INFO),
            "FILE_PATHS": deepcopy(engine.FILE_PATHS),
            "PROVIDED_BANK_CODES": deepcopy(engine.PROVIDED_BANK_CODES),
        }
        # patch
        engine.COMPANY_NAME = self.company_name
        engine.COMPANY_KEYWORDS = self.company_keywords
        engine.RELATED_PARTIES = self.related_parties
        engine.ACCOUNT_INFO = self.account_info
        engine.FILE_PATHS = self.file_paths
        if self.provided_bank_codes is not None:
            engine.PROVIDED_BANK_CODES = set(self.provided_bank_codes)

        return self

    def __exit__(self, exc_type, exc, tb):
        # restore
        engine.COMPANY_NAME = self._backup["COMPANY_NAME"]
        engine.COMPANY_KEYWORDS = self._backup["COMPANY_KEYWORDS"]
        engine.RELATED_PARTIES = self._backup["RELATED_PARTIES"]
        engine.ACCOUNT_INFO = self._backup["ACCOUNT_INFO"]
        engine.FILE_PATHS = self._backup["FILE_PATHS"]
        engine.PROVIDED_BANK_CODES = self._backup["PROVIDED_BANK_CODES"]
        return False


# =============================================================================
# UI
# =============================================================================

st.title("Part 1 — Processed Statements ➜ Analysis JSON (Engine v5.2.1)")
st.caption("Upload one or more *processed bank statement JSON* files (your Part 0 output). This app creates one consolidated analysis JSON for Part 2.")

with st.sidebar:
    st.header("Optional: Account Registry")
    st.write(
        "If you keep getting **Unknown account numbers** or you don't want to re-key metadata every run, "
        "use an **account_registry.json**."
    )
    registry_file = st.file_uploader("Upload account_registry.json (optional)", type=["json"], accept_multiple_files=False)

    st.markdown("---")
    st.header("Optional: Related Parties")
    st.write("Paste JSON list of related parties (optional).")
    default_related = "[]"
    related_parties_text = st.text_area(
        "RELATED_PARTIES (JSON)",
        value=default_related,
        height=160,
        help='Example: [{"name":"ABC SDN BHD","relationship":"Sister Company"}]',
    )

    st.markdown("---")
    st.header("Output Options")
    output_basename = st.text_input("Output filename base", value="analysis_output")
    st.write("The download will be a single JSON file suitable for Part 2.")


uploaded_files = st.file_uploader(
    "Upload processed statement JSON files",
    type=["json"],
    accept_multiple_files=True,
    help="These are the per-bank/per-account processed JSONs (each contains summary, monthly_summary, transactions).",
)

if not uploaded_files:
    st.info("Upload at least one processed statement JSON to begin.")
    st.stop()

# Load registry (upload or secrets)
registry = load_registry_from_secrets_or_upload(registry_file)

# Parse statements
statements: List[Dict[str, Any]] = []
filenames: List[str] = []
errors: List[str] = []

for uf in uploaded_files:
    try:
        obj = json.load(uf)
        if not isinstance(obj, dict):
            raise ValueError("Uploaded JSON is not an object")
        # basic schema check
        if "transactions" not in obj or "monthly_summary" not in obj:
            raise ValueError("Missing required keys: transactions/monthly_summary")
        statements.append(obj)
        filenames.append(uf.name)
    except Exception as e:
        errors.append(f"{uf.name}: {e}")

if errors:
    st.error("Some uploads could not be parsed:")
    for e in errors:
        st.write(f"- {e}")
    st.stop()

# Build initial accounts table
rows = []
for stmt, fn in zip(statements, filenames):
    bank_detected = detect_bank_name(stmt, fn)

    # Default account_id derived from filename
    default_account_id = _slugify(Path(fn).stem).upper()
    # make it look nicer: ABC_DEF
    default_account_id = re.sub(r"[^A-Z0-9_]+", "_", default_account_id).strip("_") or "ACCOUNT_1"

    rows.append(
        {
            "filename": fn,
            "account_id": default_account_id,
            "bank_detected": bank_detected,
            "bank_name": bank_detected,  # default to detected
            "account_number": "",
            "account_type": "Current",
            "classification": "SECONDARY",
            "is_od": False,
            "od_limit": None,
        }
    )

accounts_df = pd.DataFrame(rows)

# Apply registry defaults if present
reg_company_name = None
reg_company_keywords = None
reg_related_parties = None
if registry:
    accounts_df, reg_company_name, reg_company_keywords, reg_related_parties = apply_registry_defaults(accounts_df, registry)

# Company name suggestions
auto_company, ranked = suggest_company_name(statements)
company_default = reg_company_name or auto_company or "YOUR COMPANY"

# Keywords
if isinstance(reg_company_keywords, list) and reg_company_keywords:
    keyword_default_list = [str(x).upper() for x in reg_company_keywords if str(x).strip()]
else:
    keyword_default_list = derive_company_keywords(company_default)

# Related parties
related_parties = None
parsed_related = _safe_json_loads(related_parties_text)
if isinstance(parsed_related, list):
    related_parties = parsed_related
elif reg_related_parties and isinstance(reg_related_parties, list):
    related_parties = reg_related_parties
else:
    related_parties = []

# Main layout
left, right = st.columns([1.25, 1])

with left:
    st.subheader("1) Detected Inputs")
    with st.expander("Company name suggestions (auto-detected)", expanded=False):
        if ranked:
            st.write("Top candidates (coverage across accounts, hits):")
            st.table(pd.DataFrame(ranked, columns=["coverage_accounts", "hits", "name"]))
        else:
            st.write("No strong company-name candidates found in descriptions. You can type the company name manually.")

    company_name = st.text_input("Company name", value=company_default)

    kw_text = st.text_area(
        "Company keywords (one per line)",
        value="\n".join(keyword_default_list),
        height=120,
        help="Used for inter-account transfer detection. Keep a few strong keywords; avoid very short ones.",
    )
    company_keywords = [k.strip().upper() for k in kw_text.splitlines() if k.strip()]

    st.subheader("2) Account metadata")
    st.caption("Bank name is auto-detected. Account number usually is NOT present in the processed JSON, so use registry/secrets to fill it automatically.")

    edited_df = st.data_editor(
        accounts_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "filename": st.column_config.TextColumn("Filename", disabled=True),
            "bank_detected": st.column_config.TextColumn("Bank (detected)", disabled=True),
            "account_id": st.column_config.TextColumn("Account ID"),
            "bank_name": st.column_config.TextColumn("Bank name (for report)"),
            "account_number": st.column_config.TextColumn("Account number"),
            "account_type": st.column_config.SelectboxColumn(
                "Account type",
                options=["Current", "Savings", "Overdraft", "Other"],
                default="Current",
            ),
            "classification": st.column_config.SelectboxColumn(
                "Classification",
                options=["PRIMARY", "SECONDARY"],
                default="SECONDARY",
            ),
            "is_od": st.column_config.CheckboxColumn("OD?"),
            "od_limit": st.column_config.NumberColumn("OD limit (RM)", step=1000),
        },
    )

    # Auto-set first row PRIMARY if none selected
    if "PRIMARY" not in set(edited_df["classification"].astype(str).str.upper()):
        edited_df.loc[0, "classification"] = "PRIMARY"

    # Validate uniqueness of account_id
    acc_ids = [str(x).strip() for x in edited_df["account_id"].tolist()]
    if len(acc_ids) != len(set(acc_ids)):
        st.error("Account IDs must be unique. Please edit the Account ID column so there are no duplicates.")
        st.stop()

    # Data quality checks
    st.subheader("3) Data quality checks (per uploaded statement)")
    checks = []
    for stmt, fn in zip(statements, filenames):
        ok, msg = validate_monthly_totals(stmt)
        checks.append({"filename": fn, "monthly_summary_validation": "OK" if ok else f"FAIL: {msg}"})
    st.table(pd.DataFrame(checks))

with right:
    st.subheader("Run analysis")
    st.write("When you click **Generate Analysis JSON**, the app writes your uploaded statement JSONs to a temporary directory, patches the engine config, runs the analysis, and gives you a downloadable JSON for Part 2.")

    # Registry export (based on current edits)
    def _build_registry_payload() -> Dict[str, Any]:
        accounts_payload = []
        for _, row in edited_df.iterrows():
            accounts_payload.append(
                {
                    "match": {"filename": row["filename"]},
                    "account_id": str(row["account_id"]).strip(),
                    "bank_name": str(row["bank_name"]).strip(),
                    "account_number": str(row["account_number"]).strip(),
                    "account_type": str(row["account_type"]).strip(),
                    "classification": str(row["classification"]).strip().upper(),
                    "is_od": bool(row["is_od"]),
                    "od_limit": row["od_limit"] if pd.notna(row["od_limit"]) else None,
                }
            )
        payload = {
            "company": {
                "name": company_name,
                "keywords": company_keywords,
                "related_parties": related_parties,
            },
            "accounts": accounts_payload,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        return payload

    st.download_button(
        "Download account_registry.json (based on current inputs)",
        data=json.dumps(_build_registry_payload(), indent=2).encode("utf-8"),
        file_name="account_registry.json",
        mime="application/json",
        help="Commit this to a private repo, or paste it into Streamlit secrets as ACCOUNT_REGISTRY_JSON.",
    )

    st.markdown("---")

    run = st.button("🚀 Generate Analysis JSON", type="primary", use_container_width=True)

    if run:
        # Build account_info and write temp files
        with ENGINE_LOCK:
            with tempfile.TemporaryDirectory(prefix="bank_analysis_") as tmpdir:
                file_paths: Dict[str, str] = {}
                account_info: Dict[str, Dict[str, Any]] = {}

                # Write uploaded JSON files into temp dir using the edited account_id as key
                for stmt, (_, row) in zip(statements, edited_df.iterrows()):
                    acc_id = str(row["account_id"]).strip()
                    tmp_path = str(Path(tmpdir) / f"{acc_id}.json")
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        json.dump(stmt, f, ensure_ascii=False)
                    file_paths[acc_id] = tmp_path

                    account_info[acc_id] = {
                        "bank_name": str(row["bank_name"]).strip() or str(row["bank_detected"]).strip(),
                        "account_number": str(row["account_number"]).strip() or "Unknown",
                        "account_holder": company_name,
                        "account_type": str(row["account_type"]).strip() or "Current",
                        "classification": str(row["classification"]).strip().upper() or "SECONDARY",
                    }

                # Patch + run engine
                try:
                    with EnginePatch(
                        company_name=company_name,
                        company_keywords=company_keywords,
                        related_parties=related_parties,
                        account_info=account_info,
                        file_paths=file_paths,
                        provided_bank_codes=None,  # keep engine defaults unless you add a UI for it
                    ):
                        result = engine.analyze()

                    # Ensure output metadata
                    result.setdefault("report_info", {})
                    result["report_info"]["schema_version"] = result["report_info"].get("schema_version", "5.2.1")
                    result["report_info"]["generated_at"] = datetime.now(timezone.utc).isoformat()

                    out_name = f"{output_basename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    st.success("Analysis complete. Download your Part 2 input JSON below.")
                    st.download_button(
                        "⬇️ Download analysis JSON",
                        data=json.dumps(result, indent=2).encode("utf-8"),
                        file_name=out_name,
                        mime="application/json",
                        use_container_width=True,
                    )

                    # Quick summary
                    st.markdown("---")
                    st.subheader("High-level summary")
                    accounts = result.get("accounts", []) or []
                    st.write(f"Accounts analysed: **{len(accounts)}**")
                    if accounts:
                        summary_rows = []
                        for a in accounts:
                            summary_rows.append(
                                {
                                    "account_id": a.get("account_id"),
                                    "bank_name": a.get("bank_name"),
                                    "account_number": a.get("account_number"),
                                    "txns": a.get("transaction_count"),
                                    "total_credits": a.get("total_credits"),
                                    "total_debits": a.get("total_debits"),
                                    "closing_balance": a.get("closing_balance"),
                                }
                            )
                        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

                except Exception as e:
                    st.error(f"Engine error: {e}")
