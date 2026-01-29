import json
import re
import tempfile
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List

import streamlit as st

import bank_analysis_v5_2_1 as engine

_ENGINE_LOCK = Lock()


# -----------------------------
# Helpers
# -----------------------------
def _to_float(value: Any) -> float:
    """Best-effort conversion of JSON numeric fields to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if not s:
        return 0.0

    # Handle accounting negative: (1,234.56)
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace(",", "")
    try:
        out = float(s)
        return -out if neg else out
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _clean_date(value: Any) -> str:
    """Return date as YYYY-MM-DD if possible."""
    s = str(value or "").strip()
    if not s:
        return ""

    # If already in ISO, keep first 10 chars (handles timestamps)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    # Try common formats (defensive)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    # Engine requires strict ISO dates; fail explicitly
    return ""


def _sanitize_account_id(filename: str, idx: int) -> str:
    base = re.sub(r"\.json$", "", filename, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_")
    if not base:
        base = f"ACCOUNT_{idx+1}"
    return base.upper()


def _parse_csv_or_lines(raw: str, upper: bool = True) -> List[str]:
    parts = re.split(r"[,\n]+", raw or "")
    cleaned: List[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        cleaned.append(p.upper() if upper else p)

    # de-dupe (preserve order)
    seen = set()
    out: List[str] = []
    for p in cleaned:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def _parse_related_parties(raw: str) -> List[Dict[str, str]]:
    """
    Expected formats (one per line):
      - NAME | Relationship
      - NAME, Relationship
      - NAME (defaults relationship to 'Related Party')
    """
    parties: List[Dict[str, str]] = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line:
            continue

        if "|" in line:
            name, rel = [x.strip() for x in line.split("|", 1)]
        elif "," in line:
            name, rel = [x.strip() for x in line.split(",", 1)]
        else:
            name, rel = line, "Related Party"

        if name:
            parties.append({"name": name, "relationship": rel or "Related Party"})
    return parties


def _normalize_statement_json(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make uploaded JSON compatible with bank_analysis_v5_2_1.py.

    Ensures presence of:
      - summary: { total_transactions, date_range }
      - monthly_summary: list with net_change/highest_balance/lowest_balance
      - transactions: list with ISO dates

    IMPORTANT: v5.2.1 uses m['net_change'] in monthly_summary; if your upstream JSON
    doesn't include it, we compute it as (total_credit - total_debit).
    """
    if not isinstance(raw, dict):
        raise ValueError("Top-level JSON must be an object/dict.")

    if "transactions" not in raw or not isinstance(raw["transactions"], list):
        raise ValueError("Missing required key: 'transactions' (must be a list).")

    # ---- Transactions ----
    txns: List[Dict[str, Any]] = []
    for t in raw["transactions"]:
        if not isinstance(t, dict):
            continue
        date = _clean_date(t.get("date"))
        desc = str(t.get("description", "")).strip()
        if not date or not desc:
            continue

        debit = _to_float(t.get("debit", 0.0))
        credit = _to_float(t.get("credit", 0.0))
        balance = _to_float(t.get("balance", 0.0)) if "balance" in t else 0.0

        txns.append(
            {
                "date": date,
                "description": desc,
                "debit": debit,
                "credit": credit,
                "balance": balance,
            }
        )

    if not txns:
        raise ValueError("No valid transactions found after normalization (check date formats).")

    txns_sorted = sorted(txns, key=lambda x: (x["date"], x["description"]))

    # ---- Summary ----
    summary = raw.get("summary") if isinstance(raw.get("summary"), dict) else {}
    date_range = str(summary.get("date_range", "")).strip()
    if " to " not in date_range:
        date_range = f"{txns_sorted[0]['date']} to {txns_sorted[-1]['date']}"

    total_transactions = _to_int(summary.get("total_transactions", len(txns_sorted))) or len(txns_sorted)
    summary_out = {"total_transactions": total_transactions, "date_range": date_range}

    # ---- Monthly summary ----
    monthly_raw = raw.get("monthly_summary")
    monthly_summary: List[Dict[str, Any]] = []

    if isinstance(monthly_raw, list) and monthly_raw:
        for m in monthly_raw:
            if not isinstance(m, dict):
                continue

            month = str(m.get("month", "")).strip()
            if not re.match(r"^\d{4}-\d{2}$", month):
                continue

            total_debit = _to_float(m.get("total_debit", 0.0))
            total_credit = _to_float(m.get("total_credit", 0.0))
            ending_balance = _to_float(m.get("ending_balance", 0.0))

            # Some upstreams might use different keys:
            highest_balance = _to_float(m.get("highest_balance", m.get("highest_intraday", ending_balance)))
            lowest_balance = _to_float(m.get("lowest_balance", m.get("lowest_intraday", ending_balance)))

            net_change = m.get("net_change")
            if net_change is None:
                net_change = total_credit - total_debit

            monthly_summary.append(
                {
                    "month": month,
                    "transaction_count": _to_int(m.get("transaction_count", 0)),
                    "total_debit": total_debit,
                    "total_credit": total_credit,
                    "ending_balance": ending_balance,
                    "highest_balance": highest_balance,
                    "lowest_balance": lowest_balance,
                    "net_change": _to_float(net_change),
                }
            )

    # If monthly_summary missing/empty, compute it from transactions
    if not monthly_summary:
        by_month: Dict[str, List[Dict[str, Any]]] = {}
        for t in txns_sorted:
            by_month.setdefault(t["date"][:7], []).append(t)

        for month in sorted(by_month.keys()):
            mtx = by_month[month]
            total_debit = sum(_to_float(t.get("debit", 0.0)) for t in mtx)
            total_credit = sum(_to_float(t.get("credit", 0.0)) for t in mtx)
            balances = [t.get("balance", 0.0) for t in mtx if isinstance(t.get("balance", 0.0), (int, float))]
            ending_balance = balances[-1] if balances else 0.0
            highest_balance = max(balances) if balances else ending_balance
            lowest_balance = min(balances) if balances else ending_balance

            monthly_summary.append(
                {
                    "month": month,
                    "transaction_count": len(mtx),
                    "total_debit": round(total_debit, 2),
                    "total_credit": round(total_credit, 2),
                    "ending_balance": round(ending_balance, 2),
                    "highest_balance": round(highest_balance, 2),
                    "lowest_balance": round(lowest_balance, 2),
                    "net_change": round(total_credit - total_debit, 2),
                }
            )

    return {"summary": summary_out, "monthly_summary": monthly_summary, "transactions": txns_sorted}


def _run_engine(
    company_name: str,
    company_keywords: List[str],
    related_parties: List[Dict[str, str]],
    account_info: Dict[str, Dict[str, str]],
    file_payloads_by_account: Dict[str, Dict[str, Any]],
    provided_bank_codes: List[str],
) -> Dict[str, Any]:
    """
    Writes normalized payloads to a temp directory and invokes engine.analyze().

    Uses:
      - Lock: prevents cross-session config collisions
      - Backup/restore: avoids global state leaking between runs
    """
    backup = {
        "COMPANY_NAME": getattr(engine, "COMPANY_NAME", None),
        "COMPANY_KEYWORDS": getattr(engine, "COMPANY_KEYWORDS", None),
        "RELATED_PARTIES": getattr(engine, "RELATED_PARTIES", None),
        "ACCOUNT_INFO": getattr(engine, "ACCOUNT_INFO", None),
        "FILE_PATHS": getattr(engine, "FILE_PATHS", None),
        "PROVIDED_BANK_CODES": getattr(engine, "PROVIDED_BANK_CODES", None),
    }

    with _ENGINE_LOCK, tempfile.TemporaryDirectory(prefix="bankstatements_") as tmpdir:
        file_paths: Dict[str, str] = {}
        for acc_id, payload in file_payloads_by_account.items():
            file_path = f"{tmpdir}/{acc_id}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            file_paths[acc_id] = file_path

        engine.COMPANY_NAME = company_name
        engine.COMPANY_KEYWORDS = company_keywords
        engine.RELATED_PARTIES = related_parties
        engine.ACCOUNT_INFO = account_info
        engine.FILE_PATHS = file_paths
        engine.PROVIDED_BANK_CODES = set(provided_bank_codes)

        try:
            return engine.analyze()
        finally:
            for k, v in backup.items():
                try:
                    setattr(engine, k, v)
                except Exception:
                    pass


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Bank Statement → Analysis JSON (v5.2.1)", layout="wide")
st.title("Bank Statement → Analysis JSON (v5.2.1)")
st.caption(
    "Upload one or more **processed bank statement JSON** files, configure the company + accounts, "
    "and download a **single combined analysis JSON** ready for ingestion."
)

with st.sidebar:
    st.header("Company configuration")

    company_name = st.text_input("Company name", value="").strip()

    kw_default = company_name.upper() if company_name else ""
    raw_keywords = st.text_area(
        "Company keywords (comma or newline separated)",
        value=kw_default,
        help="Used for partial matching in inter-account detection (COMPANY_KEYWORDS).",
    )
    company_keywords = _parse_csv_or_lines(raw_keywords, upper=True) if raw_keywords else []

    st.subheader("Related parties (optional)")
    rp_raw = st.text_area(
        "One per line: NAME | Relationship",
        value="",
        help="Example: SISTER COMPANY SDN BHD | Sister Company",
        height=120,
    )
    related_parties = _parse_related_parties(rp_raw)

    st.subheader("Missing-account detection (optional)")
    provided_raw = st.text_input(
        "Provided bank codes override (comma-separated)",
        value="",
        help="If you have statements for banks referenced by codes (e.g., MBB/MAYBANK), list them here to avoid 'missing account' flags.",
    )
    provided_bank_codes = _parse_csv_or_lines(provided_raw, upper=True)

st.header("1) Upload processed statement JSON files")
uploads = st.file_uploader(
    "Upload one or more JSON files",
    type=["json"],
    accept_multiple_files=True,
    help="Each file should follow: summary, monthly_summary, transactions.",
)

if not uploads:
    st.info("Upload at least 1 processed statement JSON file to continue.")
    st.stop()

st.header("2) Map uploads to account metadata")
st.write(
    "For each uploaded file, assign an **account_id** (unique key) and basic metadata. "
    "This app can process multiple statements/accounts in one run."
)

account_info: Dict[str, Dict[str, str]] = {}
payloads_by_account: Dict[str, Dict[str, Any]] = {}
input_errors: List[str] = []

for idx, uf in enumerate(uploads):
    default_acc_id = _sanitize_account_id(uf.name, idx)

    with st.expander(f"{idx+1}. {uf.name}", expanded=(idx == 0)):
        col1, col2, col3 = st.columns([1.2, 1.2, 1.2])

        acc_id = col1.text_input(
            "account_id (unique)",
            value=default_acc_id,
            key=f"acc_id_{idx}_{uf.name}",
            help="This becomes the account identifier in the output JSON.",
        ).strip().upper()

        bank_name = col2.text_input("bank_name", value="", key=f"bank_name_{idx}_{uf.name}").strip()
        account_number = col3.text_input("account_number", value="", key=f"acc_no_{idx}_{uf.name}").strip()

        col4, col5, col6 = st.columns([1.2, 1.2, 1.2])
        account_type = col4.selectbox(
            "account_type", options=["Current", "Savings", "OD"], index=0, key=f"type_{idx}_{uf.name}"
        )
        classification = col5.selectbox(
            "classification",
            options=["PRIMARY", "SECONDARY", "ESCROW", "PROJECT", "OTHER"],
            index=0,
            key=f"class_{idx}_{uf.name}",
        )

        per_acc_codes_raw = col6.text_input(
            "extra provided bank codes (optional)",
            value="",
            key=f"extra_codes_{idx}_{uf.name}",
            help="Comma-separated. Added to PROVIDED_BANK_CODES for missing-account detection.",
        )
        per_acc_codes = _parse_csv_or_lines(per_acc_codes_raw, upper=True)

        # Parse + normalize JSON now (so user sees errors per file)
        try:
            raw_payload = json.loads(uf.getvalue().decode("utf-8"))
            normalized = _normalize_statement_json(raw_payload)
            payloads_by_account[acc_id] = normalized

            st.caption(
                f"Preview: {normalized['summary']['total_transactions']} transactions | "
                f"{normalized['summary']['date_range']}"
            )
        except Exception as e:
            input_errors.append(f"{uf.name}: {e}")
            st.error(f"❌ {e}")
            continue

        if not acc_id:
            input_errors.append(f"{uf.name}: account_id is empty")
        if acc_id in account_info:
            input_errors.append(f"{uf.name}: duplicate account_id '{acc_id}'")

        account_info[acc_id] = {
            "bank_name": bank_name or "Unknown Bank",
            "account_number": account_number or "Unknown",
            "account_holder": company_name or "Unknown Company",
            "account_type": account_type,
            "classification": classification,
        }

        for c in per_acc_codes:
            if c not in provided_bank_codes:
                provided_bank_codes.append(c)

if not company_name:
    input_errors.append("Company name is required (sidebar).")
if not company_keywords:
    input_errors.append("At least 1 company keyword is required (sidebar).")

if input_errors:
    st.warning("Fix the issues above before running the analysis.")
    st.stop()

st.divider()
run = st.button("🚀 Run analysis and generate ingestion JSON", type="primary")

if run:
    with st.spinner("Running deterministic analysis (v5.2.1)..."):
        try:
            result = _run_engine(
                company_name=company_name,
                company_keywords=company_keywords,
                related_parties=related_parties,
                account_info=account_info,
                file_payloads_by_account=payloads_by_account,
                provided_bank_codes=provided_bank_codes,
            )
        except Exception as e:
            st.error(f"Analysis failed: {e}")
            st.stop()

    st.success("Analysis complete.")

    # Quick summary metrics
    try:
        gross = result["consolidated"]["gross"]
        net = result["consolidated"]["net"]
        colA, colB, colC, colD = st.columns(4)
        colA.metric("Gross credits", f"RM {gross['total_credits']:,.2f}")
        colB.metric("Gross debits", f"RM {gross['total_debits']:,.2f}")
        colC.metric("Net business credits", f"RM {net['net_credits']:,.2f}")
        colD.metric("Net business debits", f"RM {net['net_debits']:,.2f}")
    except Exception:
        pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_company = re.sub(r"[^A-Za-z0-9]+", "_", company_name).strip("_").upper() or "COMPANY"
    out_name = f"{safe_company}_analysis_v5_2_1_{ts}.json"
    out_bytes = json.dumps(result, indent=2, ensure_ascii=False).encode("utf-8")

    st.download_button(
        "⬇️ Download analysis JSON",
        data=out_bytes,
        file_name=out_name,
        mime="application/json",
    )

    st.subheader("Result preview")
    st.json(result, expanded=False)
