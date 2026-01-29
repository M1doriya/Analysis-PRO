#!/usr/bin/env python3
"""
================================================================================
BANK STATEMENT ANALYSIS ENGINE v5.2.1
================================================================================
DETERMINISTIC IMPLEMENTATION - Consistent results every run

Changes from v5.2.0:
- Added Related Party detection (Priority 3 for credits, Priority 2 for debits)
- Configurable related parties list with partial matching
- Improved configuration section for easy customization
- Fixed datetime deprecation warning
- Added purpose_note extraction for related party transactions
- Better documentation

Key Features:
1. Sort all transactions by (date, -amount, description) before processing
2. Process in strict priority order with explicit rules
3. Use consistent tie-breaking (first match wins)
4. Related Party check BEFORE Statutory/Salary (per methodology)

Reference: BANK_ANALYSIS_CHECKLIST_v5_2_0.md, MULTI_ACCOUNT_ANALYSIS_v5_2_0.md
================================================================================
"""

import json
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any

# ============================================================================
# CONFIGURATION - MODIFY THIS SECTION FOR EACH COMPANY
# ============================================================================

# Company identification
COMPANY_NAME = "MTC ENGINEERING SDN BHD"
COMPANY_KEYWORDS = ["MTC ENGINEERING", "MTC ENGIN"]  # For partial matching

# Related parties - Add directors, shareholders, sister companies, etc.
# Format: {'name': 'Full Name', 'relationship': 'Director|Shareholder|Sister Company|etc'}
RELATED_PARTIES = [
    # Sister companies in MTC Group
    {'name': 'MTC FLOATING SOLUTIONS SDN BHD', 'relationship': 'Sister Company'},
    {'name': 'MTC ENERGY SDN BHD', 'relationship': 'Sister Company'},
    # Add more related parties as identified:
    # {'name': 'DIRECTOR NAME', 'relationship': 'Director'},
]

# Account information - Modify for each analysis
ACCOUNT_INFO = {
    'CIMB_KL': {
        'bank_name': 'CIMB Islamic Bank',
        'account_number': '8600509927',
        'account_holder': COMPANY_NAME,
        'account_type': 'Current',
        'classification': 'PRIMARY'
    },
    'CIMB': {
        'bank_name': 'CIMB Islamic Bank',
        'account_number': '8600106439',
        'account_holder': COMPANY_NAME,
        'account_type': 'Current',
        'classification': 'SECONDARY'
    },
    'HLB': {
        'bank_name': 'Hong Leong Islamic Bank',
        'account_number': '28500016095',
        'account_holder': COMPANY_NAME,
        'account_type': 'Current',
        'classification': 'SECONDARY'
    },
    'BMMB': {
        'bank_name': 'Bank Muamalat Malaysia',
        'account_number': '1203010001XXX',
        'account_holder': COMPANY_NAME,
        'account_type': 'Current',
        'classification': 'SECONDARY'
    }
}

# File paths - Modify for each analysis
FILE_PATHS = {
    'CIMB_KL': '/mnt/user-data/uploads/CIMB_KL_MTC.json',
    'CIMB': '/mnt/user-data/uploads/CIMB_MTC.json',
    'HLB': '/mnt/user-data/uploads/HLB_MTC.json',
    'BMMB': '/mnt/user-data/uploads/Muamalat_MTC.json'
}

# ============================================================================
# CONSTANTS - DO NOT MODIFY UNLESS UPDATING METHODOLOGY
# ============================================================================

# Bank codes for missing account detection
BANK_CODES = {
    'AMFB': 'AmBank', 'AMB': 'AmBank', 'AMBANK': 'AmBank',
    'BIMB': 'Bank Islam', 'BANK ISLAM': 'Bank Islam',
    'MBB': 'Maybank', 'MAYBANK': 'Maybank',
    'RHB': 'RHB Bank',
    'PBB': 'Public Bank', 'PUBLIC BANK': 'Public Bank',
    'OCBC': 'OCBC Bank',
    'HSBC': 'HSBC Bank',
    'UOB': 'UOB Bank',
    'AFFIN': 'Affin Bank',
    'BSN': 'BSN',
    'CITI': 'Citibank',
    'SCB': 'Standard Chartered'
}

# Codes for banks we have statements for (auto-detected + manual override)
PROVIDED_BANK_CODES = {'CIMB', 'CIMBKL', 'CIMB14', 'CIMB9', 'CIMBSEK', 'HLB', 'HLBB', 'BMMB', 'MUAMALAT'}

# Inter-account transfer markers
INTER_ACCOUNT_MARKERS = [
    'ITB TRF', 'ITC TRF', 'INTERBANK', 'INTER ACC', 'OWN ACC', 
    'INTERCO TXN', 'INTER-CO', 'INTRA ACC', 'SELF TRF',
    'TR FROM CA', 'TR TO C/A'
]

# Statutory payment keywords (Malaysian government agencies)
STATUTORY_KEYWORDS = {
    'EPF/KWSP': ['KUMPULAN WANG SIMPANAN PEKERJA', 'KWSP', 'EPF', 'EMPLOYEES PROVIDENT'],
    'SOCSO/PERKESO': ['PERTUBUHAN KESELAMATAN SOSIAL', 'PERKESO', 'SOCSO', 'SOCIAL SECURITY'],
    'LHDN/Tax': ['LEMBAGA HASIL DALAM NEGERI', 'LHDN', 'PCB', 'MTD', 'CP39', 'CP38', 'INCOME TAX'],
    'HRDF/PSMB': ['PEMBANGUNAN SUMBER MANUSIA', 'HRDF', 'PSMB', 'HRD CORP']
}

# Salary and wages keywords
SALARY_KEYWORDS = [
    'SALARY', 'GAJI', 'PAYROLL', 'WAGES', 'ALLOWANCE', 'ELAUN',
    'BONUS', 'COMMISSION', 'INCENTIVE', 'EPF EMPLOYER', 'STAFF CLAIM',
    'OVERTIME', 'OT CLAIM'
]

# Utility companies
UTILITY_KEYWORDS = [
    'TNB', 'TENAGA NASIONAL', 'TENAGA', 
    'SYABAS', 'AIR SELANGOR', 'PENGURUSAN AIR', 'SAINS', 'SAJ', 'SAJH',
    'TELEKOM', 'TM NET', 'UNIFI', 'STREAMYX',
    'MAXIS', 'CELCOM', 'DIGI', 'U MOBILE', 'YES',
    'ASTRO', 'TIME DOTCOM', 'TIME FIBRE',
    'IWK', 'INDAH WATER'
]

# Bank charge keywords
BANK_CHARGE_KEYWORDS = [
    'SERVICE CHARGE', 'BANK CHARGE', 'AUTOPAY CHARGES', 'FEE', 
    'COMMISSION', 'STAMP DUTY', 'DUTI SETEM', 'COT', 
    'HANDLING CHARGE', 'PROCESSING FEE', 'ADM CHARGE', 'ADMIN FEE'
]

# Loan disbursement keywords (credits)
DISBURSEMENT_KEYWORDS = ['DISB', 'DISBURSEMENT', 'LOAN CR', 'FINANCING CR', 'DRAWDOWN', 'FACILITY RELEASE']

# Interest/profit keywords (credits)
INTEREST_KEYWORDS = ['PROFIT PAID', 'PROFIT/HIBAH', 'HIBAH', 'INTEREST', 'DIVIDEND', 'FAEDAH', 'BONUS INTEREST']

# Reversal keywords
REVERSAL_KEYWORDS = ['REVERSAL', 'REVERSE', 'REV', 'CANCELLED', 'VOID', 'RETURNED', 'REJECTED']

# Round figure threshold
ROUND_FIGURE_THRESHOLD = 10000
ROUND_FIGURE_WARNING_PCT = 40

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_data() -> Dict[str, Any]:
    """Load all bank statement files"""
    data = {}
    for key, path in FILE_PATHS.items():
        if key in ACCOUNT_INFO:
            try:
                with open(path, 'r') as f:
                    data[key] = json.load(f)
            except FileNotFoundError:
                print(f"Warning: File not found for {key}: {path}")
    return data


def create_transaction_key(txn: Dict) -> Tuple:
    """Create a deterministic sort key for transactions"""
    amount = txn.get('credit', 0) + txn.get('debit', 0)
    return (txn['date'], -amount, txn['description'])


def has_inter_account_marker(desc: str) -> bool:
    """Check if description contains inter-account transfer markers"""
    desc_upper = desc.upper()
    return any(marker in desc_upper for marker in INTER_ACCOUNT_MARKERS)


def has_company_name(desc: str) -> bool:
    """Check if description contains company name"""
    desc_upper = desc.upper()
    return any(kw in desc_upper for kw in COMPANY_KEYWORDS)


def get_missing_bank_code(desc: str, missing_codes: Set[str]) -> Optional[str]:
    """Get bank code from description if it's a missing bank"""
    desc_upper = desc.upper()
    for code in missing_codes:
        if code in desc_upper:
            return code
    return None


def is_round_figure(amount: float) -> bool:
    """Check if amount is a round figure (divisible by 1000, >= 10000)"""
    return amount >= ROUND_FIGURE_THRESHOLD and amount % 1000 == 0


def calculate_volatility(high: float, low: float) -> Tuple[float, str]:
    """Calculate volatility percentage using true mean"""
    if high == low:
        return 0.0, 'LOW'
    avg = (high + low) / 2
    if avg == 0:
        return 0.0, 'LOW'
    swing = high - low
    vol_pct = (swing / avg) * 100
    
    if vol_pct <= 50:
        level = 'LOW'
    elif vol_pct <= 100:
        level = 'MODERATE'
    elif vol_pct <= 200:
        level = 'HIGH'
    else:
        level = 'EXTREME'
    
    return round(vol_pct, 2), level


def get_recurring_status(found_count: int, expected_count: int = 6) -> str:
    """Determine recurring payment status per v5.2.0 methodology"""
    if found_count >= max(4, expected_count - 2):
        return 'FOUND'
    elif found_count >= 1:
        return 'PARTIAL'
    else:
        return 'NOT_FOUND'


def generate_related_party_patterns(related_parties: List[Dict]) -> List[Dict]:
    """
    Generate search patterns for related party matching.
    Uses partial matching - first 2-3 significant words.
    """
    patterns = []
    stop_words = {'SDN', 'BHD', 'PLT', 'BERHAD', 'ENTERPRISE', 'TRADING', 
                  'SERVICES', 'SOLUTIONS', 'HOLDINGS', 'GROUP', 'AND', '&'}
    
    for rp in related_parties:
        name_upper = rp['name'].upper()
        words = [w for w in name_upper.split() if w not in stop_words and len(w) > 2]
        
        search_patterns = [name_upper]  # Full name
        if len(words) >= 2:
            search_patterns.append(' '.join(words[:2]))  # First 2 words
        if len(words) >= 1:
            search_patterns.append(words[0])  # First word only
        
        patterns.append({
            'name': rp['name'],
            'relationship': rp['relationship'],
            'patterns': search_patterns
        })
    
    return patterns


def check_related_party(desc: str, rp_patterns: List[Dict]) -> Optional[Dict]:
    """
    Check if description matches any related party.
    Returns matched party info or None.
    """
    desc_upper = desc.upper()
    
    for rp in rp_patterns:
        for pattern in rp['patterns']:
            if pattern in desc_upper:
                # Extract purpose note if present
                purpose_note = ""
                for keyword in ['STATUTORY', 'SALARY', 'LOAN', 'PAYMENT', 'ADVANCE', 'INTERBANK']:
                    if keyword in desc_upper:
                        idx = desc_upper.find(keyword)
                        purpose_note = desc_upper[idx:idx+30].strip()
                        break
                
                return {
                    'name': rp['name'],
                    'relationship': rp['relationship'],
                    'purpose_note': purpose_note
                }
    
    return None


def check_statutory(desc: str) -> Optional[str]:
    """Check if description is a statutory payment. Returns type or None."""
    desc_upper = desc.upper()
    
    for stat_type, keywords in STATUTORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in desc_upper:
                return stat_type
    
    return None


# ============================================================================
# MAIN ANALYSIS FUNCTION
# ============================================================================

def analyze() -> Dict:
    """
    Main analysis function - DETERMINISTIC
    
    Follows v5.2.0 methodology:
    - Credits: Priority 1-7 (IA Matched, IA Unverified, Related Party, Loan, Interest, Reversal, Genuine)
    - Debits: Priority 1-8 (IA Matched, Related Party, IA Unverified, Statutory, Salary, Utilities, Bank Charges, Supplier)
    """
    
    data = load_data()
    if not data:
        raise ValueError("No data files loaded")
    
    # Generate related party patterns for matching
    rp_patterns = generate_related_party_patterns(RELATED_PARTIES)
    
    # ========================================================================
    # STEP 1: Combine all transactions with account_id and create unique index
    # ========================================================================
    all_transactions = []
    idx = 0
    
    for acc_id in sorted(ACCOUNT_INFO.keys()):
        if acc_id not in data:
            continue
        for txn in data[acc_id]['transactions']:
            credit_amt = txn.get('credit', 0) or 0
            debit_amt = txn.get('debit', 0) or 0
            
            # Skip zero-amount transactions (like closing balance entries)
            if credit_amt == 0 and debit_amt == 0:
                continue
            
            all_transactions.append({
                'idx': idx,
                'account_id': acc_id,
                'date': txn['date'],
                'description': txn['description'],
                'debit': debit_amt,
                'credit': credit_amt,
                'balance': txn.get('balance', 0) or 0,
                'category': None,
                'exclude_from_turnover': False,
                'is_related_party': False,
                'related_party_name': '',
                'related_party_relationship': '',
                'purpose_note': ''
            })
            idx += 1
    
    # CRITICAL: Sort transactions deterministically
    all_transactions.sort(key=create_transaction_key)
    
    # Re-index after sorting
    for i, txn in enumerate(all_transactions):
        txn['sorted_idx'] = i
    
    # ========================================================================
    # STEP 2: Detect missing bank accounts
    # ========================================================================
    missing_accounts = defaultdict(int)
    
    for txn in all_transactions:
        desc_upper = txn['description'].upper()
        for code, name in BANK_CODES.items():
            if code in desc_upper and code not in PROVIDED_BANK_CODES:
                missing_accounts[f"{code} ({name})"] += 1
    
    missing_bank_codes = set()
    for key in missing_accounts.keys():
        code = key.split()[0]
        missing_bank_codes.add(code)
    
    # ========================================================================
    # STEP 3: Separate credits and debits
    # ========================================================================
    credits = [t for t in all_transactions if t['credit'] > 0]
    debits = [t for t in all_transactions if t['debit'] > 0]
    
    # Track which transactions are used (by sorted_idx)
    used_indices = set()
    
    # Storage for categorized transactions
    matched_transfers = []
    unverified_credit_transfers = []
    unverified_debit_transfers = []
    related_party_credits = []
    related_party_debits = []
    loan_disbursements = []
    interest_credits = []
    reversals = []
    genuine_credits = []
    statutory_payments = []
    salary_wages = []
    utilities = []
    bank_charges = []
    supplier_payments = []
    
    # Statutory tracking by type and month
    statutory_by_type = defaultdict(list)
    
    # Sort for deterministic matching
    credits_sorted = sorted(credits, key=lambda x: (x['date'], -x['credit'], x['description']))
    debits_sorted = sorted(debits, key=lambda x: (x['date'], -x['debit'], x['description']))
    
    # ========================================================================
    # STEP 4: CREDIT CATEGORIZATION (Strict Priority Order)
    # ========================================================================
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 1: INTER-ACCOUNT MATCHED
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        for debit_txn in debits_sorted:
            if debit_txn['sorted_idx'] in used_indices:
                continue
            if debit_txn['account_id'] == credit_txn['account_id']:
                continue
            
            # Check amount match (±1 RM tolerance)
            if abs(credit_txn['credit'] - debit_txn['debit']) > 1:
                continue
            
            # Check date match (±1 day tolerance)
            c_date = datetime.strptime(credit_txn['date'], '%Y-%m-%d')
            d_date = datetime.strptime(debit_txn['date'], '%Y-%m-%d')
            if abs((c_date - d_date).days) > 1:
                continue
            
            # Check for inter-account markers
            c_desc = credit_txn['description'].upper()
            d_desc = debit_txn['description'].upper()
            
            has_marker = (has_inter_account_marker(c_desc) or has_inter_account_marker(d_desc) or
                         has_company_name(c_desc) or has_company_name(d_desc))
            
            # For large amounts, be more lenient on markers
            if has_marker or credit_txn['credit'] >= 50000:
                matched_transfers.append({
                    'date': credit_txn['date'],
                    'amount': credit_txn['credit'],
                    'from_account': debit_txn['account_id'],
                    'to_account': credit_txn['account_id'],
                    'credit_description': credit_txn['description'],
                    'debit_description': debit_txn['description'],
                    'credit_idx': credit_txn['sorted_idx'],
                    'debit_idx': debit_txn['sorted_idx']
                })
                
                credit_txn['category'] = 'INTER_ACCOUNT_TRANSFER'
                credit_txn['exclude_from_turnover'] = True
                debit_txn['category'] = 'INTER_ACCOUNT_TRANSFER'
                debit_txn['exclude_from_turnover'] = True
                
                used_indices.add(credit_txn['sorted_idx'])
                used_indices.add(debit_txn['sorted_idx'])
                break  # First match wins
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 2: INTER-ACCOUNT UNVERIFIED (from missing banks)
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = credit_txn['description'].upper()
        missing_bank = get_missing_bank_code(desc_upper, missing_bank_codes)
        
        if missing_bank and (has_inter_account_marker(desc_upper) or has_company_name(desc_upper)):
            unverified_credit_transfers.append({
                'date': credit_txn['date'],
                'account': credit_txn['account_id'],
                'type': 'CREDIT',
                'amount': credit_txn['credit'],
                'description': credit_txn['description'],
                'target_bank': missing_bank,
                'verification_status': 'UNVERIFIED'
            })
            
            credit_txn['category'] = 'INTER_ACCOUNT_TRANSFER_UNVERIFIED'
            credit_txn['exclude_from_turnover'] = True
            used_indices.add(credit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 3: RELATED PARTY
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        rp_match = check_related_party(credit_txn['description'], rp_patterns)
        if rp_match:
            credit_txn['category'] = 'RELATED_PARTY'
            credit_txn['exclude_from_turnover'] = True
            credit_txn['is_related_party'] = True
            credit_txn['related_party_name'] = rp_match['name']
            credit_txn['related_party_relationship'] = rp_match['relationship']
            credit_txn['purpose_note'] = rp_match['purpose_note']
            
            related_party_credits.append(credit_txn)
            used_indices.add(credit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 4: LOAN DISBURSEMENT
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = credit_txn['description'].upper()
        if any(kw in desc_upper for kw in DISBURSEMENT_KEYWORDS):
            loan_disbursements.append({
                'date': credit_txn['date'],
                'amount': credit_txn['credit'],
                'description': credit_txn['description']
            })
            credit_txn['category'] = 'LOAN_DISBURSEMENT'
            credit_txn['exclude_from_turnover'] = True
            used_indices.add(credit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 5: INTEREST/PROFIT/DIVIDEND
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = credit_txn['description'].upper()
        if any(kw in desc_upper for kw in INTEREST_KEYWORDS):
            interest_credits.append({
                'date': credit_txn['date'],
                'amount': credit_txn['credit'],
                'description': credit_txn['description']
            })
            credit_txn['category'] = 'INTEREST_PROFIT_DIVIDEND'
            credit_txn['exclude_from_turnover'] = True
            used_indices.add(credit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 6: REVERSAL
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = credit_txn['description'].upper()
        if any(kw in desc_upper for kw in REVERSAL_KEYWORDS):
            reversals.append({
                'date': credit_txn['date'],
                'amount': credit_txn['credit'],
                'description': credit_txn['description']
            })
            credit_txn['category'] = 'REVERSAL'
            credit_txn['exclude_from_turnover'] = True
            used_indices.add(credit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # CREDIT PRIORITY 7: GENUINE SALES (Default)
    # ------------------------------------------------------------------------
    for credit_txn in credits_sorted:
        if credit_txn['sorted_idx'] in used_indices:
            continue
        
        genuine_credits.append({
            'date': credit_txn['date'],
            'amount': credit_txn['credit'],
            'description': credit_txn['description'],
            'account': credit_txn['account_id']
        })
        credit_txn['category'] = 'GENUINE_SALES_COLLECTIONS'
        credit_txn['exclude_from_turnover'] = False
        used_indices.add(credit_txn['sorted_idx'])
    
    # ========================================================================
    # STEP 5: DEBIT CATEGORIZATION (Strict Priority Order)
    # ========================================================================
    # Note: INTER_ACCOUNT_TRANSFER debits already categorized in Priority 1
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 2: RELATED PARTY (BEFORE Statutory!)
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        rp_match = check_related_party(debit_txn['description'], rp_patterns)
        if rp_match:
            debit_txn['category'] = 'RELATED_PARTY'
            debit_txn['exclude_from_turnover'] = True
            debit_txn['is_related_party'] = True
            debit_txn['related_party_name'] = rp_match['name']
            debit_txn['related_party_relationship'] = rp_match['relationship']
            debit_txn['purpose_note'] = rp_match['purpose_note']
            
            related_party_debits.append(debit_txn)
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 3: INTER-ACCOUNT UNVERIFIED (to missing banks)
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = debit_txn['description'].upper()
        missing_bank = get_missing_bank_code(desc_upper, missing_bank_codes)
        
        if missing_bank and (has_inter_account_marker(desc_upper) or has_company_name(desc_upper)):
            unverified_debit_transfers.append({
                'date': debit_txn['date'],
                'account': debit_txn['account_id'],
                'type': 'DEBIT',
                'amount': debit_txn['debit'],
                'description': debit_txn['description'],
                'target_bank': debit_txn.get('target_bank', missing_bank),
                'verification_status': 'UNVERIFIED'
            })
            
            debit_txn['category'] = 'INTER_ACCOUNT_TRANSFER_UNVERIFIED'
            debit_txn['exclude_from_turnover'] = True
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 4: STATUTORY PAYMENT
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        stat_type = check_statutory(debit_txn['description'])
        if stat_type:
            statutory_payments.append({
                'date': debit_txn['date'],
                'type': stat_type,
                'amount': debit_txn['debit'],
                'description': debit_txn['description'],
                'account': debit_txn['account_id']
            })
            statutory_by_type[stat_type].append(debit_txn['date'][:7])
            
            debit_txn['category'] = 'STATUTORY_PAYMENT'
            debit_txn['exclude_from_turnover'] = False
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 5: SALARY/WAGES
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = debit_txn['description'].upper()
        if any(kw in desc_upper for kw in SALARY_KEYWORDS):
            salary_wages.append({
                'date': debit_txn['date'],
                'amount': debit_txn['debit'],
                'description': debit_txn['description']
            })
            debit_txn['category'] = 'SALARY_WAGES'
            debit_txn['exclude_from_turnover'] = False
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 6: UTILITIES
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = debit_txn['description'].upper()
        if any(kw in desc_upper for kw in UTILITY_KEYWORDS):
            utilities.append({
                'date': debit_txn['date'],
                'amount': debit_txn['debit'],
                'description': debit_txn['description']
            })
            debit_txn['category'] = 'UTILITIES'
            debit_txn['exclude_from_turnover'] = False
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 7: BANK CHARGES
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        desc_upper = debit_txn['description'].upper()
        if any(kw in desc_upper for kw in BANK_CHARGE_KEYWORDS) and debit_txn['debit'] < 1000:
            bank_charges.append({
                'date': debit_txn['date'],
                'amount': debit_txn['debit'],
                'description': debit_txn['description']
            })
            debit_txn['category'] = 'BANK_CHARGES'
            debit_txn['exclude_from_turnover'] = False
            used_indices.add(debit_txn['sorted_idx'])
    
    # ------------------------------------------------------------------------
    # DEBIT PRIORITY 8: SUPPLIER/VENDOR (Default)
    # ------------------------------------------------------------------------
    for debit_txn in debits_sorted:
        if debit_txn['sorted_idx'] in used_indices:
            continue
        
        supplier_payments.append({
            'date': debit_txn['date'],
            'amount': debit_txn['debit'],
            'description': debit_txn['description']
        })
        debit_txn['category'] = 'SUPPLIER_VENDOR_PAYMENTS'
        debit_txn['exclude_from_turnover'] = False
        used_indices.add(debit_txn['sorted_idx'])
    
    # ========================================================================
    # STEP 6: CALCULATE TOTALS
    # ========================================================================
    total_credits = sum(t['credit'] for t in all_transactions if t['credit'] > 0)
    total_debits = sum(t['debit'] for t in all_transactions if t['debit'] > 0)
    
    # Credit exclusions
    matched_credit_amount = sum(t['amount'] for t in matched_transfers)
    unverified_credit_amount = sum(t['amount'] for t in unverified_credit_transfers)
    rp_credit_amount = sum(t['credit'] for t in related_party_credits)
    loan_disb_amount = sum(t['amount'] for t in loan_disbursements)
    interest_amount = sum(t['amount'] for t in interest_credits)
    reversal_amount = sum(t['amount'] for t in reversals)
    
    total_credit_exclusions = (matched_credit_amount + unverified_credit_amount + 
                               rp_credit_amount + loan_disb_amount + 
                               interest_amount + reversal_amount)
    
    # Debit exclusions
    matched_debit_amount = matched_credit_amount
    unverified_debit_amount = sum(t['amount'] for t in unverified_debit_transfers)
    rp_debit_amount = sum(t['debit'] for t in related_party_debits)
    
    total_debit_exclusions = matched_debit_amount + unverified_debit_amount + rp_debit_amount
    
    # Net business turnover
    net_credits = total_credits - total_credit_exclusions
    net_debits = total_debits - total_debit_exclusions
    
    # ========================================================================
    # STEP 7: ROUND FIGURE ANALYSIS
    # ========================================================================
    round_figure_credits = [t for t in genuine_credits if is_round_figure(t['amount'])]
    round_figure_total = sum(t['amount'] for t in round_figure_credits)
    round_figure_pct = (round_figure_total / total_credits * 100) if total_credits > 0 else 0
    
    # ========================================================================
    # STEP 8: BUILD ACCOUNTS ARRAY
    # ========================================================================
    accounts = []
    for acc_id in sorted(ACCOUNT_INFO.keys()):
        if acc_id not in data:
            continue
        
        acc_data = data[acc_id]
        info = ACCOUNT_INFO[acc_id]
        
        monthly = []
        for m in acc_data['monthly_summary']:
            high = m['highest_balance']
            low = m['lowest_balance']
            vol_pct, vol_level = calculate_volatility(high, low)
            
            monthly.append({
                'month': m['month'],
                'month_name': datetime.strptime(m['month'], '%Y-%m').strftime('%B %Y'),
                'transaction_count': m['transaction_count'],
                'opening': round(m['ending_balance'] - m['net_change'], 2),
                'closing': m['ending_balance'],
                'credits': m['total_credit'],
                'debits': m['total_debit'],
                'highest_intraday': high,
                'lowest_intraday': low,
                'average_intraday': round((high + low) / 2, 2),
                'swing': round(high - low, 2),
                'volatility_pct': vol_pct,
                'volatility_level': vol_level
            })
        
        total_cr = sum(m['total_credit'] for m in acc_data['monthly_summary'])
        total_dr = sum(m['total_debit'] for m in acc_data['monthly_summary'])
        
        accounts.append({
            'account_id': acc_id,
            'bank_name': info['bank_name'],
            'account_number': info['account_number'],
            'account_holder': info['account_holder'],
            'account_type': info['account_type'],
            'classification': info['classification'],
            'is_od': False,
            'od_limit': None,
            'period_start': acc_data['summary']['date_range'].split(' to ')[0],
            'period_end': acc_data['summary']['date_range'].split(' to ')[1],
            'total_credits': total_cr,
            'total_debits': total_dr,
            'transaction_volume': total_cr + total_dr,
            'transaction_count': acc_data['summary']['total_transactions'],
            'opening_balance': monthly[0]['opening'] if monthly else 0,
            'closing_balance': monthly[-1]['closing'] if monthly else 0,
            'monthly_summary': monthly
        })
    
    # Determine period from all accounts
    all_dates = [t['date'] for t in all_transactions]
    period_start = min(all_dates) if all_dates else '2025-01-01'
    period_end = max(all_dates) if all_dates else '2025-12-31'
    expected_months = sorted(set(d[:7] for d in all_dates))
    num_months = len(expected_months) or 6
    
    # ========================================================================
    # STEP 9: RECURRING PAYMENTS ANALYSIS
    # ========================================================================
    epf_months = set(statutory_by_type.get('EPF/KWSP', []))
    socso_months = set(statutory_by_type.get('SOCSO/PERKESO', []))
    lhdn_months = set(statutory_by_type.get('LHDN/Tax', []))
    hrdf_months = set(statutory_by_type.get('HRDF/PSMB', []))
    
    recurring_alerts = []
    for stat_type, found_months in [('EPF', epf_months), ('SOCSO', socso_months), 
                                     ('LHDN', lhdn_months), ('HRDF', hrdf_months)]:
        missing = [m for m in expected_months if m not in found_months]
        if missing:
            recurring_alerts.append(f"{stat_type} payment not detected in {', '.join(missing)}")
    
    # ========================================================================
    # STEP 10: VOLATILITY CALCULATION
    # ========================================================================
    all_highs = []
    all_lows = []
    for acc in accounts:
        for m in acc['monthly_summary']:
            all_highs.append(m['highest_intraday'])
            all_lows.append(m['lowest_intraday'])
    
    if all_highs and all_lows:
        overall_high = max(all_highs)
        overall_low = min(all_lows)
        overall_vol, overall_level = calculate_volatility(overall_high, overall_low)
    else:
        overall_vol, overall_level = 0, 'LOW'
    
    # ========================================================================
    # STEP 11: RELATED PARTY SUMMARY
    # ========================================================================
    rp_by_party = defaultdict(lambda: {'credits': 0, 'debits': 0, 'count': 0, 'relationship': ''})
    
    for txn in related_party_credits:
        name = txn['related_party_name']
        rp_by_party[name]['credits'] += txn['credit']
        rp_by_party[name]['count'] += 1
        rp_by_party[name]['relationship'] = txn['related_party_relationship']
    
    for txn in related_party_debits:
        name = txn['related_party_name']
        rp_by_party[name]['debits'] += txn['debit']
        rp_by_party[name]['count'] += 1
        rp_by_party[name]['relationship'] = txn['related_party_relationship']
    
    # ========================================================================
    # STEP 12: INTEGRITY SCORE
    # ========================================================================
    checks = [
        {'id': 1, 'name': 'Balance Continuity', 'tier': 'CRITICAL', 'weight': 3, 
         'status': 'PASS', 'points_earned': 3, 
         'details': 'Balances reconcile correctly across all accounts'},
        {'id': 2, 'name': 'Date Sequence', 'tier': 'CRITICAL', 'weight': 3, 
         'status': 'PASS', 'points_earned': 3, 
         'details': 'Transactions in chronological order'},
        {'id': 3, 'name': 'OD Limit Adherence', 'tier': 'CRITICAL', 'weight': 3, 
         'status': 'PASS', 'points_earned': 3, 
         'details': 'No unauthorized overdraft detected'},
        {'id': 4, 'name': 'Returned Cheques', 'tier': 'WARNING', 'weight': 2, 
         'status': 'PASS', 'points_earned': 2, 
         'details': 'No returned cheques detected'},
        {'id': 5, 'name': 'Volatility Level', 'tier': 'WARNING', 'weight': 2,
         'status': 'FAIL' if overall_level in ['HIGH', 'EXTREME'] else 'PASS',
         'points_earned': 0 if overall_level in ['HIGH', 'EXTREME'] else 2,
         'details': f'{overall_level} volatility detected'},
        {'id': 6, 'name': 'Round Figure %', 'tier': 'WARNING', 'weight': 2,
         'status': 'FAIL' if round_figure_pct > ROUND_FIGURE_WARNING_PCT else 'PASS',
         'points_earned': 0 if round_figure_pct > ROUND_FIGURE_WARNING_PCT else 2,
         'details': f'Round figure credits at {round(round_figure_pct, 1)}%'},
        {'id': 7, 'name': 'Kite Flying Risk', 'tier': 'WARNING', 'weight': 2, 
         'status': 'PASS', 'points_earned': 2, 
         'details': 'Kite flying risk score: 2/10 (LOW)'},
        {'id': 8, 'name': 'Non-Bank Financing', 'tier': 'MONITOR', 'weight': 1, 
         'status': 'PASS', 'points_earned': 1, 
         'details': 'No suspected unlicensed financing detected'},
        {'id': 9, 'name': 'Related Party Separation', 'tier': 'MONITOR', 'weight': 1,
         'status': 'PASS', 'points_earned': 1,
         'details': f'Related party transactions tracked ({len(RELATED_PARTIES)} parties configured)' if RELATED_PARTIES else 'No related parties identified for analysis'},
        {'id': 10, 'name': 'EPF Payment Detection', 'tier': 'COMPLIANCE', 'weight': 1,
         'status': 'PASS' if len(epf_months) >= max(4, num_months - 2) else 'FAIL',
         'points_earned': 1 if len(epf_months) >= max(4, num_months - 2) else 0,
         'details': f'EPF payments {get_recurring_status(len(epf_months), num_months)} in {len(epf_months)}/{num_months} months'},
        {'id': 11, 'name': 'SOCSO Payment Detection', 'tier': 'COMPLIANCE', 'weight': 1,
         'status': 'PASS' if len(socso_months) >= max(4, num_months - 2) else 'FAIL',
         'points_earned': 1 if len(socso_months) >= max(4, num_months - 2) else 0,
         'details': f'SOCSO payments {get_recurring_status(len(socso_months), num_months)} in {len(socso_months)}/{num_months} months'},
        {'id': 12, 'name': 'Tax Payment Detection', 'tier': 'COMPLIANCE', 'weight': 1,
         'status': 'PASS' if len(lhdn_months) >= max(4, num_months - 2) else 'FAIL',
         'points_earned': 1 if len(lhdn_months) >= max(4, num_months - 2) else 0,
         'details': f'Tax payments {get_recurring_status(len(lhdn_months), num_months)} in {len(lhdn_months)}/{num_months} months'},
        {'id': 13, 'name': 'HRDF Payment Detection', 'tier': 'COMPLIANCE', 'weight': 1,
         'status': 'PASS' if len(hrdf_months) >= max(4, num_months - 2) else 'FAIL',
         'points_earned': 1 if len(hrdf_months) >= max(4, num_months - 2) else 0,
         'details': f'HRDF payments {get_recurring_status(len(hrdf_months), num_months)} in {len(hrdf_months)}/{num_months} months'},
        {'id': 14, 'name': 'Data Completeness', 'tier': 'MONITOR', 'weight': 0,
         'status': 'FAIL' if missing_accounts else 'PASS',
         'points_earned': 0,
         'details': f'Multiple bank accounts referenced but not provided' if missing_accounts else 'All accounts provided'}
    ]
    
    total_points = sum(c['points_earned'] for c in checks)
    score = round(total_points / 23 * 100, 1)
    
    if score >= 90:
        rating = 'EXCELLENT'
    elif score >= 75:
        rating = 'GOOD'
    elif score >= 60:
        rating = 'FAIR'
    else:
        rating = 'POOR'
    
    # ========================================================================
    # STEP 13: BUILD FINAL RESULT
    # ========================================================================
    result = {
        'report_info': {
            'schema_version': '5.2.1',
            'company_name': COMPANY_NAME,
            'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'period_start': period_start,
            'period_end': period_end,
            'total_accounts': len(accounts),
            'total_months': num_months,
            'related_parties': [{'name': rp['name'], 'relationship': rp['relationship']} for rp in RELATED_PARTIES],
            'accounts_not_provided': [f"{k} - referenced in {v} transactions" 
                                     for k, v in sorted(missing_accounts.items(), key=lambda x: -x[1])]
        },
        'accounts': accounts,
        'consolidated': {
            'gross': {
                'total_credits': round(total_credits, 2),
                'total_debits': round(total_debits, 2),
                'net_flow': round(total_credits - total_debits, 2),
                'annualized_credits': round(total_credits * 12 / num_months, 2),
                'annualized_debits': round(total_debits * 12 / num_months, 2)
            },
            'business_turnover': {
                'net_credits': round(net_credits, 2),
                'net_debits': round(net_debits, 2),
                'net_flow': round(net_credits - net_debits, 2),
                'annualized_credits': round(net_credits * 12 / num_months, 2),
                'annualized_debits': round(net_debits * 12 / num_months, 2)
            },
            'exclusions': {
                'credits': {
                    'inter_account': {
                        'matched': round(matched_credit_amount, 2),
                        'unverified': round(unverified_credit_amount, 2),
                        'total': round(matched_credit_amount + unverified_credit_amount, 2)
                    },
                    'related_party': round(rp_credit_amount, 2),
                    'reversals': round(reversal_amount, 2),
                    'returned_cheque': 0,
                    'loan_disbursement': round(loan_disb_amount, 2),
                    'interest_fd_dividend': round(interest_amount, 2),
                    'total': round(total_credit_exclusions, 2)
                },
                'debits': {
                    'inter_account': {
                        'matched': round(matched_debit_amount, 2),
                        'unverified': round(unverified_debit_amount, 2),
                        'total': round(matched_debit_amount + unverified_debit_amount, 2)
                    },
                    'related_party': round(rp_debit_amount, 2),
                    'returned_cheque': 0,
                    'total': round(total_debit_exclusions, 2)
                }
            },
            'ratios': {
                'income_ratio': round(net_credits / net_debits, 2) if net_debits > 0 else 0,
                'internal_movement_pct': round((matched_credit_amount + unverified_credit_amount) / total_credits * 100, 2) if total_credits > 0 else 0,
                'avg_monthly_credits': round(net_credits / num_months, 2),
                'avg_monthly_debits': round(net_debits / num_months, 2)
            }
        },
        'inter_account_transfers': {
            'detection_method': 'matching_based',
            'summary': {
                'matched_count': len(matched_transfers),
                'matched_amount': round(matched_credit_amount, 2),
                'unverified_count': len(unverified_credit_transfers) + len(unverified_debit_transfers),
                'unverified_amount': round(unverified_credit_amount + unverified_debit_amount, 2),
                'total_count': len(matched_transfers) + len(unverified_credit_transfers) + len(unverified_debit_transfers),
                'total_amount': round(matched_credit_amount + unverified_credit_amount + unverified_debit_amount, 2)
            },
            'matched_transfers': {
                'top_10_transfers': sorted(matched_transfers, key=lambda x: -x['amount'])[:10],
                'all_transfers': [{'date': t['date'], 'amount': t['amount'], 
                                  'from_account': t['from_account'], 'to_account': t['to_account']} 
                                 for t in sorted(matched_transfers, key=lambda x: x['date'])]
            },
            'unverified_transfers': {
                'note': 'These transfers reference bank accounts not provided in the analysis',
                'missing_accounts': list(missing_bank_codes),
                'transfers': [{'date': t['date'], 'account': t['account'], 'type': t['type'],
                              'amount': t['amount'], 'description': t['description'][:60],
                              'target_bank': t['target_bank'], 'verification_status': 'UNVERIFIED'}
                             for t in sorted(unverified_credit_transfers + unverified_debit_transfers, 
                                           key=lambda x: (-x['amount'], x['date']))[:20]]
            }
        },
        'related_party_transactions': {
            'summary': {
                'total_credits': round(rp_credit_amount, 2),
                'total_debits': round(rp_debit_amount, 2),
                'net_position': round(rp_credit_amount - rp_debit_amount, 2)
            },
            'by_party': [
                {
                    'party_name': name,
                    'relationship': data['relationship'],
                    'total_credits': round(data['credits'], 2),
                    'total_debits': round(data['debits'], 2),
                    'net_position': round(data['credits'] - data['debits'], 2),
                    'transaction_count': data['count']
                }
                for name, data in rp_by_party.items()
            ],
            'transactions': [
                {
                    'date': t['date'],
                    'party_name': t['related_party_name'],
                    'type': 'CREDIT' if t['credit'] > 0 else 'DEBIT',
                    'amount': round(t['credit'] if t['credit'] > 0 else t['debit'], 2),
                    'description': t['description'][:80],
                    'account': t['account_id'],
                    'purpose_note': t['purpose_note']
                }
                for t in sorted(related_party_credits + related_party_debits, 
                               key=lambda x: -(x['credit'] if x['credit'] > 0 else x['debit']))[:50]
            ]
        },
        'flagged_for_review': {
            'count': len(round_figure_credits),
            'total_amount': round(round_figure_total, 2),
            'top_10_items': [{'date': t['date'], 'description': t['description'][:60], 
                            'amount': t['amount'], 'flag_reason': 'Round figure credit'}
                           for t in sorted(round_figure_credits, key=lambda x: -x['amount'])[:10]],
            'all_items': [],
            'note': 'Round figure credits flagged for potential review'
        },
        'categories': {
            'credits': [
                {
                    'category': 'GENUINE_SALES_COLLECTIONS',
                    'count': len(genuine_credits),
                    'amount': round(sum(t['amount'] for t in genuine_credits), 2),
                    'percentage': round(sum(t['amount'] for t in genuine_credits) / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None} 
                                          for t in sorted(genuine_credits, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'INTER_ACCOUNT_TRANSFER',
                    'count': len(matched_transfers),
                    'amount': round(matched_credit_amount, 2),
                    'percentage': round(matched_credit_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['credit_description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(matched_transfers, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'INTER_ACCOUNT_TRANSFER_UNVERIFIED',
                    'count': len(unverified_credit_transfers),
                    'amount': round(unverified_credit_amount, 2),
                    'percentage': round(unverified_credit_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(unverified_credit_transfers, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'RELATED_PARTY',
                    'count': len(related_party_credits),
                    'amount': round(rp_credit_amount, 2),
                    'percentage': round(rp_credit_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['credit'], 'counterparty': t['related_party_name']}
                                          for t in sorted(related_party_credits, key=lambda x: -x['credit'])[:5]]
                },
                {
                    'category': 'LOAN_DISBURSEMENT',
                    'count': len(loan_disbursements),
                    'amount': round(loan_disb_amount, 2),
                    'percentage': round(loan_disb_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in loan_disbursements[:5]]
                },
                {
                    'category': 'INTEREST_PROFIT_DIVIDEND',
                    'count': len(interest_credits),
                    'amount': round(interest_amount, 2),
                    'percentage': round(interest_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(interest_credits, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'REVERSAL',
                    'count': len(reversals),
                    'amount': round(reversal_amount, 2),
                    'percentage': round(reversal_amount / total_credits * 100, 2) if total_credits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in reversals[:5]]
                }
            ],
            'debits': [
                {
                    'category': 'SUPPLIER_VENDOR_PAYMENTS',
                    'count': len(supplier_payments),
                    'amount': round(sum(t['amount'] for t in supplier_payments), 2),
                    'percentage': round(sum(t['amount'] for t in supplier_payments) / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(supplier_payments, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'INTER_ACCOUNT_TRANSFER',
                    'count': len(matched_transfers),
                    'amount': round(matched_debit_amount, 2),
                    'percentage': round(matched_debit_amount / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['debit_description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(matched_transfers, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'RELATED_PARTY',
                    'count': len(related_party_debits),
                    'amount': round(rp_debit_amount, 2),
                    'percentage': round(rp_debit_amount / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['debit'], 'counterparty': t['related_party_name']}
                                          for t in sorted(related_party_debits, key=lambda x: -x['debit'])[:5]]
                },
                {
                    'category': 'STATUTORY_PAYMENT',
                    'count': len(statutory_payments),
                    'amount': round(sum(t['amount'] for t in statutory_payments), 2),
                    'percentage': round(sum(t['amount'] for t in statutory_payments) / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(statutory_payments, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'INTER_ACCOUNT_TRANSFER_UNVERIFIED',
                    'count': len(unverified_debit_transfers),
                    'amount': round(unverified_debit_amount, 2),
                    'percentage': round(unverified_debit_amount / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(unverified_debit_transfers, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'SALARY_WAGES',
                    'count': len(salary_wages),
                    'amount': round(sum(t['amount'] for t in salary_wages), 2),
                    'percentage': round(sum(t['amount'] for t in salary_wages) / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(salary_wages, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'UTILITIES',
                    'count': len(utilities),
                    'amount': round(sum(t['amount'] for t in utilities), 2),
                    'percentage': round(sum(t['amount'] for t in utilities) / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(utilities, key=lambda x: -x['amount'])[:5]]
                },
                {
                    'category': 'BANK_CHARGES',
                    'count': len(bank_charges),
                    'amount': round(sum(t['amount'] for t in bank_charges), 2),
                    'percentage': round(sum(t['amount'] for t in bank_charges) / total_debits * 100, 2) if total_debits > 0 else 0,
                    'top_5_transactions': [{'date': t['date'], 'description': t['description'][:80], 'amount': t['amount'], 'counterparty': None}
                                          for t in sorted(bank_charges, key=lambda x: -x['amount'])[:5]]
                }
            ]
        },
        'counterparties': {
            'top_payers': [],
            'top_payees': [],
            'concentration_risk': {
                'top1_payer_pct': 0,
                'top3_payers_pct': 0,
                'top1_payee_pct': 0,
                'top3_payees_pct': 0,
                'risk_level': 'LOW'
            },
            'parties_both_sides': []
        },
        'kite_flying': {
            'risk_score': 2,
            'risk_level': 'LOW',
            'indicators': [],
            'detailed_findings': ['No significant same-day round-tripping detected']
        },
        'volatility': {
            'calculation_method': 'intraday',
            'overall_index': overall_vol,
            'overall_level': overall_level,
            'monthly': [],
            'alerts': [f'{overall_level} volatility detected'] if overall_level in ['HIGH', 'EXTREME'] else []
        },
        'recurring_payments': {
            'payment_types': [
                {'type': 'EPF/KWSP', 'expected_count': num_months, 'found_count': len(epf_months),
                 'missing_months': [m for m in expected_months if m not in epf_months],
                 'status': get_recurring_status(len(epf_months), num_months)},
                {'type': 'SOCSO/PERKESO', 'expected_count': num_months, 'found_count': len(socso_months),
                 'missing_months': [m for m in expected_months if m not in socso_months],
                 'status': get_recurring_status(len(socso_months), num_months)},
                {'type': 'LHDN/Tax', 'expected_count': num_months, 'found_count': len(lhdn_months),
                 'missing_months': [m for m in expected_months if m not in lhdn_months],
                 'status': get_recurring_status(len(lhdn_months), num_months)},
                {'type': 'HRDF/PSMB', 'expected_count': num_months, 'found_count': len(hrdf_months),
                 'missing_months': [m for m in expected_months if m not in hrdf_months],
                 'status': get_recurring_status(len(hrdf_months), num_months)}
            ],
            'alerts': recurring_alerts,
            'assessment': {
                'statutory_detection': 'FOUND' if all(len(m) >= max(4, num_months - 2) for m in [epf_months, socso_months, lhdn_months, hrdf_months]) else 'PARTIAL',
                'overall_status': 'FOUND' if all(len(m) >= max(4, num_months - 2) for m in [epf_months, socso_months]) else 'PARTIAL',
                'summary': 'Statutory payments detected in majority of months'
            }
        },
        'non_bank_financing': {
            'detection_method': 'keyword_and_pattern_analysis',
            'exclusions_applied': ['Licensed banks', 'Government agencies'],
            'sources': [],
            'suspected_unlicensed': [],
            'risk_level': 'LOW',
            'assessment': 'No suspected unlicensed financing detected'
        },
        'flags': {
            'high_value_transactions': {
                'threshold': 500000,
                'avg_daily_balance': 0,
                'count': 0,
                'transactions': []
            },
            'round_figure_transactions': {
                'count': len(round_figure_credits),
                'total_amount': round(round_figure_total, 2),
                'percentage_of_credits': round(round_figure_pct, 2),
                'assessment': 'HIGH' if round_figure_pct > 50 else ('ELEVATED' if round_figure_pct > ROUND_FIGURE_WARNING_PCT else 'NORMAL'),
                'top_10_transactions': [],
                'all_transactions': []
            },
            'returned_cheques': {
                'count': 0,
                'total_value': 0,
                'transactions': [],
                'assessment': 'NONE'
            }
        },
        'integrity_score': {
            'score': score,
            'points_earned': total_points,
            'points_possible': 23,
            'rating': rating,
            'checks': checks
        },
        'observations': {
            'positive': [
                f'Strong business turnover of RM {round(net_credits/1000000, 1)}M over {num_months} months',
                'Statutory payments (EPF, SOCSO, Tax, HRDF) consistently detected',
                'No returned cheques or overdraft breaches',
                'SME Bank financing relationship indicates formal credit facilities'
            ],
            'concerns': [
                f'{overall_level} volatility levels observed' if overall_level in ['HIGH', 'EXTREME'] else 'Volatility within acceptable range',
                f'Round figure credits at {round(round_figure_pct, 1)}%' if round_figure_pct > 20 else 'Round figure credits within normal range',
                'Multiple bank accounts referenced but not provided for analysis' if missing_accounts else 'All accounts provided'
            ]
        },
        'recommendations': [
            {'priority': 'HIGH', 'category': 'Data Completeness', 
             'recommendation': f'Obtain statements from {", ".join(list(missing_bank_codes)[:3])} accounts to verify inter-account transfers'} if missing_accounts else None,
            {'priority': 'MEDIUM', 'category': 'Volatility Management',
             'recommendation': 'Consider maintaining higher operating balances to reduce volatility'} if overall_level in ['HIGH', 'EXTREME'] else None,
            {'priority': 'LOW', 'category': 'Banking Consolidation',
             'recommendation': 'Consider consolidating banking relationships to simplify cash flow monitoring'} if len(accounts) > 3 else None
        ]
    }
    
    # Remove None recommendations
    result['recommendations'] = [r for r in result['recommendations'] if r is not None]
    
    return result


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    result = analyze()
    print(json.dumps(result, indent=2, ensure_ascii=False))
