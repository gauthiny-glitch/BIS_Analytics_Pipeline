"""
generate_data.py
================
Generates synthetic source data for the BIS Analytics Pipeline project.

Simulates flat file extracts from Openlink Findur (BIS deal booking system):
  - src_deals.csv    : currency deposit deal book (~500 rows)
  - src_fx_rates.csv : monthly SDR exchange rates (Apr 2023 – Mar 2025)

Output files are written to the ../data/ directory.

Usage:
    python scripts/generate_data.py

Requirements:
    pip install pandas numpy
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# ── Config ────────────────────────────────────────────────────────────────────

RANDOM_SEED   = 42
N_DEALS       = 500
DATA_DIR      = os.path.join(os.path.dirname(__file__), "..", "data")
START_DATE    = date(2023, 4, 1)   # BIS FY start
END_DATE      = date(2025, 3, 31)  # BIS FY end

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

os.makedirs(DATA_DIR, exist_ok=True)

# ── Counterparties ─────────────────────────────────────────────────────────────
# Simulated central bank counterparties, grouped by BIS geographic region.
# Distribution approximates Annual Report p.106, Graph 4.3.

COUNTERPARTIES = [
    # Asia-Pacific (~61%)
    ("PBOC_01",  "People's Bank of China",                    "CN"),
    ("BOJ_01",   "Bank of Japan",                             "JP"),
    ("RBI_01",   "Reserve Bank of India",                     "IN"),
    ("BOK_01",   "Bank of Korea",                             "KR"),
    ("RBA_01",   "Reserve Bank of Australia",                 "AU"),
    ("MAS_01",   "Monetary Authority of Singapore",           "SG"),
    ("HKMA_01",  "Hong Kong Monetary Authority",              "HK"),
    ("BOT_01",   "Bank of Thailand",                          "TH"),
    ("BNM_01",   "Bank Negara Malaysia",                      "MY"),
    ("BI_01",    "Bank Indonesia",                            "ID"),
    ("BSP_01",   "Bangko Sentral ng Pilipinas",               "PH"),
    ("BB_01",    "Bangladesh Bank",                           "BD"),
    ("SBP_01",   "State Bank of Pakistan",                    "PK"),
    ("CBSL_01",  "Central Bank of Sri Lanka",                 "LK"),
    ("CBM_01",   "Central Bank of Myanmar",                   "MM"),
    ("NBC_01",   "National Bank of Cambodia",                 "KH"),
    ("BOM_01",   "Bank of Mongolia",                          "MN"),
    ("RBNZ_01",  "Reserve Bank of New Zealand",               "NZ"),
    ("SBV_01",   "State Bank of Vietnam",                     "VN"),
    ("NBT_01",   "National Bank of Tajikistan",               "TJ"),
    ("NBK_01",   "National Bank of Kazakhstan",               "KZ"),
    ("NBU_01",   "National Bank of Uzbekistan",               "UZ"),
    ("BCBN_01",  "Bangko Central ng Brunei",                  "BN"),
    # Americas (~9%)
    ("FED_01",   "Federal Reserve Bank of New York",          "US"),
    ("BCB_01",   "Banco Central do Brasil",                   "BR"),
    ("BOM_MX",   "Banco de Mexico",                           "MX"),
    ("BCRA_01",  "Banco Central de la Republica Argentina",   "AR"),
    ("BCC_01",   "Banco Central de Chile",                    "CL"),
    ("BRC_01",   "Banco de la Republica Colombia",            "CO"),
    ("BCRP_01",  "Banco Central de Reserva del Peru",         "PE"),
    # Europe (~8%)
    ("BBK_01",   "Deutsche Bundesbank",                       "DE"),
    ("BDF_01",   "Banque de France",                          "FR"),
    ("BOE_01",   "Bank of England",                           "GB"),
    ("BANCIT_01","Banca d'Italia",                            "IT"),
    ("BDE_01",   "Banco de Espana",                           "ES"),
    ("SNB_01",   "Swiss National Bank",                       "CH"),
    ("NBP_01",   "Narodowy Bank Polski",                      "PL"),
    ("CNB_01",   "Czech National Bank",                       "CZ"),
    ("CBAZ_01",  "Central Bank of Azerbaijan",                "AZ"),
    ("NBA_01",   "National Bank of Armenia",                  "AM"),
    ("NBG_01",   "National Bank of Georgia",                  "GE"),
    # Middle East (~7%)
    ("SAMA_01",  "Saudi Central Bank (SAMA)",                 "SA"),
    ("CBUAE_01", "Central Bank of the UAE",                   "AE"),
    ("CBK_01",   "Central Bank of Kuwait",                    "KW"),
    ("QCB_01",   "Qatar Central Bank",                        "QA"),
    ("CBB_01",   "Central Bank of Bahrain",                   "BH"),
    ("CBO_01",   "Central Bank of Oman",                      "OM"),
    ("CBJ_01",   "Central Bank of Jordan",                    "JO"),
    # Africa (~6%)
    ("SARB_01",  "South African Reserve Bank",                "ZA"),
    ("CBN_01",   "Central Bank of Nigeria",                   "NG"),
    ("CBE_01",   "Central Bank of Egypt",                     "EG"),
    ("CBK_KE",   "Central Bank of Kenya",                     "KE"),
    ("BOG_01",   "Bank of Ghana",                             "GH"),
    ("BOT_TZ",   "Bank of Tanzania",                          "TZ"),
    ("CBM_MW",   "Reserve Bank of Malawi",                    "MW"),
    # International (~9%)
    ("IMF_01",   "International Monetary Fund",               "XX"),
    ("BIS_01",   "Bank for International Settlements",        "XX"),
    ("ECB_01",   "European Central Bank",                     "EU"),
]

# Weighted sampling: Asia-Pacific ~61%, Int'l 9%, Americas 9%, Europe 8%, ME 7%, Africa 6%
REGION_WEIGHTS = {
    "CN":3.5,"JP":3.0,"IN":2.5,"KR":2.0,"AU":2.0,"SG":2.0,"HK":2.0,
    "TH":1.5,"MY":1.5,"ID":1.5,"PH":1.2,"BD":1.0,"PK":1.0,"LK":0.8,
    "MM":0.8,"KH":0.8,"MN":0.6,"NZ":0.8,"VN":0.8,"TJ":0.5,"KZ":0.8,
    "UZ":0.6,"BN":0.4,                           # Asia-Pacific
    "US":1.5,"BR":1.5,"MX":1.2,"AR":0.8,"CL":0.8,"CO":0.8,"PE":0.7,  # Americas
    "DE":1.2,"FR":1.2,"GB":1.2,"IT":0.8,"ES":0.8,"CH":0.8,"PL":0.6,
    "CZ":0.5,"AZ":0.4,"AM":0.3,"GE":0.3,         # Europe
    "SA":1.0,"AE":1.0,"KW":0.8,"QA":0.8,"BH":0.5,"OM":0.5,"JO":0.4,  # ME
    "ZA":0.8,"NG":0.7,"EG":0.7,"KE":0.5,"GH":0.4,"TZ":0.4,"MW":0.3,  # Africa
    "XX":2.0,"XX":2.0,"EU":2.0,                   # International
}
cpty_weights = [REGION_WEIGHTS.get(c[2], 0.5) for c in COUNTERPARTIES]

# ── Products ──────────────────────────────────────────────────────────────────
# Weights approximate Annual Report p.105, Graph 4.2
PRODUCTS = [
    ("FIXBIS_DEP", 0.50),   # most common
    ("MTI_DEP",    0.20),
    ("SIGHT_DEP",  0.20),
    ("NOTICE_DEP", 0.10),
]

# ── Currencies ────────────────────────────────────────────────────────────────
CURRENCIES = ["USD","EUR","GBP","JPY","CNY","CHF","AUD","CAD","SGD","KRW"]
CCY_WEIGHTS = [0.35,0.25,0.10,0.08,0.07,0.05,0.04,0.03,0.02,0.01]

# ── Portfolios ────────────────────────────────────────────────────────────────
PORTFOLIOS = ["TRSRY-APAC","TRSRY-EMEA","TRSRY-AMER","TRSRY-INTL"]

# ── Helper functions ──────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def add_business_days(d: date, n: int) -> date:
    """Add n calendar days (simplified — not adjusting for holidays)."""
    return d + timedelta(days=n)

def maturity_offset(deal_type: str) -> int | None:
    """Returns days to maturity from value date (None for sight accounts)."""
    if deal_type == "SIGHT_DEP":
        return None
    elif deal_type == "NOTICE_DEP":
        return random.choice([30, 60, 90, 180])
    elif deal_type == "MTI_DEP":
        years = random.randint(1, 7)
        return years * 365
    else:  # FIXBIS_DEP
        options = [30, 60, 90, 120, 180, 270, 365, 540, 730]
        weights = [0.10, 0.15, 0.20, 0.10, 0.15, 0.10, 0.10, 0.05, 0.05]
        return random.choices(options, weights=weights)[0]

def nominal_amount(deal_type: str, currency: str) -> float:
    """Generates a realistic deposit amount in full currency units."""
    if currency == "JPY" or currency == "KRW":
        base = random.choice([10_000_000_000, 25_000_000_000, 50_000_000_000,
                               100_000_000_000, 250_000_000_000])
    elif deal_type == "MTI_DEP":
        base = random.choice([500_000_000, 1_000_000_000, 2_000_000_000,
                               5_000_000_000, 10_000_000_000])
    else:
        base = random.choice([100_000_000, 250_000_000, 500_000_000,
                               750_000_000, 1_000_000_000, 2_000_000_000])
    # Add small random variation
    return round(base * random.uniform(0.9, 1.1), 2)

def interest_rate(deal_type: str, currency: str) -> float:
    """Returns a realistic interest rate (%) given product and currency."""
    # Base rates by currency (approximate 2023-2025 environment)
    base = {"USD": 5.00, "EUR": 3.50, "GBP": 5.00, "JPY": 0.10,
            "CNY": 2.50, "CHF": 1.50, "AUD": 4.25, "CAD": 4.75,
            "SGD": 3.50, "KRW": 3.25}.get(currency, 3.00)
    spread = {"FIXBIS_DEP": 0.0, "MTI_DEP": 0.10, "SIGHT_DEP": -0.50,
              "NOTICE_DEP": -0.20}.get(deal_type, 0.0)
    noise = random.uniform(-0.25, 0.25)
    rate = max(0.0, base + spread + noise)
    return round(rate, 4)

# ── Generate src_deals ────────────────────────────────────────────────────────

print("Generating src_deals.csv ...")

rows = []
for i in range(1, N_DEALS + 1):
    cpty     = random.choices(COUNTERPARTIES, weights=cpty_weights)[0]
    product  = random.choices([p[0] for p in PRODUCTS],
                               weights=[p[1] for p in PRODUCTS])[0]
    currency = random.choices(CURRENCIES, weights=CCY_WEIGHTS)[0]
    portfolio= random.choice(PORTFOLIOS)
    book     = f"BK-{random.randint(1, 10):03d}"

    trade_dt  = random_date(START_DATE, date(2025, 2, 28))
    value_dt  = add_business_days(trade_dt, 2)
    offset    = maturity_offset(product)
    mat_dt    = add_business_days(value_dt, offset) if offset else None

    # Deal status
    today = date(2025, 3, 31)
    if mat_dt and mat_dt < today:
        status = random.choices(["MATURED", "CANCELLED"], weights=[0.95, 0.05])[0]
    elif random.random() < 0.02:
        status = "CANCELLED"
    else:
        status = "VALIDATED"

    created    = f"{trade_dt} {random.randint(7,17):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    modified   = created

    month_str  = trade_dt.strftime("%Y%m")
    deal_id    = f"FIN-{month_str}-{i:06d}"

    rows.append({
        "deal_id":          deal_id,
        "deal_type":        product,
        "cpty_code":        cpty[0],
        "cpty_name":        cpty[1],
        "cpty_country":     cpty[2],
        "trade_date":       trade_dt.strftime("%Y-%m-%d"),
        "value_date":       value_dt.strftime("%Y-%m-%d"),
        "maturity_date":    mat_dt.strftime("%Y-%m-%d") if mat_dt else "",
        "currency":         currency,
        "nominal_amount":   nominal_amount(product, currency),
        "rate_pct":         interest_rate(product, currency),
        "deal_status":      status,
        "portfolio":        portfolio,
        "book":             book,
        "created_dt":       created,
        "last_modified_dt": modified,
    })

deals_df = pd.DataFrame(rows)
out_deals = os.path.join(DATA_DIR, "src_deals.csv")
deals_df.to_csv(out_deals, index=False)
print(f"  -> {out_deals}  ({len(deals_df)} rows)")

# ── Generate src_fx_rates ─────────────────────────────────────────────────────

print("Generating src_fx_rates.csv ...")

# Approximate SDR/unit rates for Apr 2023 – Mar 2025 (monthly, end-of-month)
# SDR rate = how many SDR per 1 unit of foreign currency
BASE_RATES = {
    "USD": 0.752, "EUR": 0.820, "GBP": 0.947, "JPY": 0.00542,
    "CNY": 0.104, "CHF": 0.845, "AUD": 0.497, "CAD": 0.558,
    "SGD": 0.557, "KRW": 0.000572,
}

rate_rows = []
# Generate monthly dates: last day of each month Apr 2023 → Mar 2025
d = date(2023, 4, 1)
while d <= date(2025, 3, 31):
    eom = (d.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
    months_elapsed = (eom.year - 2023) * 12 + eom.month - 4
    for ccy, base in BASE_RATES.items():
        # Slight random walk over time (±5% drift)
        drift = 1.0 + (months_elapsed / 24.0) * random.uniform(-0.05, 0.05)
        noise = random.uniform(0.98, 1.02)
        rate  = round(base * drift * noise, 6)
        rate_rows.append({
            "rate_date":     eom.strftime("%Y-%m-%d"),
            "currency_code": ccy,
            "sdr_per_unit":  rate,
        })
    d += relativedelta(months=1)

rates_df = pd.DataFrame(rate_rows)
out_rates = os.path.join(DATA_DIR, "src_fx_rates.csv")
rates_df.to_csv(out_rates, index=False)
print(f"  -> {out_rates}  ({len(rates_df)} rows)")

print("\nDone. Load both CSVs into MS Fabric OneLake using the PySpark notebook.")
print("Then run: dbt run  (from the bis_analytics/ directory)")
