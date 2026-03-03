#!/usr/bin/env python3
"""
Utility Affordability Index Calculator (GTM Edition)

Goal:
Rank utilities by "Affordability Pressure"

Affordability Pressure = Electricity Rate × Poverty Rate

Electricity Rate = Residential Revenue ÷ Residential Sales ($/kWh)
Poverty Rate = % below poverty line in utility service territory

Important note about Schedule 9 (Service Territory):
Your Service_Territory_2024.xlsx contains utility, state, and county names,
but it does not contain county FIPS or customer share weights.

Because weights are missing, this script computes a county poverty rate per utility
using an equal-weight average across counties listed in the territory file.

If county matching fails for a utility, it falls back to state-level poverty.

Inputs:
- EIA 861 Sales to Ultimate Customers file (Excel)
- EIA 861 Service Territory file (Excel, Schedule 9)
- ACS S1701 poverty (Census API)

Outputs:
- outputs/utility_affordability_index_top10.csv
"""

import os
import sys
import argparse
import warnings
import re
from typing import Optional, Dict

import pandas as pd
import requests # type: ignore

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Optional visualization libraries
try:
    import matplotlib.pyplot as plt  # noqa: F401
    import seaborn as sns  # noqa: F401
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False


DEFAULT_SALES_FILE = "Sales_Ult_Cust_2024.xlsx"
DEFAULT_TERRITORY_FILE = "Service_Territory_2024.xlsx"
DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_TOP_N = 10

RATE_MIN_THRESHOLD = 0.01
RATE_MAX_THRESHOLD = 1.00

ACS_YEARS = [2023, 2022, 2021]


STATE_ABBREV_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia",
    "PR": "Puerto Rico",
}

STATE_NAME_TO_ABBREV = {v: k for k, v in STATE_ABBREV_TO_NAME.items()}


def parse_args():
    parser = argparse.ArgumentParser(description="Calculate Utility Affordability Index")
    parser.add_argument("--sales", default=DEFAULT_SALES_FILE)
    parser.add_argument("--territory", default=DEFAULT_TERRITORY_FILE)
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--top", "-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--no-county", action="store_true", help="Skip county matching and use state poverty only")
    parser.add_argument("--keep-outliers", action="store_true")
    parser.add_argument("--no-viz", action="store_true")
    parser.add_argument("--min-county-coverage", type=float, default=0.70,
                        help="Minimum county match coverage ratio required to accept county poverty for a utility")
    return parser.parse_args()


def log_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def get_census_url(year: int, geography: str) -> str:
    if geography == "county":
        return (
            f"https://api.census.gov/data/{year}/acs/acs5/subject"
            f"?get=NAME,S1701_C03_001E&for=county:*&in=state:*"
        )
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S1701_C03_001E&for=state:*"


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            cleaned = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != "nan"]
            new_cols.append("_".join(cleaned))
        else:
            new_cols.append(str(col).strip())
    df.columns = new_cols
    return df


def drop_units_row(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df
    first_row_str = " ".join([str(val) for val in df.iloc[0].values]).upper()
    if any(kw in first_row_str for kw in ["THOUSAND", "MEGAWATT", "MEGAWATTHOUR", "MILLION", "DOLLAR"]):
        return df.iloc[1:].reset_index(drop=True)
    return df


def norm_utility_number(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(".0", "", regex=False)


def county_key(s: str) -> str:
    """
    Produce a canonical county key to make name-based joins less fragile.

    Strategy:
    - lowercase
    - strip common suffixes (county, parish, borough, census area, municipio)
    - remove punctuation
    - collapse whitespace
    """
    if s is None:
        return ""
    x = str(s).strip().lower()

    # Normalize common abbreviations
    x = x.replace("&", " and ")
    x = x.replace("st.", "saint ")
    x = x.replace("ste.", "sainte ")

    # Remove common geographic suffixes
    suffixes = [
        " county", " parish", " borough", " census area", " municipality",
        " city and borough", " municipio", " island", " islands",
    ]
    for suf in suffixes:
        if x.endswith(suf):
            x = x[: -len(suf)].strip()

    # Remove punctuation and non alphanum except spaces
    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def load_eia_sales_data(filepath: str) -> pd.DataFrame:
    log_section("Loading EIA-861 Sales Data")

    if not os.path.exists(filepath):
        print(f"✗ ERROR: File '{filepath}' not found")
        sys.exit(1)

    xls = pd.ExcelFile(filepath)
    print(f"Available sheets: {xls.sheet_names}")

    sheet_name = None
    for name in ["States", "State", "Data"]:
        if name in xls.sheet_names:
            sheet_name = name
            break

    if not sheet_name:
        print("✗ ERROR: Could not find sales data sheet (expected States, State, or Data)")
        sys.exit(1)

    df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
    print(f"✓ Loaded {len(df):,} rows from '{sheet_name}'")

    df = drop_units_row(df)
    df = flatten_columns(df)

    util_char_cols = sorted([col for col in df.columns if "Utility Characteristics" in str(col)])

    col_map: Dict[str, str] = {}

    # Identify residential revenue and sales
    for col in df.columns:
        col_upper = str(col).upper()
        if "RESIDENTIAL" in col_upper and "REVENUE" in col_upper:
            col_map["revenue"] = col
        elif "RESIDENTIAL" in col_upper and "SALES" in col_upper:
            col_map["sales"] = col

    # Detect utility characteristics columns by sampling
    if util_char_cols:
        for col in util_char_cols[:12]:
            first_val = str(df[col].iloc[0]) if len(df) > 0 else ""
            if first_val.isdigit() and len(first_val) == 4:
                col_map.setdefault("data_year", col)
            elif first_val.isdigit() and 4 <= len(first_val) <= 7:
                col_map.setdefault("utility_number", col)
            elif any(c.isalpha() for c in first_val) and len(first_val) > 5:
                col_map.setdefault("utility_name", col)
            elif first_val.isalpha() and len(first_val) == 2:
                col_map.setdefault("state", col)

    # Fallback positional mapping
    if "data_year" not in col_map and len(util_char_cols) > 0:
        col_map["data_year"] = util_char_cols[0]
    if "utility_number" not in col_map and len(util_char_cols) > 1:
        col_map["utility_number"] = util_char_cols[1]
    if "utility_name" not in col_map and len(util_char_cols) > 2:
        col_map["utility_name"] = util_char_cols[2]
    if "state" not in col_map and len(util_char_cols) > 3:
        col_map["state"] = util_char_cols[3]

    required = ["data_year", "utility_number", "utility_name", "state", "revenue", "sales"]
    missing = [k for k in required if k not in col_map]
    if missing:
        print(f"✗ ERROR: Could not auto-detect columns: {', '.join(missing)}")
        print("Here are the first 40 columns:")
        for i, col in enumerate(df.columns[:40], 1):
            print(f"{i:2d}. {col}")
        sys.exit(1)

    df = df.rename(columns={
        col_map["data_year"]: "data_year",
        col_map["utility_number"]: "utility_number",
        col_map["utility_name"]: "utility_name",
        col_map["state"]: "state",
        col_map["revenue"]: "residential_revenue_thousand_usd",
        col_map["sales"]: "residential_sales_mwh",
    })

    df = df[
        [
            "data_year",
            "utility_number",
            "utility_name",
            "state",
            "residential_revenue_thousand_usd",
            "residential_sales_mwh",
        ]
    ].copy()

    df["utility_number"] = norm_utility_number(df["utility_number"])
    df["state"] = df["state"].astype(str).str.strip().str.upper()
    df["state_full_name"] = df["state"].map(STATE_ABBREV_TO_NAME)

    print(f"✓ Processed {len(df):,} rows")
    return df


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    log_section("Cleaning Sales Data")

    df["utility_name"] = df["utility_name"].astype(str)
    df = df[~df["utility_name"].str.contains("ADJUSTMENT", case=False, na=False)].copy()

    initial = len(df)
    df["data_year"] = pd.to_numeric(df["data_year"], errors="coerce")
    df = df[df["data_year"] == 2024].copy()
    print(f"Year 2024: {len(df):,}/{initial:,} rows")

    df["residential_revenue_thousand_usd"] = pd.to_numeric(df["residential_revenue_thousand_usd"], errors="coerce")
    df["residential_sales_mwh"] = pd.to_numeric(df["residential_sales_mwh"], errors="coerce")

    df = df[
        df["residential_revenue_thousand_usd"].notna() & (df["residential_revenue_thousand_usd"] > 0) &
        df["residential_sales_mwh"].notna() & (df["residential_sales_mwh"] >= 1000)
    ].copy()

    print(f"After filters: {len(df):,} rows")
    return df


def compute_rates(df: pd.DataFrame, keep_outliers: bool = False) -> pd.DataFrame:
    log_section("Computing Electricity Rates")

    df["electricity_rate_usd_per_kwh"] = (
        (df["residential_revenue_thousand_usd"] * 1000.0) / (df["residential_sales_mwh"] * 1000.0)
    )

    rate_min = df["electricity_rate_usd_per_kwh"].min()
    rate_max = df["electricity_rate_usd_per_kwh"].max()
    print(f"Rate stats: ${rate_min:.4f} to ${rate_max:.4f} per kWh")

    outliers = (
        (df["electricity_rate_usd_per_kwh"] < RATE_MIN_THRESHOLD)
        | (df["electricity_rate_usd_per_kwh"] > RATE_MAX_THRESHOLD)
    )

    if outliers.sum() > 0:
        print(f"Outliers flagged: {int(outliers.sum())}")
        if not keep_outliers:
            df = df[~outliers].copy()
            print(f"After outlier removal: {len(df):,} rows")

    return df


def load_service_territory_state_county(filepath: str) -> Optional[pd.DataFrame]:
    """
    Load Schedule 9 service territory as utility_number, state, county_name.

    Your workbook contains counties by name, not FIPS and not customer share weights.
    """
    log_section("Loading Service Territory (State + County)")

    if not os.path.exists(filepath):
        print(f"⚠ File '{filepath}' not found, skipping county method")
        return None

    try:
        xls = pd.ExcelFile(filepath)
        print(f"Available sheets: {xls.sheet_names}")

        sheet_name = None
        if "Counties_States" in xls.sheet_names:
            sheet_name = "Counties_States"
        elif "Counties_Territories" in xls.sheet_names:
            sheet_name = "Counties_Territories"

        if not sheet_name:
            print("⚠ Could not find a territory sheet (expected Counties_States or Counties_Territories)")
            return None

        # Try multi-header first, then fallback
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
        except Exception:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=0)

        df = drop_units_row(df)
        df = flatten_columns(df)
        print(f"✓ Loaded {len(df):,} rows from '{sheet_name}'")

        util_col = None
        state_col = None
        county_col = None

        for col in df.columns:
            c = str(col).upper()
            if util_col is None and "UTILITY" in c and "NUMBER" in c:
                util_col = col
            if state_col is None and c.startswith("STATE"):
                state_col = col
            if county_col is None and c.startswith("COUNTY"):
                county_col = col

        if not (util_col and state_col and county_col):
            print("⚠ Missing required columns in territory sheet")
            print(f"Utility column found: {'yes' if util_col else 'no'}")
            print(f"State column found: {'yes' if state_col else 'no'}")
            print(f"County column found: {'yes' if county_col else 'no'}")
            print("First 30 columns:")
            for c in list(df.columns[:30]):
                print(f" - {c}")
            return None

        df = df.rename(columns={
            util_col: "utility_number",
            state_col: "state",
            county_col: "county",
        })

        df["utility_number"] = norm_utility_number(df["utility_number"])
        df["state"] = df["state"].astype(str).str.strip().str.upper()
        df["county"] = df["county"].astype(str).str.strip()

        # Build join keys
        df["county_key"] = df["county"].apply(county_key)

        # Drop blanks
        df = df[df["utility_number"].str.len() > 0].copy()
        df = df[df["state"].str.len() == 2].copy()
        df = df[df["county_key"].str.len() > 0].copy()

        # Deduplicate
        df = df.drop_duplicates(subset=["utility_number", "state", "county_key"]).copy()

        print(f"✓ Territory rows after cleaning: {len(df):,}")
        print(f"✓ Utilities covered: {df['utility_number'].nunique():,}")

        return df[["utility_number", "state", "county", "county_key"]].copy()

    except Exception as e:
        print(f"⚠ Error reading territory file: {e}")
        return None


def fetch_county_poverty() -> Optional[pd.DataFrame]:
    """
    Fetch county poverty data (ACS S1701_C03_001E) and create state + county keys.
    """
    log_section("Fetching County Poverty Data (ACS S1701)")

    for year in ACS_YEARS:
        try:
            url = get_census_url(year, "county")
            print(f"Trying Census ACS {year}...")
            response = requests.get(url, timeout=20)

            if response.status_code != 200:
                print(f"HTTP {response.status_code}")
                continue

            data = response.json()
            df = pd.DataFrame(data[1:], columns=data[0])

            df["state_fips"] = df["state"].astype(str).str.zfill(2)
            df["county_fips"] = df["county"].astype(str).str.zfill(3)
            df["fips_code"] = df["state_fips"] + df["county_fips"]

            df["poverty_rate_percent"] = pd.to_numeric(df["S1701_C03_001E"], errors="coerce")
            df = df[
                df["poverty_rate_percent"].notna()
                & (df["poverty_rate_percent"] >= 0)
                & (df["poverty_rate_percent"] <= 100)
            ].copy()

            # Parse NAME like "Abbeville County, South Carolina"
            # Sometimes contains extra commas, so take first chunk as county-like, last chunk as state-like.
            parts = df["NAME"].astype(str).str.split(",")
            df["county_name_raw"] = parts.str[0].str.strip()
            df["state_name"] = parts.str[-1].str.strip()

            df["state"] = df["state_name"].map(STATE_NAME_TO_ABBREV)
            df = df[df["state"].notna()].copy()

            df["county_name"] = (
                df["county_name_raw"]
                .str.replace(r"\s+County$", "", regex=True)
                .str.replace(r"\s+Parish$", "", regex=True)
                .str.replace(r"\s+Borough$", "", regex=True)
                .str.replace(r"\s+Census Area$", "", regex=True)
                .str.replace(r"\s+Municipality$", "", regex=True)
                .str.replace(r"\s+City and Borough$", "", regex=True)
                .str.replace(r"\s+Municipio$", "", regex=True)
                .str.strip()
            )

            df["county_key"] = df["county_name"].apply(county_key)

            print(f"✓ Loaded {len(df):,} counties from {year}")
            return df[["fips_code", "state", "county_name", "county_key", "poverty_rate_percent"]].copy()

        except Exception as e:
            print(f"Error: {e}")

    print("✗ Failed to fetch county poverty data from all years")
    return None


def fetch_state_poverty() -> pd.DataFrame:
    log_section("Fetching State Poverty Data (ACS S1701)")

    for year in ACS_YEARS:
        try:
            url = get_census_url(year, "state")
            print(f"Trying Census API {year}...")
            response = requests.get(url, timeout=15)

            if response.status_code != 200:
                print(f"HTTP {response.status_code}")
                continue

            data = response.json()
            df = pd.DataFrame(data[1:], columns=data[0])
            df["poverty_rate_percent"] = pd.to_numeric(df["S1701_C03_001E"], errors="coerce")
            df = df[
                df["poverty_rate_percent"].notna()
                & (df["poverty_rate_percent"] >= 0)
                & (df["poverty_rate_percent"] <= 100)
            ].copy()

            print(f"✓ Loaded {len(df):,} states from {year}")
            return df[["NAME", "poverty_rate_percent"]].copy()

        except Exception as e:
            print(f"Error: {e}")

    print("✗ ERROR: Could not load state poverty data")
    sys.exit(1)


def compute_equal_weighted_poverty(
    territory_df: pd.DataFrame,
    county_poverty_df: pd.DataFrame,
    min_coverage: float
) -> pd.DataFrame:
    merged = territory_df.merge(
        county_poverty_df,
        on=["state", "county_key"],
        how="left"
    )

    total_cty = merged.groupby("utility_number")["county_key"].nunique().rename("counties_total").reset_index()
    matched_cty = (
        merged[merged["poverty_rate_percent"].notna()]
        .groupby("utility_number")["county_key"]
        .nunique()
        .rename("counties_matched")
        .reset_index()
    )

    stats = total_cty.merge(matched_cty, on="utility_number", how="left")
    stats["counties_matched"] = stats["counties_matched"].fillna(0).astype(int)
    stats["coverage_ratio"] = stats["counties_matched"] / stats["counties_total"]

    utility_poverty = (
        merged[merged["poverty_rate_percent"].notna()]
        .groupby("utility_number", as_index=False)
        .agg(poverty_rate_percent=("poverty_rate_percent", "mean"))
    ).merge(stats, on="utility_number", how="right")

    # enforce coverage threshold
    utility_poverty.loc[utility_poverty["coverage_ratio"] < min_coverage, "poverty_rate_percent"] = pd.NA

    return utility_poverty


def join_state_poverty(eia_df: pd.DataFrame, state_poverty_df: pd.DataFrame) -> pd.DataFrame:
    log_section("Joining State Poverty")

    tmp = state_poverty_df.rename(columns={"NAME": "state_full_name"})
    out = eia_df.merge(tmp, on="state_full_name", how="left")
    return out


def compute_affordability_pressure(df: pd.DataFrame) -> pd.DataFrame:
    log_section("Computing Affordability Pressure")

    df["poverty_rate_decimal"] = df["poverty_rate_percent"] / 100.0
    df["affordability_pressure"] = df["electricity_rate_usd_per_kwh"] * df["poverty_rate_decimal"]
    df = df[df["affordability_pressure"].notna()].copy()

    print(f"✓ Computed affordability pressure for {len(df):,} utilities")
    return df


def main():
    args = parse_args()

    print("\n" + "=" * 70)
    print("UTILITY AFFORDABILITY INDEX CALCULATOR (GTM EDITION)")
    print("=" * 70)

    os.makedirs(args.output_dir, exist_ok=True)

    # Load and compute utility electricity rates
    eia_df = load_eia_sales_data(args.sales)
    eia_df = clean_sales_data(eia_df)
    eia_df = compute_rates(eia_df, keep_outliers=args.keep_outliers)

    # Poverty integration
    method_used = "state"
    eia_df["poverty_source"] = "state"

    if not args.no_county:
        territory_df = load_service_territory_state_county(args.territory)
        county_poverty_df = fetch_county_poverty()

        if territory_df is not None and county_poverty_df is not None:
            util_poverty_df = compute_equal_weighted_poverty(
                territory_df=territory_df,
                county_poverty_df=county_poverty_df,
                min_coverage=args.min_county_coverage
            )
            util_poverty_df = util_poverty_df.groupby("utility_number", as_index=False).agg({
    "poverty_rate_percent": "mean",
    "counties_total": "first",
    "counties_matched": "first",
    "coverage_ratio": "first",
})
            eia_df = eia_df.drop_duplicates(subset=["utility_number"])



            eia_df = eia_df.merge(util_poverty_df, on="utility_number", how="left")

            # Mark county where we have county poverty
            eia_df.loc[eia_df["poverty_rate_percent"].notna(), "poverty_source"] = "county_equal_weight"

            # Fill missing poverty from state
            state_poverty_df = fetch_state_poverty()
            state_joined = join_state_poverty(eia_df[["utility_number", "state_full_name"]].drop_duplicates(), state_poverty_df)
            state_joined = state_joined.rename(columns={"poverty_rate_percent": "poverty_rate_percent_state"})
            eia_df = eia_df.merge(state_joined[["utility_number", "poverty_rate_percent_state"]], on="utility_number", how="left")

            eia_df["poverty_rate_percent"] = eia_df["poverty_rate_percent"].fillna(eia_df["poverty_rate_percent_state"])
            eia_df = eia_df.drop(columns=["poverty_rate_percent_state"])

            method_used = "county where coverage ok, else state fallback"
        else:
            state_poverty_df = fetch_state_poverty()
            eia_df = join_state_poverty(eia_df, state_poverty_df)
            eia_df["poverty_source"] = "state"
            method_used = "state"
    else:
        state_poverty_df = fetch_state_poverty()
        eia_df = join_state_poverty(eia_df, state_poverty_df)
        eia_df["poverty_source"] = "state"
        method_used = "state"

    # Compute final metric
    eia_df = compute_affordability_pressure(eia_df)

    # 🔒 Enforce one row per utility before ranking
    eia_df = eia_df.drop_duplicates(subset=["utility_number"])

    # Rank
    eia_df = eia_df.sort_values("affordability_pressure", ascending=False).copy()
    top_df = eia_df.head(args.top).copy()
    top_df["rank"] = range(1, len(top_df) + 1)

    # Save
    log_section("Saving Output")

    top_df["residential_revenue_usd"] = top_df["residential_revenue_thousand_usd"] * 1000.0
    top_df["utility_id"] = top_df["utility_number"].astype(str)

    # Helpful ordering
    cols = [
        "rank",
        "utility_number",
        "utility_name",
        "state",
        "electricity_rate_usd_per_kwh",
        "poverty_rate_percent",
        "affordability_pressure",
        "poverty_source",
    ]

    # Add territory coverage columns if present
    for extra in ["counties_total", "counties_matched", "coverage_ratio"]:
        if extra in top_df.columns:
            cols.append(extra)

    # Add raw inputs
    for extra in ["residential_revenue_thousand_usd", "residential_sales_mwh", "residential_revenue_usd"]:
        if extra in top_df.columns and extra not in cols:
            cols.append(extra)

    # Keep any remaining columns after the core set
    remaining = [c for c in top_df.columns if c not in cols]
    top_df = top_df[cols + remaining].copy()

    output_path = os.path.join(args.output_dir, "utility_affordability_index_top10.csv")
    top_df.to_csv(output_path, index=False)
    print(f"✓ Saved: {output_path}")

    # Summary
    log_section("COMPLETE")
    print(f"✓ Top {len(top_df)} utilities identified")
    print(f"✓ Poverty methodology: {method_used}")

    if len(top_df) > 0:
        first = top_df.iloc[0]
        print(f"\n#1 Utility: {first['utility_name']} ({first['state']})")
        print(f"Electricity rate: ${first['electricity_rate_usd_per_kwh']:.4f} per kWh")
        print(f"Poverty rate: {first['poverty_rate_percent']:.2f}%")
        print(f"Affordability pressure: {first['affordability_pressure']:.6f}")


if __name__ == "__main__":
    main()
