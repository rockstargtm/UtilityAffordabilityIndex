#!/usr/bin/env python3
"""
Utility Affordability Index Calculator - GTM Edition (Quick Fix)

This version is adapted to handle the specific EIA file format with:
- RESIDENTIAL_Revenues, RESIDENTIAL_Sales format (uppercase)
- Utility Characteristics_Unnamed columns for metadata

Edits included:
- Service Territory workbook sheet-name fix: supports Counties_States and Counties_Territories
- More robust sheet detection and fallback header parsing for the territory workbook
- Utility number normalization (string) so merges do not fail due to dtype mismatches
"""

import os
import sys
import argparse
import warnings
from typing import Optional
import pandas as pd
import requests

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Try to import visualization libraries (optional)
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

# Configuration
DEFAULT_SALES_FILE = "Sales_Ult_Cust_2024.xlsx"
DEFAULT_TERRITORY_FILE = "Service_Territory_2024.xlsx"
DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_TOP_N = 10

RATE_MIN_THRESHOLD = 0.01
RATE_MAX_THRESHOLD = 1.00

# Census API URLs - Try multiple years
ACS_YEARS = [2023, 2022, 2021]  # Census data lags, try recent years


def get_census_url(year: int, geography: str) -> str:
    """Generate Census API URL for given year and geography."""
    if geography == "county":
        return (
            f"https://api.census.gov/data/{year}/acs/acs5/subject"
            f"?get=NAME,S1701_C03_001E&for=county:*&in=state:*"
        )
    # state
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S1701_C03_001E&for=state:*"


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
}


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Calculate Utility Affordability Index")
    parser.add_argument("--sales", default=DEFAULT_SALES_FILE)
    parser.add_argument("--territory", default=DEFAULT_TERRITORY_FILE)
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--top", "-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--no-county-weighting", action="store_true")
    parser.add_argument("--keep-outliers", action="store_true")
    parser.add_argument("--no-viz", action="store_true")
    return parser.parse_args()


def log_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'=' * 70}")
    print(title)
    print("=" * 70)


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns."""
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
    """Drop units metadata row if present."""
    if len(df) == 0:
        return df
    first_row_str = " ".join([str(val) for val in df.iloc[0].values]).upper()
    if any(kw in first_row_str for kw in ["THOUSAND", "MEGAWATT", "MEGAWATTHOUR", "MILLION", "DOLLAR"]):
        return df.iloc[1:].reset_index(drop=True)
    return df


def load_eia_sales_data(filepath: str) -> pd.DataFrame:
    """Load EIA sales data - handles the specific format with RESIDENTIAL_Revenues."""
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
        print("✗ ERROR: Could not find sales data sheet")
        sys.exit(1)

    df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
    print(f"✓ Loaded {len(df):,} rows from '{sheet_name}'")

    df = drop_units_row(df)
    df = flatten_columns(df)
            # ---- DETECT utility, state, and county columns by name ----
    utility_col = None
    state_col = None
    county_col = None

    for col in df.columns:
            col_upper = col.upper()

            if utility_col is None and "UTILITY" in col_upper and "NUMBER" in col_upper:
                utility_col = col

            elif state_col is None and col_upper.startswith("STATE"):
                state_col = col

            elif county_col is None and col_upper.startswith("COUNTY"):
                county_col = col


        if not all([utility_col, state_col, county_col]):
            print("⚠ Missing required columns in territory sheet")
            print(f"  Utility column found: {'✓' if utility_col else '✗'}")
            print(f"  State column found: {'✓' if state_col else '✗'}")
            print(f"  County column found: {'✓' if county_col else '✗'}")

            print("\n  First 30 columns:")
            for c in df.columns[:30]:
                print(f"   - {c}")

            return None


        print(f"✓ Using columns:")
    print(f"   Utility: {utility_col}")
    print(f"   State:   {state_col}")
    print(f"   County:  {county_col}")
    
    df = df.rename(columns={
            utility_col: "utility_number",
            state_col: "state",
            county_col: "county",
        })
    
    df["state"] = df["state"].astype(str).str.strip().str.upper()
    df["county"] = df["county"].astype(str).str.strip()

return df[["utility_number", "state", "county"]].copy()


    print("\n📋 Detected column format:")
    print(f"   Sample columns: {list(df.columns[:5])}")

    util_char_cols = sorted([col for col in df.columns if "Utility Characteristics" in col])

    col_map = {}

    # Find revenue and sales
    for col in df.columns:
        col_upper = col.upper()
        if "RESIDENTIAL" in col_upper and "REVENUE" in col_upper:
            col_map["revenue"] = col
        elif "RESIDENTIAL" in col_upper and "SALES" in col_upper:
            col_map["sales"] = col

    # Detect utility characteristics columns by samples
    if util_char_cols:
        print(f"\n🔍 Found {len(util_char_cols)} Utility Characteristics columns")
        print("   Inspecting first few rows...")

        for i, col in enumerate(util_char_cols[:8]):
            sample_vals = df[col].dropna().head(3).tolist()
            print(f"   {col}: {sample_vals}")

            first_val = str(df[col].iloc[0]) if len(df) > 0 else ""

            if first_val.isdigit() and len(first_val) == 4:
                col_map["data_year"] = col
            elif first_val.isdigit() and 4 <= len(first_val) <= 7:
                if "utility_number" not in col_map:
                    col_map["utility_number"] = col
            elif any(c.isalpha() for c in first_val) and len(first_val) > 5:
                if "utility_name" not in col_map:
                    col_map["utility_name"] = col
            elif first_val.isalpha() and len(first_val) == 2:
                col_map["state"] = col

    # Fallback positional mapping
    if "data_year" not in col_map and len(util_char_cols) > 0:
        col_map["data_year"] = util_char_cols[0]
    if "utility_number" not in col_map and len(util_char_cols) > 1:
        col_map["utility_number"] = util_char_cols[1]
    if "utility_name" not in col_map and len(util_char_cols) > 2:
        col_map["utility_name"] = util_char_cols[2]
    if "state" not in col_map and len(util_char_cols) > 3:
        col_map["state"] = util_char_cols[3]

    print("\n✓ Column mapping:")
    for k, v in col_map.items():
        print(f"   {k}: {v}")

    required = ["data_year", "utility_number", "utility_name", "state", "revenue", "sales"]
    missing = [k for k in required if k not in col_map]

    if missing:
        print(f"\n✗ ERROR: Could not auto-detect columns: {', '.join(missing)}")
        print("\n📋 All available columns:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. {col}")
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

    # Normalize identifiers and state
    df["utility_number"] = df["utility_number"].astype(str).str.strip().str.replace(".0", "", regex=False)
    df["state"] = df["state"].astype(str).str.strip().str.upper()
    df["state_full_name"] = df["state"].map(STATE_ABBREV_TO_NAME)

    print(f"✓ Successfully processed {len(df):,} rows")
    return df


def load_service_territory_data(filepath: str) -> Optional[pd.DataFrame]:
    """Load service territory data (Schedule 9 / Service Territory), supports Counties_States."""
    log_section("Loading Service Territory Data")

    if not os.path.exists(filepath):
        print(f"⚠ File '{filepath}' not found - will use state-level poverty")
        return None

    try:
        xls = pd.ExcelFile(filepath)
        print(f"Available sheets: {xls.sheet_names}")

        # IMPORTANT: Your file has these sheet names
        preferred_sheet_names = [
            "Counties_States",
            "Counties_Territories"
        ]

        sheet_name = None
        for name in preferred_sheet_names:
            if name in xls.sheet_names:
                sheet_name = name
                break

        if not sheet_name:
            print("⚠ Could not find a territory sheet (expected Counties_States or Counties_Territories)")
            return None

        # Try multi-header first, then fallback to single header
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
            df = drop_units_row(df)
            df = flatten_columns(df)
        except Exception:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=0)
            df = drop_units_row(df)
            df = flatten_columns(df)

        print(f"✓ Loaded {len(df):,} rows from '{sheet_name}'")

        utility_col = None
        fips_col = None
        weight_col = None

        for col in df.columns:
            col_upper = str(col).upper()

            if utility_col is None and ("UTILITY" in col_upper) and (("NUMBER" in col_upper) or ("ID" in col_upper)):
                utility_col = col

            if fips_col is None and ("FIPS" in col_upper or ("COUNTY" in col_upper and "CODE" in col_upper)):
                fips_col = col

            if weight_col is None and any(kw in col_upper for kw in ["CUSTOMER", "CUSTOMERS", "PERCENT", "SHARE", "PCT"]):
                weight_col = col

        if not all([utility_col, fips_col, weight_col]):
            print("⚠ Missing required columns in territory sheet")
            print(f"  Utility column found: {'✓' if utility_col else '✗'}")
            print(f"  FIPS column found: {'✓' if fips_col else '✗'}")
            print(f"  Weight column found: {'✓' if weight_col else '✗'}")
            print("\n  First 30 columns:")
            for c in list(df.columns[:30]):
                print(f"   - {c}")
            return None

        df = df.rename(columns={
            utility_col: "utility_number",
            fips_col: "fips_code",
            weight_col: "weight_value",
        })

        # Normalize
        df["utility_number"] = df["utility_number"].astype(str).str.strip().str.replace(".0", "", regex=False)

        df["fips_code"] = (
            df["fips_code"]
            .astype(str)
            .str.strip()
            .str.replace(".0", "", regex=False)
            .str.zfill(5)
        )

        df["weight_value"] = pd.to_numeric(df["weight_value"], errors="coerce")

        df = df[df["fips_code"].str.len() == 5].copy()
        df = df[df["weight_value"].notna() & (df["weight_value"] > 0)].copy()

        print(f"✓ Loaded {len(df):,} county-utility records after cleaning")
        print(f"  Utilities covered: {df['utility_number'].nunique():,}")
        print(f"  Counties covered: {df['fips_code'].nunique():,}")

        return df[["utility_number", "fips_code", "weight_value"]].copy()

    except Exception as e:
        print(f"⚠ Error reading territory file: {e}")
        return None


def fetch_county_poverty() -> Optional[pd.DataFrame]:
    """Fetch county poverty data - tries multiple years."""
    log_section("Fetching County Poverty Data")

    for year in ACS_YEARS:
        try:
            url = get_census_url(year, "county")
            print(f"Trying Census ACS {year}...")
            response = requests.get(url, timeout=15)

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data[1:], columns=data[0])

                df["state"] = df["state"].astype(str).str.zfill(2)
                df["county"] = df["county"].astype(str).str.zfill(3)
                df["fips_code"] = df["state"] + df["county"]

                df["poverty_rate_percent"] = pd.to_numeric(df["S1701_C03_001E"], errors="coerce")
                df = df[
                    df["poverty_rate_percent"].notna()
                    & (df["poverty_rate_percent"] >= 0)
                    & (df["poverty_rate_percent"] <= 100)
                ].copy()

                print(f"✓ Loaded {len(df):,} counties from {year} ACS data")
                return df[["fips_code", "NAME", "poverty_rate_percent"]].copy()

            print(f"  ✗ HTTP {response.status_code}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    print("✗ Failed to fetch county data from all years")
    return None


def fetch_state_poverty() -> pd.DataFrame:
    """Fetch state poverty data - checks uploads, then API, then local files."""
    log_section("Fetching State Poverty Data")

    census_upload_files = [
        "/mnt/user-data/uploads/ACSST5Y2023_S1701-Data.csv",
        "/mnt/user-data/uploads/ACSST5Y2022_S1701-Data.csv",
    ]

    for filepath in census_upload_files:
        if os.path.exists(filepath):
            try:
                print(f"Found uploaded Census file: {os.path.basename(filepath)}")
                df = pd.read_csv(filepath)

                poverty_col = [col for col in df.columns if "S1701_C03_001E" in col]

                if poverty_col and "NAME" in df.columns:
                    result = df[["NAME", poverty_col[0]]].copy()
                    result.columns = ["NAME", "poverty_rate_percent"]

                    result = result[result["NAME"] != "Geographic Area Name"]
                    result["poverty_rate_percent"] = pd.to_numeric(result["poverty_rate_percent"], errors="coerce")
                    result = result[result["poverty_rate_percent"].notna()]

                    print(f"✓ Loaded {len(result)} states from uploaded Census file")
                    return result[["NAME", "poverty_rate_percent"]].copy()
            except Exception as e:
                print(f"  ✗ Could not read {filepath}: {e}")

    print("\nNo uploaded Census files found. Trying API...")
    for year in ACS_YEARS:
        try:
            url = get_census_url(year, "state")
            print(f"Trying Census API {year}...")
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data[1:], columns=data[0])

                df["poverty_rate_percent"] = pd.to_numeric(df["S1701_C03_001E"], errors="coerce")
                df = df[
                    df["poverty_rate_percent"].notna()
                    & (df["poverty_rate_percent"] >= 0)
                    & (df["poverty_rate_percent"] <= 100)
                ].copy()

                print(f"✓ Loaded {len(df):,} states from {year} Census API")
                return df[["NAME", "poverty_rate_percent"]].copy()

            print(f"  ✗ HTTP {response.status_code}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    print("\n⚠ Census API unavailable. Searching for local CSV files...")

    fallback_files = [
        "state_poverty_2023_acs5yr.csv",
        "state_poverty_2024.csv",
        "state_poverty.csv",
        "poverty_data.csv",
        "acs_state_poverty.csv",
    ]

    for filename in fallback_files:
        if os.path.exists(filename):
            print(f"✓ Found local file: {filename}")
            try:
                df = pd.read_csv(filename)

                name_col = None
                poverty_col = None

                for col in df.columns:
                    col_lower = str(col).lower()
                    if not name_col and ("name" in col_lower or "state" in col_lower or "geography" in col_lower):
                        name_col = col
                    if not poverty_col and ("poverty" in col_lower or "s1701" in col_lower or "percent" in col_lower):
                        poverty_col = col

                if name_col and poverty_col:
                    df = df.rename(columns={name_col: "NAME", poverty_col: "poverty_rate_percent"})
                    df["poverty_rate_percent"] = pd.to_numeric(df["poverty_rate_percent"], errors="coerce")
                    df = df[df["poverty_rate_percent"].notna()].copy()
                    print(f"✓ Loaded {len(df)} states from local file")
                    return df[["NAME", "poverty_rate_percent"]].copy()

                print(f"  ✗ Could not find NAME and poverty columns in {filename}")
            except Exception as e:
                print(f"  ✗ Could not read {filename}: {e}")

    print("\n✗ ERROR: Could not load state poverty data")
    print("\n📥 SOLUTION: Provide a state poverty CSV in the working directory and re-run.")
    sys.exit(1)


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean sales data."""
    log_section("Cleaning Sales Data")
    # Drop EIA adjustment rows (not real utilities)
    df["utility_name"] = df["utility_name"].astype(str)
    df = df[~df["utility_name"].str.contains("ADJUSTMENT", case=False, na=False)].copy()

    initial = len(df)
    df["data_year"] = pd.to_numeric(df["data_year"], errors="coerce")
    df = df[df["data_year"] == 2024].copy()
    print(f"Year 2024: {len(df):,}/{initial:,} rows")

    df["residential_revenue_thousand_usd"] = pd.to_numeric(df["residential_revenue_thousand_usd"], errors="coerce")
    df["residential_sales_mwh"] = pd.to_numeric(df["residential_sales_mwh"], errors="coerce")

    df = df[
    df['residential_revenue_thousand_usd'].notna() & (df['residential_revenue_thousand_usd'] > 0) &
    df['residential_sales_mwh'].notna() & (df['residential_sales_mwh'] >= 1000)
    ].copy()

    
   # Drop extremely small utilities or partial reporters
   # df = df[df["residential_sales_mwh"] >= 1000].copy()
   # print(f"After min sales filter (>=1000 MWh): {len(df):,} rows")

    return df


def compute_rates(df: pd.DataFrame, keep_outliers: bool = False) -> pd.DataFrame:
    """Compute rates."""
    log_section("Computing Rates")

    df["electricity_rate_usd_per_kwh"] = (
        (df["residential_revenue_thousand_usd"] * 1000) / (df["residential_sales_mwh"] * 1000)
    )

    print(
        f"Rate stats: ${df['electricity_rate_usd_per_kwh'].min():.4f} - "
        f"${df['electricity_rate_usd_per_kwh'].max():.4f}"
    )

    outliers = (
        (df["electricity_rate_usd_per_kwh"] < RATE_MIN_THRESHOLD)
        | (df["electricity_rate_usd_per_kwh"] > RATE_MAX_THRESHOLD)
    )

    if outliers.sum() > 0:
        print(f"⚠ {int(outliers.sum())} outliers")
        if not keep_outliers:
            df = df[~outliers].copy()

    return df


def compute_weighted_poverty(territory_df: pd.DataFrame, county_poverty_df: pd.DataFrame) -> pd.DataFrame:
    """Compute weighted poverty."""
    log_section("Computing Weighted Poverty")

    df = territory_df.merge(county_poverty_df, on="fips_code", how="left")
    df = df[df["poverty_rate_percent"].notna()].copy()

    if len(df) == 0:
        return pd.DataFrame()

    df["basis_weight"] = df["weight_value"].astype(float)
    max_weight = df["basis_weight"].max()

    # If weights look like percent shares, normalize to 0-1
    if max_weight <= 100:
        df["basis_weight"] = df["basis_weight"] / 100.0 if max_weight > 1 else df["basis_weight"]

    totals = df.groupby("utility_number")["basis_weight"].sum().reset_index()
    totals.columns = ["utility_number", "service_territory_weight"]

    df = df.merge(totals, on="utility_number", how="left")
    df["normalized_weight"] = df["basis_weight"] / df["service_territory_weight"]
    df["weighted_poverty_component"] = df["poverty_rate_percent"] * df["normalized_weight"]

    result = (
        df.groupby("utility_number")
        .agg(
            weighted_poverty_component=("weighted_poverty_component", "sum"),
            service_territory_weight=("service_territory_weight", "first"),
            counties_covered=("fips_code", "nunique"),
        )
        .reset_index()
    )

    result.columns = ["utility_number", "poverty_rate_percent", "service_territory_weight", "counties_covered"]

    print(f"✓ Weighted poverty for {len(result):,} utilities")
    return result


def join_state_poverty(eia_df: pd.DataFrame, poverty_df: pd.DataFrame) -> pd.DataFrame:
    """Join state poverty."""
    log_section("Joining State Poverty")

    poverty_df = poverty_df.rename(columns={"NAME": "state_full_name"})
    df = eia_df.merge(poverty_df, on="state_full_name", how="left")
    df = df[df["poverty_rate_percent"].notna()].copy()

    print(f"✓ Matched {len(df):,} utilities")
    return df


def compute_affordability_pressure(df: pd.DataFrame) -> pd.DataFrame:
    """Compute affordability pressure."""
    log_section("Computing Affordability Pressure")

    df["poverty_rate_decimal"] = df["poverty_rate_percent"] / 100.0
    df["affordability_pressure"] = df["electricity_rate_usd_per_kwh"] * df["poverty_rate_decimal"]
    df = df[df["affordability_pressure"].notna()].copy()

    print(f"✓ Computed for {len(df):,} utilities")
    return df


def main():
    """Main pipeline."""
    args = parse_args()

    print("\n" + "=" * 70)
    print("UTILITY AFFORDABILITY INDEX CALCULATOR - GTM EDITION")
    print("=" * 70)

    os.makedirs(args.output_dir, exist_ok=True)

    # Load and process
    eia_df = load_eia_sales_data(args.sales)
    eia_df = clean_sales_data(eia_df)
    eia_df = compute_rates(eia_df, keep_outliers=args.keep_outliers)

    # Get poverty data
    method = "State-level"
    use_county = not args.no_county_weighting

    if use_county:
        territory_df = load_service_territory_data(args.territory)
        county_poverty_df = fetch_county_poverty()

        if territory_df is not None and county_poverty_df is not None:
            poverty_df = compute_weighted_poverty(territory_df, county_poverty_df)
            if len(poverty_df) > 0:
                # Ensure utility_number is consistent dtype before merge
                poverty_df["utility_number"] = poverty_df["utility_number"].astype(str).str.strip()
                eia_df["utility_number"] = eia_df["utility_number"].astype(str).str.strip()

                eia_df = eia_df.merge(poverty_df, on="utility_number", how="inner")
                method = "County-weighted"
            else:
                state_poverty_df = fetch_state_poverty()
                eia_df = join_state_poverty(eia_df, state_poverty_df)
        else:
            state_poverty_df = fetch_state_poverty()
            eia_df = join_state_poverty(eia_df, state_poverty_df)
    else:
        state_poverty_df = fetch_state_poverty()
        eia_df = join_state_poverty(eia_df, state_poverty_df)

    # Compute final metric
    eia_df = compute_affordability_pressure(eia_df)

    # Rank
    eia_df = eia_df.sort_values("affordability_pressure", ascending=False).copy()
    top_df = eia_df.head(args.top).copy()
    top_df["rank"] = range(1, len(top_df) + 1)

    # Save
    log_section("Saving Output")

    top_df["residential_revenue_usd"] = top_df["residential_revenue_thousand_usd"] * 1000
    top_df["utility_id"] = top_df["utility_number"].astype(str)

    output_path = os.path.join(args.output_dir, "utility_affordability_index_top10.csv")
    top_df.to_csv(output_path, index=False)
    print(f"✓ Saved: {output_path}")

    # Summary
    log_section("COMPLETE")
    print(f"✓ Top {len(top_df)} utilities identified")
    print(f"✓ Methodology: {method}")
    print(f"\n🎯 #1: {top_df.iloc[0]['utility_name']} ({top_df.iloc[0]['state']})")
    print(f"   Rate: ${top_df.iloc[0]['electricity_rate_usd_per_kwh']:.4f}/kWh")
    print(f"   Poverty: {top_df.iloc[0]['poverty_rate_percent']:.2f}%")


if __name__ == "__main__":
    main()
