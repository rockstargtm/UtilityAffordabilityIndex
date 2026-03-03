#!/usr/bin/env python3
"""
Utility Affordability Index Calculator - GTM Edition

Combines EIA-861 electricity data with Census poverty data to identify utilities
where high electricity rates collide with high poverty rates.

This creates a "Provoke" for sales conversations:
"Your customers are paying X% more than the national average while Y% live below poverty."

Key improvements over original:
- CLI arguments for flexibility
- Summary statistics for storytelling
- Automatic visualization generation
- Better documentation and metadata
- Streamlined while maintaining robustness
- Python 3.9+ compatible
"""

import os
import sys
import argparse
import warnings
from pathlib import Path
from typing import Optional, Dict, Tuple
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
    print("⚠ Visualization libraries not available. Install matplotlib and seaborn for charts.")

# -----------------------------
# Configuration
# -----------------------------
DEFAULT_SALES_FILE = "Sales_Ult_Cust_2024.xlsx"
DEFAULT_TERRITORY_FILE = "Service_Territory_2024.xlsx"
DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_TOP_N = 10

RATE_MIN_THRESHOLD = 0.01  # $/kWh - flags likely data errors
RATE_MAX_THRESHOLD = 1.00  # $/kWh - flags likely data errors

# Census API URLs
ACS_5YR_COUNTY_URL = (
    "https://api.census.gov/data/2024/acs/acs5/subject?"
    "get=NAME,S1701_C03_001E&for=county:*&in=state:*"
)
ACS_5YR_STATE_URL = (
    "https://api.census.gov/data/2024/acs/acs5/subject?"
    "get=NAME,S1701_C03_001E&for=state:*"
)

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
    parser = argparse.ArgumentParser(
        description="Calculate Utility Affordability Index - where high rates meet high poverty",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --sales Sales_Ult_Cust_2024.xlsx --territory Service_Territory_2024.xlsx
  %(prog)s --top 20 --no-county-weighting
  %(prog)s --output-dir results --keep-outliers
        """
    )
    
    parser.add_argument(
        "--sales", 
        default=DEFAULT_SALES_FILE,
        help=f"EIA-861 sales file (default: {DEFAULT_SALES_FILE})"
    )
    parser.add_argument(
        "--territory", 
        default=DEFAULT_TERRITORY_FILE,
        help=f"EIA Schedule 9 service territory file (default: {DEFAULT_TERRITORY_FILE})"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--top", "-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Number of top utilities to return (default: {DEFAULT_TOP_N})"
    )
    parser.add_argument(
        "--no-county-weighting",
        action="store_true",
        help="Use state-level poverty instead of county-weighted"
    )
    parser.add_argument(
        "--keep-outliers",
        action="store_true",
        help="Keep rate outliers instead of flagging them"
    )
    parser.add_argument(
        "--no-viz",
        action="store_true",
        help="Skip visualization generation"
    )
    
    return parser.parse_args()


# -----------------------------
# Utility Functions
# -----------------------------

def log_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'=' * 70}")
    print(f"{title}")
    print('=' * 70)


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns to single-level names."""
    new_cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            cleaned = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != "nan"]
            new_cols.append("_".join(cleaned))
        else:
            new_cols.append(str(col).strip())
    df.columns = new_cols
    return df


def find_column(df: pd.DataFrame, *keywords) -> Optional[str]:
    """Find first column name containing all keywords (case-insensitive)."""
    for col in df.columns:
        s = str(col).upper()
        if all(str(k).upper() in s for k in keywords):
            return col
    return None


def drop_units_row(df: pd.DataFrame) -> pd.DataFrame:
    """Drop units metadata row if detected in first row."""
    if len(df) == 0:
        return df
    first_row_str = " ".join([str(val) for val in df.iloc[0].values]).upper()
    if any(kw in first_row_str for kw in ["THOUSAND", "MEGAWATT", "MEGAWATTHOUR", "MILLION", "DOLLAR"]):
        return df.iloc[1:].reset_index(drop=True)
    return df


def detect_weight_basis(max_weight: float) -> str:
    """Determine what the territory weight column likely represents."""
    if max_weight <= 1.0:
        return "share_0_1"
    if max_weight <= 100.0:
        return "percent_0_100"
    return "customer_count"


# -----------------------------
# Data Loading Functions
# -----------------------------

def load_eia_sales_data(filepath: str) -> pd.DataFrame:
    """Load and preprocess EIA-861 sales data."""
    log_section("Loading EIA-861 Sales Data")
    
    if not os.path.exists(filepath):
        print(f"✗ ERROR: File '{filepath}' not found")
        sys.exit(1)
    
    # Read file and detect sheet
    xls = pd.ExcelFile(filepath)
    print(f"Available sheets: {xls.sheet_names}")
    
    sheet_name = None
    for name in ["States", "State", "Data"]:
        if name in xls.sheet_names:
            sheet_name = name
            break
    
    if not sheet_name:
        print(f"✗ ERROR: Could not find sales data sheet")
        sys.exit(1)
    
    df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
    print(f"✓ Loaded {len(df):,} rows from '{sheet_name}'")
    
    # Clean up structure
    df = drop_units_row(df)
    df = flatten_columns(df)
    
    # Find required columns
    col_map = {
        'data_year': find_column(df, "data", "year") or find_column(df, "year"),
        'utility_number': find_column(df, "utility", "number") or find_column(df, "utility", "id"),
        'utility_name': find_column(df, "utility", "name"),
        'state': find_column(df, "state"),
        'revenue': find_column(df, "residential", "revenue"),
        'sales': find_column(df, "residential", "sales"),
    }
    
    missing = [k for k, v in col_map.items() if v is None]
    if missing:
        print(f"✗ ERROR: Missing columns: {', '.join(missing)}")
        print("\nAvailable columns:")
        for i, col in enumerate(sorted(df.columns)[:30], 1):
            print(f"  {i:2d}. {col}")
        sys.exit(1)
    
    # Rename and subset
    df = df.rename(columns={
        col_map['data_year']: 'data_year',
        col_map['utility_number']: 'utility_number',
        col_map['utility_name']: 'utility_name',
        col_map['state']: 'state',
        col_map['revenue']: 'residential_revenue_thousand_usd',
        col_map['sales']: 'residential_sales_mwh',
    })
    
    df = df[[
        'data_year', 'utility_number', 'utility_name', 'state',
        'residential_revenue_thousand_usd', 'residential_sales_mwh'
    ]].copy()
    
    # Normalize state
    df['state'] = df['state'].astype(str).str.strip().str.upper()
    df['state_full_name'] = df['state'].map(STATE_ABBREV_TO_NAME)
    
    print(f"✓ Successfully processed sales data")
    return df


def load_service_territory_data(filepath: str) -> Optional[pd.DataFrame]:
    """Load EIA Schedule 9 service territory data."""
    log_section("Loading Service Territory Data")
    
    if not os.path.exists(filepath):
        print(f"⚠ File '{filepath}' not found - will use state-level poverty")
        return None
    
    try:
        xls = pd.ExcelFile(filepath)
        
        # Find the territory sheet
        sheet_name = None
        for name in ["Service_Territory", "Service Territory", "Data", "Territory"]:
            if name in xls.sheet_names:
                sheet_name = name
                break
        
        if not sheet_name:
            print("⚠ Could not find territory sheet - will use state-level poverty")
            return None
        
        df = pd.read_excel(filepath, sheet_name=sheet_name, header=[0, 1])
        df = drop_units_row(df)
        df = flatten_columns(df)
        
        # Find required columns
        utility_col = find_column(df, "utility", "number") or find_column(df, "utility", "id")
        fips_col = find_column(df, "fips") or find_column(df, "county", "code")
        weight_col = (
            find_column(df, "customer") or find_column(df, "customers") or
            find_column(df, "residential", "customer") or
            find_column(df, "percent") or find_column(df, "share")
        )
        
        if not all([utility_col, fips_col, weight_col]):
            print("⚠ Missing required columns - will use state-level poverty")
            return None
        
        df = df.rename(columns={
            utility_col: 'utility_number',
            fips_col: 'fips_code',
            weight_col: 'weight_value',
        })
        
        # Clean FIPS codes
        df['fips_code'] = df['fips_code'].astype(str).str.strip().str.replace('.0', '', regex=False)
        df['fips_code'] = df['fips_code'].str.zfill(5)
        df['weight_value'] = pd.to_numeric(df['weight_value'], errors='coerce')
        
        # Filter valid records
        df = df[df['fips_code'].str.len() == 5].copy()
        df = df[df['weight_value'].notna() & (df['weight_value'] > 0)].copy()
        
        print(f"✓ Loaded {len(df):,} county-utility records")
        print(f"  {df['utility_number'].nunique():,} utilities × {df['fips_code'].nunique():,} counties")
        
        return df[['utility_number', 'fips_code', 'weight_value']].copy()
        
    except Exception as e:
        print(f"⚠ Error loading territory file: {e}")
        print("  Will use state-level poverty")
        return None


def fetch_county_poverty() -> Optional[pd.DataFrame]:
    """Fetch county-level poverty data from Census ACS 5-year."""
    log_section("Fetching County-Level Poverty Data")
    
    try:
        response = requests.get(ACS_5YR_COUNTY_URL, timeout=15)
        if response.status_code != 200:
            print(f"✗ Census API returned HTTP {response.status_code}")
            return None
        
        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # Build FIPS codes
        df['state'] = df['state'].astype(str).str.zfill(2)
        df['county'] = df['county'].astype(str).str.zfill(3)
        df['fips_code'] = df['state'] + df['county']
        
        # Parse poverty rate
        df['poverty_rate_percent'] = pd.to_numeric(df['S1701_C03_001E'], errors='coerce')
        df = df[
            df['poverty_rate_percent'].notna() &
            (df['poverty_rate_percent'] >= 0) &
            (df['poverty_rate_percent'] <= 100)
        ].copy()
        
        print(f"✓ Loaded poverty data for {len(df):,} counties")
        return df[['fips_code', 'NAME', 'poverty_rate_percent']].copy()
        
    except Exception as e:
        print(f"✗ Error fetching county poverty: {e}")
        return None


def fetch_state_poverty() -> pd.DataFrame:
    """Fetch state-level poverty data from Census ACS 5-year."""
    log_section("Fetching State-Level Poverty Data")
    
    try:
        response = requests.get(ACS_5YR_STATE_URL, timeout=15)
        if response.status_code != 200:
            print(f"✗ Census API returned HTTP {response.status_code}")
            sys.exit(1)
        
        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # Parse poverty rate
        df['poverty_rate_percent'] = pd.to_numeric(df['S1701_C03_001E'], errors='coerce')
        df = df[
            df['poverty_rate_percent'].notna() &
            (df['poverty_rate_percent'] >= 0) &
            (df['poverty_rate_percent'] <= 100)
        ].copy()
        
        print(f"✓ Loaded poverty data for {len(df):,} states/territories")
        return df[['NAME', 'poverty_rate_percent']].copy()
        
    except Exception as e:
        print(f"✗ Error fetching state poverty: {e}")
        sys.exit(1)


# -----------------------------
# Data Processing Functions
# -----------------------------

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data quality filters to sales data."""
    log_section("Cleaning Sales Data")
    
    initial_rows = len(df)
    
    # Filter to 2024
    df['data_year'] = pd.to_numeric(df['data_year'], errors='coerce')
    df = df[df['data_year'] == 2024].copy()
    print(f"Year 2024 filter: {len(df):,} / {initial_rows:,} rows ({len(df)/initial_rows*100:.1f}%)")
    
    # Convert numeric columns
    df['residential_revenue_thousand_usd'] = pd.to_numeric(df['residential_revenue_thousand_usd'], errors='coerce')
    df['residential_sales_mwh'] = pd.to_numeric(df['residential_sales_mwh'], errors='coerce')
    
    # Filter positive values
    df = df[
        df['residential_revenue_thousand_usd'].notna() & 
        (df['residential_revenue_thousand_usd'] > 0) &
        df['residential_sales_mwh'].notna() & 
        (df['residential_sales_mwh'] > 0)
    ].copy()
    
    print(f"Valid revenue & sales: {len(df):,} / {initial_rows:,} rows ({len(df)/initial_rows*100:.1f}%)")
    
    return df


def compute_rates(df: pd.DataFrame, keep_outliers: bool = False) -> pd.DataFrame:
    """Compute electricity rates and optionally flag outliers."""
    log_section("Computing Electricity Rates")
    
    # Calculate rate
    df['electricity_rate_usd_per_kwh'] = (
        (df['residential_revenue_thousand_usd'] * 1000) / 
        (df['residential_sales_mwh'] * 1000)
    )
    
    print(f"Rate statistics:")
    print(f"  Min:    ${df['electricity_rate_usd_per_kwh'].min():.6f}/kWh")
    print(f"  Median: ${df['electricity_rate_usd_per_kwh'].median():.6f}/kWh")
    print(f"  Mean:   ${df['electricity_rate_usd_per_kwh'].mean():.6f}/kWh")
    print(f"  Max:    ${df['electricity_rate_usd_per_kwh'].max():.6f}/kWh")
    
    # Check for outliers
    outlier_mask = (
        (df['electricity_rate_usd_per_kwh'] < RATE_MIN_THRESHOLD) |
        (df['electricity_rate_usd_per_kwh'] > RATE_MAX_THRESHOLD)
    )
    outlier_count = outlier_mask.sum()
    
    if outlier_count > 0:
        print(f"\n⚠ Found {outlier_count} outliers outside ${RATE_MIN_THRESHOLD:.2f}-${RATE_MAX_THRESHOLD:.2f}/kWh range")
        
        if not keep_outliers:
            df = df[~outlier_mask].copy()
            print(f"  Removed outliers: {len(df):,} utilities remaining")
        else:
            df['is_outlier'] = outlier_mask
            print(f"  Flagged but kept outliers")
    
    return df


def compute_weighted_poverty(territory_df: pd.DataFrame, county_poverty_df: pd.DataFrame) -> pd.DataFrame:
    """Compute utility-level poverty rate weighted by county coverage."""
    log_section("Computing County-Weighted Poverty Rates")
    
    # Merge territory with poverty data
    df = territory_df.merge(county_poverty_df, on='fips_code', how='left')
    
    matched = df['poverty_rate_percent'].notna().sum()
    total = len(df)
    print(f"County match rate: {matched:,}/{total:,} ({matched/total*100:.1f}%)")
    
    # Drop unmatched
    df = df[df['poverty_rate_percent'].notna()].copy()
    
    if len(df) == 0:
        print("✗ No matched records - cannot compute weighted poverty")
        return pd.DataFrame()
    
    # Detect weight basis
    max_weight = float(df['weight_value'].max())
    weight_basis = detect_weight_basis(max_weight)
    print(f"✓ Weight basis: {weight_basis} (max={max_weight:.2f})")
    
    # Normalize weights
    df['basis_weight'] = df['weight_value'].astype(float)
    if weight_basis == "percent_0_100":
        df['basis_weight'] = df['basis_weight'] / 100.0
    
    # Calculate total weight per utility
    totals = df.groupby('utility_number')['basis_weight'].sum().reset_index()
    totals.columns = ['utility_number', 'service_territory_weight']
    
    df = df.merge(totals, on='utility_number')
    df['normalized_weight'] = df['basis_weight'] / df['service_territory_weight']
    df['weighted_poverty_component'] = df['poverty_rate_percent'] * df['normalized_weight']
    
    # Aggregate by utility
    result = df.groupby('utility_number').agg({
        'weighted_poverty_component': 'sum',
        'service_territory_weight': 'first',
        'fips_code': 'nunique'
    }).reset_index()
    
    result.columns = ['utility_number', 'poverty_rate_percent', 'service_territory_weight', 'counties_covered']
    result['weight_basis'] = weight_basis
    
    print(f"✓ Computed weighted poverty for {len(result):,} utilities")
    print(f"  Poverty range: {result['poverty_rate_percent'].min():.2f}% - {result['poverty_rate_percent'].max():.2f}%")
    
    return result


def join_state_poverty(eia_df: pd.DataFrame, poverty_df: pd.DataFrame) -> pd.DataFrame:
    """Join EIA data with state-level poverty."""
    log_section("Joining State-Level Poverty Data")
    
    poverty_df = poverty_df.rename(columns={'NAME': 'state_full_name'})
    
    df = eia_df.merge(poverty_df, on='state_full_name', how='left')
    
    matched = df['poverty_rate_percent'].notna().sum()
    total = len(df)
    print(f"State match rate: {matched:,}/{total:,} ({matched/total*100:.1f}%)")
    
    df = df[df['poverty_rate_percent'].notna()].copy()
    
    return df


def compute_affordability_pressure(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate affordability pressure metric."""
    log_section("Computing Affordability Pressure")
    
    df['poverty_rate_decimal'] = df['poverty_rate_percent'] / 100.0
    df['affordability_pressure'] = df['electricity_rate_usd_per_kwh'] * df['poverty_rate_decimal']
    
    # Remove invalid values
    df = df[
        df['affordability_pressure'].notna() &
        ~df['affordability_pressure'].isin([float('inf'), float('-inf')])
    ].copy()
    
    print(f"✓ Computed affordability pressure for {len(df):,} utilities")
    print(f"  Range: {df['affordability_pressure'].min():.6f} - {df['affordability_pressure'].max():.6f}")
    
    return df


# -----------------------------
# Analysis & Output Functions
# -----------------------------

def calculate_summary_statistics(df: pd.DataFrame, top_df: pd.DataFrame) -> Dict:
    """Calculate summary statistics for GTM storytelling."""
    log_section("Calculating Summary Statistics")
    
    stats = {
        'total_utilities': len(df),
        'national_avg_rate': df['electricity_rate_usd_per_kwh'].mean(),
        'national_median_rate': df['electricity_rate_usd_per_kwh'].median(),
        'national_avg_poverty': df['poverty_rate_percent'].mean(),
        'national_median_poverty': df['poverty_rate_percent'].median(),
        'top_avg_rate': top_df['electricity_rate_usd_per_kwh'].mean(),
        'top_avg_poverty': top_df['poverty_rate_percent'].mean(),
    }
    
    # Calculate differentials
    stats['top_rate_premium_pct'] = (
        (stats['top_avg_rate'] - stats['national_avg_rate']) / stats['national_avg_rate'] * 100
    )
    stats['top_poverty_premium_pct'] = (
        (stats['top_avg_poverty'] - stats['national_avg_poverty']) / stats['national_avg_poverty'] * 100
    )
    
    print(f"✓ Summary Statistics:")
    print(f"  Total utilities analyzed: {stats['total_utilities']:,}")
    print(f"  National avg rate: ${stats['national_avg_rate']:.4f}/kWh")
    print(f"  National avg poverty: {stats['national_avg_poverty']:.2f}%")
    print(f"  Top {len(top_df)} avg rate: ${stats['top_avg_rate']:.4f}/kWh ({stats['top_rate_premium_pct']:+.1f}%)")
    print(f"  Top {len(top_df)} avg poverty: {stats['top_avg_poverty']:.2f}% ({stats['top_poverty_premium_pct']:+.1f}%)")
    
    return stats


def create_visualizations(top_df: pd.DataFrame, stats: Dict, output_dir: str):
    """Create GTM-ready visualizations."""
    if not VISUALIZATION_AVAILABLE:
        print("\n⚠ Skipping visualizations (matplotlib/seaborn not installed)")
        return
    
    log_section("Creating Visualizations")
    
    sns.set_style("whitegrid")
    
    # 1. Dual-axis bar chart: Rate & Poverty
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    x = range(len(top_df))
    width = 0.35
    
    # Rates on left axis
    ax1.bar([i - width/2 for i in x], top_df['electricity_rate_usd_per_kwh'], 
            width, label='Electricity Rate ($/kWh)', color='#e74c3c', alpha=0.8)
    ax1.set_xlabel('Utility', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Electricity Rate ($/kWh)', fontsize=12, fontweight='bold', color='#e74c3c')
    ax1.tick_params(axis='y', labelcolor='#e74c3c')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{row['rank']}. {row['utility_name'][:25]}" 
                         for _, row in top_df.iterrows()], 
                        rotation=45, ha='right', fontsize=9)
    
    # Poverty on right axis
    ax2 = ax1.twinx()
    ax2.bar([i + width/2 for i in x], top_df['poverty_rate_percent'], 
            width, label='Poverty Rate (%)', color='#3498db', alpha=0.8)
    ax2.set_ylabel('Poverty Rate (%)', fontsize=12, fontweight='bold', color='#3498db')
    ax2.tick_params(axis='y', labelcolor='#3498db')
    
    plt.title('Top 10 Utilities by Affordability Pressure\nHigh Rates + High Poverty', 
              fontsize=16, fontweight='bold', pad=20)
    
    # Add legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top10_rate_poverty_comparison.png'), dpi=300, bbox_inches='tight')
    print(f"✓ Saved: top10_rate_poverty_comparison.png")
    plt.close()
    
    # 2. Affordability Pressure Ranking
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = plt.cm.RdYlGn_r(top_df['affordability_pressure'] / top_df['affordability_pressure'].max())
    bars = ax.barh(range(len(top_df)), top_df['affordability_pressure'], color=colors)
    
    ax.set_yticks(range(len(top_df)))
    ax.set_yticklabels([f"{row['rank']}. {row['utility_name'][:30]}" 
                        for _, row in top_df.iterrows()], fontsize=10)
    ax.set_xlabel('Affordability Pressure (Rate × Poverty)', fontsize=12, fontweight='bold')
    ax.set_title('Utility Affordability Pressure Index\nWhere High Rates Meet High Poverty', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.invert_yaxis()
    
    # Add value labels
    for i, (_, row) in enumerate(top_df.iterrows()):
        ax.text(row['affordability_pressure'], i, 
                f"  {row['affordability_pressure']:.4f}", 
                va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'affordability_pressure_ranking.png'), dpi=300, bbox_inches='tight')
    print(f"✓ Saved: affordability_pressure_ranking.png")
    plt.close()
    
    # 3. Scatter plot with national average lines
    fig, ax = plt.subplots(figsize=(12, 8))
    
    scatter = ax.scatter(top_df['poverty_rate_percent'], 
                        top_df['electricity_rate_usd_per_kwh'],
                        s=300, c=top_df['affordability_pressure'], 
                        cmap='RdYlGn_r', alpha=0.7, edgecolors='black', linewidth=1.5)
    
    # Add national average lines
    ax.axhline(y=stats['national_avg_rate'], color='gray', linestyle='--', 
               linewidth=2, alpha=0.5, label=f"National Avg Rate (${stats['national_avg_rate']:.4f}/kWh)")
    ax.axvline(x=stats['national_avg_poverty'], color='gray', linestyle='--', 
               linewidth=2, alpha=0.5, label=f"National Avg Poverty ({stats['national_avg_poverty']:.1f}%)")
    
    # Label each point
    for _, row in top_df.iterrows():
        ax.annotate(f"{int(row['rank'])}", 
                   (row['poverty_rate_percent'], row['electricity_rate_usd_per_kwh']),
                   fontsize=10, fontweight='bold', ha='center', va='center')
    
    ax.set_xlabel('Poverty Rate (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Electricity Rate ($/kWh)', fontsize=12, fontweight='bold')
    ax.set_title('Affordability Crisis Quadrant\nTop 10 Utilities vs. National Averages', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Affordability Pressure', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'affordability_crisis_quadrant.png'), dpi=300, bbox_inches='tight')
    print(f"✓ Saved: affordability_crisis_quadrant.png")
    plt.close()


def generate_gtm_summary(top_df: pd.DataFrame, stats: Dict, output_dir: str, method: str):
    """Generate GTM-ready summary document."""
    log_section("Generating GTM Summary")
    
    summary_path = os.path.join(output_dir, 'GTM_SUMMARY.txt')
    
    with open(summary_path, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("UTILITY AFFORDABILITY INDEX - GTM SUMMARY\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("🎯 THE PROVOKE\n")
        f.write("-" * 70 + "\n")
        top1 = top_df.iloc[0]
        rate_vs_nat = (top1['electricity_rate_usd_per_kwh'] - stats['national_avg_rate']) / stats['national_avg_rate'] * 100
        f.write(f"\"{top1['utility_name']} customers are paying {rate_vs_nat:+.1f}% more than the \n")
        f.write(f"national average (${top1['electricity_rate_usd_per_kwh']:.4f}/kWh vs ${stats['national_avg_rate']:.4f}/kWh) \n")
        f.write(f"while {top1['poverty_rate_percent']:.1f}% live below the poverty line.\n")
        f.write("Let's talk about how to help them.\"\n\n")
        
        f.write("📊 KEY FINDINGS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total Utilities Analyzed: {stats['total_utilities']:,}\n")
        f.write(f"Methodology: {method}\n\n")
        
        f.write(f"National Benchmarks:\n")
        f.write(f"  • Average Rate: ${stats['national_avg_rate']:.4f}/kWh\n")
        f.write(f"  • Average Poverty: {stats['national_avg_poverty']:.2f}%\n\n")
        
        f.write(f"Top 10 High-Pressure Utilities:\n")
        f.write(f"  • Average Rate: ${stats['top_avg_rate']:.4f}/kWh ({stats['top_rate_premium_pct']:+.1f}% vs. national)\n")
        f.write(f"  • Average Poverty: {stats['top_avg_poverty']:.2f}% ({stats['top_poverty_premium_pct']:+.1f}% vs. national)\n\n")
        
        f.write("🏆 TOP 10 UTILITIES BY AFFORDABILITY PRESSURE\n")
        f.write("-" * 70 + "\n")
        for _, row in top_df.iterrows():
            f.write(f"\n{int(row['rank'])}. {row['utility_name']}\n")
            f.write(f"   State: {row['state']}\n")
            f.write(f"   Rate: ${row['electricity_rate_usd_per_kwh']:.4f}/kWh ")
            f.write(f"({(row['electricity_rate_usd_per_kwh']/stats['national_avg_rate']-1)*100:+.1f}% vs. national)\n")
            f.write(f"   Poverty: {row['poverty_rate_percent']:.2f}% ")
            f.write(f"({(row['poverty_rate_percent']/stats['national_avg_poverty']-1)*100:+.1f}% vs. national)\n")
            f.write(f"   Affordability Pressure: {row['affordability_pressure']:.6f}\n")
            
            if 'counties_covered' in row and pd.notna(row['counties_covered']):
                f.write(f"   Service Territory: {int(row['counties_covered'])} counties\n")
        
        f.write("\n" + "=" * 70 + "\n")
        f.write("💡 SALES CONVERSATION STARTERS\n")
        f.write("=" * 70 + "\n\n")
        
        for i, (_, row) in enumerate(top_df.head(3).iterrows(), 1):
            rate_prem = (row['electricity_rate_usd_per_kwh'] - stats['national_avg_rate']) / stats['national_avg_rate'] * 100
            f.write(f"{i}. {row['utility_name']} ({row['state']})\n")
            f.write(f"   \"Your residential customers face a perfect storm: rates {rate_prem:+.1f}% above\n")
            f.write(f"   the national average while {row['poverty_rate_percent']:.1f}% of your service territory\n")
            f.write(f"   lives below the poverty line. How are you identifying and supporting\n")
            f.write(f"   these vulnerable customers?\"\n\n")
        
        f.write("=" * 70 + "\n")
        f.write(f"Methodology: {method}\n")
        f.write("Data Sources: EIA-861 (2024), Census ACS 5-Year\n")
        f.write("Formula: Affordability Pressure = Electricity Rate × Poverty Rate\n")
    
    print(f"✓ Saved: GTM_SUMMARY.txt")


def save_detailed_output(df: pd.DataFrame, output_dir: str, method: str):
    """Save detailed CSV output."""
    log_section("Saving Detailed Output")
    
    # Format output columns
    output_cols = [
        'rank', 'utility_id', 'utility_name', 'state', 'state_full_name',
        'electricity_rate_usd_per_kwh', 'poverty_rate_percent', 'affordability_pressure',
        'residential_revenue_usd', 'residential_sales_mwh'
    ]
    
    # Add optional columns if present
    for col in ['counties_covered', 'service_territory_weight', 'weight_basis']:
        if col in df.columns:
            output_cols.append(col)
    
    output_df = df[[c for c in output_cols if c in df.columns]].copy()
    
    # Save CSV
    output_path = os.path.join(output_dir, 'utility_affordability_index_top10.csv')
    output_df.to_csv(output_path, index=False)
    print(f"✓ Saved: utility_affordability_index_top10.csv")
    
    # Save metadata
    metadata_path = os.path.join(output_dir, 'METADATA.txt')
    with open(metadata_path, 'w') as f:
        f.write("UTILITY AFFORDABILITY INDEX - METADATA\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Analysis Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Methodology: {method}\n")
        f.write(f"Data Year: 2024\n")
        f.write(f"Utilities Analyzed: {len(df)}\n\n")
        
        f.write("Data Sources:\n")
        f.write("  • EIA Form 861 - Sales to Ultimate Customers (2024)\n")
        if "county-weighted" in method.lower():
            f.write("  • EIA Form 861 - Schedule 9 Service Territory (2024)\n")
            f.write("  • Census ACS 5-Year - County Poverty Rates (S1701_C03_001E)\n")
        else:
            f.write("  • Census ACS 5-Year - State Poverty Rates (S1701_C03_001E)\n")
        
        f.write("\nFormula:\n")
        f.write("  Affordability Pressure = Electricity Rate × Poverty Rate\n")
        f.write("  Where:\n")
        f.write("    • Electricity Rate = Residential Revenue ÷ Residential Sales ($/kWh)\n")
        if "county-weighted" in method.lower():
            f.write("    • Poverty Rate = County-weighted average poverty rate\n")
        else:
            f.write("    • Poverty Rate = State-level poverty rate\n")
        
        f.write("\nColumn Descriptions:\n")
        f.write("  • rank: Ranking by affordability pressure (1 = highest)\n")
        f.write("  • utility_id: EIA utility identification number\n")
        f.write("  • utility_name: Official utility name\n")
        f.write("  • state: Two-letter state abbreviation\n")
        f.write("  • electricity_rate_usd_per_kwh: Residential electricity rate ($/kWh)\n")
        f.write("  • poverty_rate_percent: Poverty rate (% of population below poverty line)\n")
        f.write("  • affordability_pressure: Composite affordability metric\n")
        f.write("  • residential_revenue_usd: Total residential revenue ($)\n")
        f.write("  • residential_sales_mwh: Total residential electricity sales (MWh)\n")
        
        if 'counties_covered' in output_df.columns:
            f.write("  • counties_covered: Number of counties in service territory\n")
            f.write("  • service_territory_weight: Total weight across all counties\n")
            f.write("  • weight_basis: Format of territory weights (share_0_1, percent_0_100, or customer_count)\n")
    
    print(f"✓ Saved: METADATA.txt")


# -----------------------------
# Main Pipeline
# -----------------------------

def main():
    """Main execution pipeline."""
    args = parse_args()
    
    print("\n" + "=" * 70)
    print("UTILITY AFFORDABILITY INDEX CALCULATOR - GTM EDITION")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Sales File: {args.sales}")
    print(f"  Territory File: {args.territory}")
    print(f"  Output Directory: {args.output_dir}")
    print(f"  Top N: {args.top}")
    print(f"  County Weighting: {not args.no_county_weighting}")
    print(f"  Keep Outliers: {args.keep_outliers}")
    print(f"  Generate Visualizations: {not args.no_viz and VISUALIZATION_AVAILABLE}")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load EIA sales data
    eia_df = load_eia_sales_data(args.sales)
    eia_df = clean_sales_data(eia_df)
    eia_df = compute_rates(eia_df, keep_outliers=args.keep_outliers)
    
    # Determine poverty methodology
    use_county = not args.no_county_weighting
    method_used = "State-level poverty"
    
    if use_county:
        territory_df = load_service_territory_data(args.territory)
        county_poverty_df = fetch_county_poverty()
        
        if territory_df is not None and county_poverty_df is not None:
            # County-weighted approach
            poverty_df = compute_weighted_poverty(territory_df, county_poverty_df)
            
            if len(poverty_df) > 0:
                eia_df = eia_df.merge(poverty_df, on='utility_number', how='inner')
                method_used = "County-weighted poverty"
            else:
                # Fallback to state
                state_poverty_df = fetch_state_poverty()
                eia_df = join_state_poverty(eia_df, state_poverty_df)
        else:
            # Fallback to state
            state_poverty_df = fetch_state_poverty()
            eia_df = join_state_poverty(eia_df, state_poverty_df)
    else:
        # Direct state-level approach
        state_poverty_df = fetch_state_poverty()
        eia_df = join_state_poverty(eia_df, state_poverty_df)
    
    # Compute affordability pressure
    eia_df = compute_affordability_pressure(eia_df)
    
    # Rank and select top N
    eia_df = eia_df.sort_values('affordability_pressure', ascending=False).copy()
    top_df = eia_df.head(args.top).copy()
    top_df['rank'] = range(1, len(top_df) + 1)
    
    # Prepare formatted output
    top_df['residential_revenue_usd'] = top_df['residential_revenue_thousand_usd'] * 1000
    top_df['utility_id'] = top_df['utility_number'].astype(str)
    
    # Calculate summary statistics
    stats = calculate_summary_statistics(eia_df, top_df)
    
    # Generate outputs
    save_detailed_output(top_df, args.output_dir, method_used)
    generate_gtm_summary(top_df, stats, args.output_dir, method_used)
    
    if not args.no_viz and VISUALIZATION_AVAILABLE:
        create_visualizations(top_df, stats, args.output_dir)
    
    # Final summary
    log_section("COMPLETE")
    print(f"✓ Analysis complete!")
    print(f"✓ Output directory: {args.output_dir}")
    print(f"✓ Methodology: {method_used}")
    print(f"✓ Top {len(top_df)} utilities identified")
    print(f"\n📁 Files created:")
    print(f"  • utility_affordability_index_top10.csv")
    print(f"  • GTM_SUMMARY.txt")
    print(f"  • METADATA.txt")
    if not args.no_viz and VISUALIZATION_AVAILABLE:
        print(f"  • top10_rate_poverty_comparison.png")
        print(f"  • affordability_pressure_ranking.png")
        print(f"  • affordability_crisis_quadrant.png")
    
    print(f"\n🎯 Your #1 target: {top_df.iloc[0]['utility_name']} ({top_df.iloc[0]['state']})")
    print(f"   Rate: ${top_df.iloc[0]['electricity_rate_usd_per_kwh']:.4f}/kWh")
    print(f"   Poverty: {top_df.iloc[0]['poverty_rate_percent']:.2f}%")
    print(f"   Pressure: {top_df.iloc[0]['affordability_pressure']:.6f}")
    

if __name__ == "__main__":
    main()