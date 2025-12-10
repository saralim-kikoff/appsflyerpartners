"""
AppsFlyer Monthly Attribution Report Generator

Pulls in-app events and Protect360 fraud data, applies flagging rules,
aggregates by agency, and outputs an Excel report + Slack notification.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import json


# =============================================================================
# CONFIGURATION
# =============================================================================

APPSFLYER_API_TOKEN = os.environ.get("APPSFLYER_API_TOKEN")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

APP_IDS = {
    "ios": "id1525159784",
    "android": "com.kikoff"
}

EVENT_NAME = "CA Payment - Make Credit Line Success"

# Agencies authorized for view-through attribution (VTA)
VTA_AUTHORIZED_AGENCIES = ["adperiomedia", "globalwidemedia"]

# Attribution window limits
VTA_WINDOW_HOURS = 6
CTA_WINDOW_DAYS = 7

BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"


# =============================================================================
# DATE HELPERS
# =============================================================================

def get_previous_month_range():
    """Get the first and last day of the previous month."""
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    
    return (
        first_of_prev_month.strftime("%Y-%m-%d"),
        last_of_prev_month.strftime("%Y-%m-%d")
    )


def get_report_month_name():
    """Get the name of the previous month for report titles."""
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    return last_of_prev_month.strftime("%B %Y")


# =============================================================================
# LOOKBACK PARSING
# =============================================================================

def parse_lookback_to_hours(value):
    """
    Convert AppsFlyer lookback format to hours.
    
    Examples:
        '7d' -> 168
        '6h' -> 6
        '24h' -> 24
        '30d' -> 720
    """
    if pd.isna(value) or value == '' or value is None:
        return 0
    
    value = str(value).strip().lower()
    
    try:
        if value.endswith('d'):
            return float(value[:-1]) * 24
        elif value.endswith('h'):
            return float(value[:-1])
        else:
            # Try to parse as numeric (assume hours)
            return float(value)
    except ValueError:
        return 0


# =============================================================================
# APPSFLYER API
# =============================================================================

def pull_appsflyer_report(app_id, report_type, from_date, to_date):
    """
    Pull raw data report from AppsFlyer Pull API.
    
    Args:
        app_id: iOS or Android app ID
        report_type: 'in_app_events' or 'protect360_in_app_events'
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
    
    Returns:
        pandas DataFrame with report data
    """
    # Map report type to endpoint
    endpoint_map = {
        "in_app_events": "in_app_events_report/v5",
        "protect360_in_app_events": "protect360_fraud/in_app_events_report/v5"
    }
    
    endpoint = endpoint_map.get(report_type)
    if not endpoint:
        raise ValueError(f"Unknown report type: {report_type}")
    
    url = f"{BASE_URL}/{app_id}/{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {APPSFLYER_API_TOKEN}",
        "Accept": "text/csv"
    }
    
    params = {
        "from": from_date,
        "to": to_date,
        "event_name": EVENT_NAME,
        "media_source": "!organic"  # Non-organic only
    }
    
    print(f"Pulling {report_type} for {app_id}...")
    print(f"  URL: {url}")
    print(f"  Date range: {from_date} to {to_date}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"  Error: {response.status_code}")
        print(f"  Response: {response.text[:500]}")
        return pd.DataFrame()
    
    if not response.text.strip():
        print("  No data returned")
        return pd.DataFrame()
    
    df = pd.read_csv(StringIO(response.text))
    print(f"  Pulled {len(df)} rows")
    
    return df


def pull_all_reports(from_date, to_date):
    """
    Pull all 4 reports (in-app events + P360 for iOS + Android).
    
    Returns:
        tuple: (delivered_df, fraud_df)
    """
    delivered_dfs = []
    fraud_dfs = []
    
    for platform, app_id in APP_IDS.items():
        # In-app events (delivered)
        df = pull_appsflyer_report(app_id, "in_app_events", from_date, to_date)
        if not df.empty:
            df["platform"] = platform
            delivered_dfs.append(df)
        
        # Protect360 fraud events
        df = pull_appsflyer_report(app_id, "protect360_in_app_events", from_date, to_date)
        if not df.empty:
            df["platform"] = platform
            fraud_dfs.append(df)
    
    delivered_df = pd.concat(delivered_dfs, ignore_index=True) if delivered_dfs else pd.DataFrame()
    fraud_df = pd.concat(fraud_dfs, ignore_index=True) if fraud_dfs else pd.DataFrame()
    
    return delivered_df, fraud_df


# =============================================================================
# FLAGGING LOGIC
# =============================================================================

def apply_flagging_rules(df):
    """
    Apply custom flagging rules to identify suspicious events.
    
    Rules:
    1. Unauthorized VTA: impression attribution from non-authorized agencies
    2. VTA Window Exceeded: impression from authorized agencies > 6h lookback
    3. CTA Window Exceeded: click attribution > 7 days lookback
    
    Returns:
        DataFrame with 'is_flagged' and 'flag_reason' columns added
    """
    if df.empty:
        df["is_flagged"] = False
        df["flag_reason"] = ""
        return df
    
    # Normalize column names (AppsFlyer uses various formats)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Identify the agency column (could be 'agency', 'partner', 'media_source', etc.)
    agency_col = None
    for col in ['agency', 'partner', 'af_prt', 'media_source']:
        if col in df.columns:
            agency_col = col
            break
    
    if agency_col is None:
        print("Warning: Could not find agency column. Using 'unknown'.")
        df["agency_normalized"] = "unknown"
    else:
        df["agency_normalized"] = df[agency_col].fillna("unknown").str.strip().str.lower()
    
    # Identify touch type column
    touch_type_col = None
    for col in ['attributed_touch_type', 'touch_type']:
        if col in df.columns:
            touch_type_col = col
            break
    
    if touch_type_col is None:
        print("Warning: Could not find touch type column.")
        df["touch_type_normalized"] = "unknown"
    else:
        df["touch_type_normalized"] = df[touch_type_col].fillna("").str.strip().str.lower()
    
    # Identify lookback column
    lookback_col = None
    for col in ['attribution_lookback', 'lookback', 'time_to_install']:
        if col in df.columns:
            lookback_col = col
            break
    
    if lookback_col:
        df["lookback_hours"] = df[lookback_col].apply(parse_lookback_to_hours)
    else:
        print("Warning: Could not find lookback column. Setting to 0.")
        df["lookback_hours"] = 0
    
    # Apply flagging rules
    df["is_flagged"] = False
    df["flag_reason"] = ""
    
    # Rule 1: Unauthorized VTA
    mask_unauthorized_vta = (
        (df["touch_type_normalized"] == "impression") &
        (~df["agency_normalized"].isin(VTA_AUTHORIZED_AGENCIES))
    )
    df.loc[mask_unauthorized_vta, "is_flagged"] = True
    df.loc[mask_unauthorized_vta, "flag_reason"] = "Unauthorized VTA"
    
    # Rule 2: VTA Window Exceeded (for authorized agencies)
    mask_vta_window = (
        (df["touch_type_normalized"] == "impression") &
        (df["agency_normalized"].isin(VTA_AUTHORIZED_AGENCIES)) &
        (df["lookback_hours"] > VTA_WINDOW_HOURS)
    )
    df.loc[mask_vta_window, "is_flagged"] = True
    df.loc[mask_vta_window, "flag_reason"] = "VTA Window Exceeded (>6h)"
    
    # Rule 3: CTA Window Exceeded
    mask_cta_window = (
        (df["touch_type_normalized"] == "click") &
        (df["lookback_hours"] > CTA_WINDOW_DAYS * 24)
    )
    df.loc[mask_cta_window, "is_flagged"] = True
    df.loc[mask_cta_window, "flag_reason"] = "CTA Window Exceeded (>7d)"
    
    flagged_count = df["is_flagged"].sum()
    print(f"Flagged {flagged_count} events out of {len(df)}")
    
    return df


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_by_agency(df, value_col_name="event_count"):
    """
    Aggregate event counts by agency.
    
    Returns:
        DataFrame with agency and event count
    """
    if df.empty:
        return pd.DataFrame(columns=["agency", value_col_name])
    
    # Ensure agency column exists
    if "agency_normalized" not in df.columns:
        # Try to find and normalize agency column
        agency_col = None
        for col in ['agency', 'partner', 'af_prt', 'media_source']:
            if col in df.columns:
                agency_col = col
                break
        
        if agency_col:
            df["agency_normalized"] = df[agency_col].fillna("unknown").str.strip().str.lower()
        else:
            df["agency_normalized"] = "unknown"
    
    aggregated = df.groupby("agency_normalized").size().reset_index(name=value_col_name)
    aggregated = aggregated.rename(columns={"agency_normalized": "agency"})
    aggregated = aggregated.sort_values(value_col_name, ascending=False)
    
    return aggregated


# =============================================================================
# EXCEL GENERATION
# =============================================================================

def style_header(ws, row_num, num_cols):
    """Apply header styling to a row."""
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")


def add_dataframe_to_sheet(ws, df, start_row=1):
    """Add a DataFrame to a worksheet with formatting."""
    if df.empty:
        ws.cell(row=start_row, column=1, value="No data")
        return
    
    # Write headers
    for col_idx, col_name in enumerate(df.columns, 1):
        ws.cell(row=start_row, column=col_idx, value=col_name.replace("_", " ").title())
    
    style_header(ws, start_row, len(df.columns))
    
    # Write data
    for row_idx, row in enumerate(df.itertuples(index=False), start_row + 1):
        for col_idx, value in enumerate(row, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(horizontal="center")
    
    # Auto-adjust column widths
    for col_idx, col_name in enumerate(df.columns, 1):
        max_length = max(
            len(str(col_name)),
            df[col_name].astype(str).str.len().max() if len(df) > 0 else 0
        )
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = max_length + 2


def generate_excel_report(summary_df, delivered_df, fraud_df, flagged_df, report_month):
    """
    Generate Excel workbook with multiple tabs.
    
    Tabs:
    1. Summary - Agency rollup with Net Valid calculation
    2. Delivered Events - All events aggregated by agency
    3. Fraud Events - P360 events aggregated by agency
    4. Flagged Events - Rule violations aggregated by agency
    """
    wb = Workbook()
    
    # Remove default sheet
    wb.remove(wb.active)
    
    # Tab 1: Summary
    ws_summary = wb.create_sheet("Summary")
    ws_summary.cell(row=1, column=1, value=f"Attribution Report - {report_month}")
    ws_summary.cell(row=1, column=1).font = Font(bold=True, size=14)
    ws_summary.merge_cells(start_row=1, start_column=1, end_row=1, end_column=6)
    add_dataframe_to_sheet(ws_summary, summary_df, start_row=3)
    
    # Tab 2: Delivered Events
    ws_delivered = wb.create_sheet("Delivered Events")
    add_dataframe_to_sheet(ws_delivered, delivered_df)
    
    # Tab 3: Fraud Events
    ws_fraud = wb.create_sheet("Fraud Events")
    add_dataframe_to_sheet(ws_fraud, fraud_df)
    
    # Tab 4: Flagged Events
    ws_flagged = wb.create_sheet("Flagged Events")
    add_dataframe_to_sheet(ws_flagged, flagged_df)
    
    # Save workbook
    filename = f"attribution_report_{report_month.replace(' ', '_').lower()}.xlsx"
    filepath = f"/tmp/{filename}"
    wb.save(filepath)
    
    print(f"Excel report saved: {filepath}")
    return filepath


# =============================================================================
# SLACK NOTIFICATION
# =============================================================================

def send_slack_notification(summary_df, report_month, excel_filepath):
    """
    Send Slack message with report summary.
    
    Note: Slack webhooks don't support file uploads directly.
    For file uploads, you'd need the Slack API with files.upload.
    This sends a formatted summary message.
    """
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook URL not configured. Skipping notification.")
        return
    
    # Build summary table
    total_delivered = summary_df["delivered"].sum() if "delivered" in summary_df.columns else 0
    total_fraud = summary_df["fraud"].sum() if "fraud" in summary_df.columns else 0
    total_flagged = summary_df["flagged"].sum() if "flagged" in summary_df.columns else 0
    total_net_valid = summary_df["net_valid"].sum() if "net_valid" in summary_df.columns else 0
    
    overall_fraud_rate = (total_fraud / total_delivered * 100) if total_delivered > 0 else 0
    overall_flag_rate = (total_flagged / total_delivered * 100) if total_delivered > 0 else 0
    
    # Top agencies by net valid
    top_agencies = summary_df.head(5).to_dict('records') if not summary_df.empty else []
    
    agency_lines = []
    for agency in top_agencies:
        name = agency.get("agency", "Unknown")
        delivered = agency.get("delivered", 0)
        net_valid = agency.get("net_valid", 0)
        fraud_rate = agency.get("fraud_rate_%", 0)
        agency_lines.append(f"• *{name}*: {net_valid:,} net valid / {delivered:,} delivered ({fraud_rate:.1f}% fraud)")
    
    agency_summary = "\n".join(agency_lines) if agency_lines else "No data"
    
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 Monthly Attribution Report — {report_month}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Overall Totals*\n"
                            f"• Delivered Events: *{total_delivered:,}*\n"
                            f"• Fraud Events (P360): *{total_fraud:,}* ({overall_fraud_rate:.1f}%)\n"
                            f"• Flagged Events: *{total_flagged:,}* ({overall_flag_rate:.1f}%)\n"
                            f"• Net Valid Events: *{total_net_valid:,}*"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Top Agencies by Net Valid*\n{agency_summary}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Event: `{EVENT_NAME}` | Full Excel report generated"
                    }
                ]
            }
        ]
    }
    
    response = requests.post(
        SLACK_WEBHOOK_URL,
        json=message,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        print("Slack notification sent successfully")
    else:
        print(f"Slack notification failed: {response.status_code} - {response.text}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution flow."""
    print("=" * 60)
    print("AppsFlyer Monthly Attribution Report")
    print("=" * 60)
    
    # Get date range for previous month
    from_date, to_date = get_previous_month_range()
    report_month = get_report_month_name()
    
    print(f"\nReport Period: {report_month}")
    print(f"Date Range: {from_date} to {to_date}")
    
    # Pull all reports
    print("\n" + "-" * 40)
    print("Pulling AppsFlyer Data")
    print("-" * 40)
    
    delivered_df, fraud_df = pull_all_reports(from_date, to_date)
    
    print(f"\nTotal delivered events: {len(delivered_df)}")
    print(f"Total fraud events: {len(fraud_df)}")
    
    # Apply flagging rules to delivered events
    print("\n" + "-" * 40)
    print("Applying Flagging Rules")
    print("-" * 40)
    
    delivered_df = apply_flagging_rules(delivered_df)
    
    # Separate flagged events
    flagged_events_df = delivered_df[delivered_df["is_flagged"] == True].copy()
    
    print(f"Flagged events: {len(flagged_events_df)}")
    
    # Aggregate by agency
    print("\n" + "-" * 40)
    print("Aggregating by Agency")
    print("-" * 40)
    
    delivered_agg = aggregate_by_agency(delivered_df, "delivered")
    fraud_agg = aggregate_by_agency(fraud_df, "fraud")
    flagged_agg = aggregate_by_agency(flagged_events_df, "flagged")
    
    # Add flag reason breakdown to flagged aggregation
    if not flagged_events_df.empty:
        flagged_by_reason = flagged_events_df.groupby(
            ["agency_normalized", "flag_reason"]
        ).size().reset_index(name="count")
        flagged_by_reason = flagged_by_reason.rename(columns={"agency_normalized": "agency"})
        flagged_agg = flagged_agg.merge(
            flagged_by_reason.pivot(index="agency", columns="flag_reason", values="count").reset_index(),
            on="agency",
            how="left"
        ).fillna(0)
    
    # Build summary
    summary_df = delivered_agg.merge(fraud_agg, on="agency", how="left")
    summary_df = summary_df.merge(flagged_agg[["agency", "flagged"]], on="agency", how="left")
    summary_df = summary_df.fillna(0)
    
    # Calculate net valid
    summary_df["net_valid"] = (
        summary_df["delivered"] - summary_df["fraud"] - summary_df["flagged"]
    ).astype(int)
    
    # Calculate rates
    summary_df["fraud_rate_%"] = (
        summary_df["fraud"] / summary_df["delivered"] * 100
    ).round(1)
    summary_df["flag_rate_%"] = (
        summary_df["flagged"] / summary_df["delivered"] * 100
    ).round(1)
    
    # Convert to int for cleaner display
    for col in ["delivered", "fraud", "flagged", "net_valid"]:
        summary_df[col] = summary_df[col].astype(int)
    
    # Sort by delivered descending
    summary_df = summary_df.sort_values("delivered", ascending=False)
    
    print("\nSummary:")
    print(summary_df.to_string(index=False))
    
    # Generate Excel report
    print("\n" + "-" * 40)
    print("Generating Excel Report")
    print("-" * 40)
    
    excel_filepath = generate_excel_report(
        summary_df,
        delivered_agg,
        fraud_agg,
        flagged_agg,
        report_month
    )
    
    # Send Slack notification
    print("\n" + "-" * 40)
    print("Sending Slack Notification")
    print("-" * 40)
    
    send_slack_notification(summary_df, report_month, excel_filepath)
    
    print("\n" + "=" * 60)
    print("Report Complete!")
    print("=" * 60)
    
    return excel_filepath


if __name__ == "__main__":
    main()
