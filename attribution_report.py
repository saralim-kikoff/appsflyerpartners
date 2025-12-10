"""
AppsFlyer Monthly Attribution Report Generator

Pulls in-app events and Protect360 fraud data, applies flagging rules,
aggregates by agency, and outputs an Excel report + Slack notification.

Supports multiple apps:
- Kikoff: Credit line payments with Outside Attribution flagging
- Grant Cash Advance: First time offers with Add'l Fraud flagging

Slack modes:
- If SLACK_BOT_TOKEN + SLACK_CHANNEL_ID are set: Sends files and message to the channel
- If only SLACK_WEBHOOK_URL is set: Sends message to webhook (test mode, no file upload)
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


# =============================================================================
# CONFIGURATION
# =============================================================================

APPSFLYER_API_TOKEN = os.environ.get("APPSFLYER_API_TOKEN")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")

# -----------------------------------------------------------------------------
# KIKOFF APP CONFIGURATION
# -----------------------------------------------------------------------------
KIKOFF_APP_IDS = {
    "ios": "id1525159784",
    "android": "com.kikoff"
}
KIKOFF_EVENT_NAME = "CA Payment - Make Credit Line Success"

VTA_AUTHORIZED_AGENCIES = ["adperiomedia", "globalwidemedia"]
VTA_WINDOW_HOURS = 6
CTA_WINDOW_DAYS = 7

# -----------------------------------------------------------------------------
# GRANT CASH ADVANCE APP CONFIGURATION
# -----------------------------------------------------------------------------
GRANT_APP_IDS = {
    "ios": "id6472350114",
    "android": "com.kikoff.theseus"
}
GRANT_EVENT_NAME = "First Time Offer Accepted"
GRANT_EXCLUDED_AGENCIES = ["mobiprobebd521", "unknown", ""]

BASE_URL = "https://hq1.appsflyer.com/api/raw-data/export/app"


# =============================================================================
# DATE HELPERS
# =============================================================================

def get_previous_month_range():
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    return (first_of_prev_month.strftime("%Y-%m-%d"), last_of_prev_month.strftime("%Y-%m-%d"))


def get_report_month_name():
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    return last_of_prev_month.strftime("%B %Y")


def get_report_month_yyyymm():
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    return last_of_prev_month.strftime("%Y%m")


# =============================================================================
# LOOKBACK PARSING
# =============================================================================

def parse_lookback_to_hours(value):
    if pd.isna(value) or value == '' or value is None:
        return 0
    value = str(value).strip().lower()
    try:
        if value.endswith('d'):
            return float(value[:-1]) * 24
        elif value.endswith('h'):
            return float(value[:-1])
        else:
            return float(value)
    except ValueError:
        return 0


# =============================================================================
# APPSFLYER API
# =============================================================================

def pull_appsflyer_report(app_id, report_type, from_date, to_date, event_name):
    endpoint_map = {
        "in_app_events": "in_app_events_report/v5",
        "protect360_in_app_events": "fraud-post-inapps/v5"
    }
    
    endpoint = endpoint_map.get(report_type)
    if not endpoint:
        raise ValueError(f"Unknown report type: {report_type}")
    
    url = f"{BASE_URL}/{app_id}/{endpoint}"
    headers = {"Authorization": f"Bearer {APPSFLYER_API_TOKEN}", "Accept": "text/csv"}
    params = {"from": from_date, "to": to_date, "event_name": event_name}
    
    print(f"Pulling {report_type} for {app_id}...")
    print(f"  URL: {url}")
    print(f"  Date range: {from_date} to {to_date}")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"  Error: {response.status_code}")
        print(f"  Response: {response.text[:500] if response.text else 'Empty'}")
        return pd.DataFrame()
    
    if not response.text.strip():
        print("  No data returned")
        return pd.DataFrame()
    
    df = pd.read_csv(StringIO(response.text), low_memory=False)
    print(f"  Pulled {len(df)} rows")
    return df


def pull_all_reports(from_date, to_date, app_ids, event_name):
    delivered_dfs = []
    fraud_dfs = []
    
    for platform, app_id in app_ids.items():
        df = pull_appsflyer_report(app_id, "in_app_events", from_date, to_date, event_name)
        if not df.empty:
            df["platform"] = platform
            delivered_dfs.append(df)
        
        df = pull_appsflyer_report(app_id, "protect360_in_app_events", from_date, to_date, event_name)
        if not df.empty:
            df["platform"] = platform
            fraud_dfs.append(df)
    
    delivered_df = pd.concat(delivered_dfs, ignore_index=True) if delivered_dfs else pd.DataFrame()
    fraud_df = pd.concat(fraud_dfs, ignore_index=True) if fraud_dfs else pd.DataFrame()
    
    if not delivered_df.empty:
        delivered_df.columns = delivered_df.columns.str.strip().str.lower().str.replace(' ', '_')
        if 'media_source' in delivered_df.columns:
            delivered_df = delivered_df[delivered_df['media_source'].str.lower() != 'organic']
            print(f"After filtering organic: {len(delivered_df)} delivered events")
    
    if not fraud_df.empty:
        fraud_df.columns = fraud_df.columns.str.strip().str.lower().str.replace(' ', '_')
        if 'media_source' in fraud_df.columns:
            fraud_df = fraud_df[fraud_df['media_source'].str.lower() != 'organic']
            print(f"After filtering organic: {len(fraud_df)} fraud events")
    
    return delivered_df, fraud_df


# =============================================================================
# FLAGGING LOGIC
# =============================================================================

def apply_kikoff_flagging_rules(df):
    if df.empty:
        df["is_flagged"] = False
        df["flag_reason"] = ""
        return df
    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    agency_col = next((col for col in ['agency', 'partner', 'af_prt', 'media_source'] if col in df.columns), None)
    df["agency_normalized"] = df[agency_col].fillna("unknown").str.strip().str.lower() if agency_col else "unknown"
    
    touch_type_col = next((col for col in ['attributed_touch_type', 'touch_type'] if col in df.columns), None)
    df["touch_type_normalized"] = df[touch_type_col].fillna("").str.strip().str.lower() if touch_type_col else "unknown"
    
    lookback_col = next((col for col in ['attribution_lookback', 'lookback', 'time_to_install'] if col in df.columns), None)
    df["lookback_hours"] = df[lookback_col].apply(parse_lookback_to_hours) if lookback_col else 0
    
    df["is_flagged"] = False
    df["flag_reason"] = ""
    
    mask_unauthorized_vta = (df["touch_type_normalized"] == "impression") & (~df["agency_normalized"].isin(VTA_AUTHORIZED_AGENCIES))
    df.loc[mask_unauthorized_vta, "is_flagged"] = True
    df.loc[mask_unauthorized_vta, "flag_reason"] = "Unauthorized VTA"
    
    mask_vta_window = (df["touch_type_normalized"] == "impression") & (df["agency_normalized"].isin(VTA_AUTHORIZED_AGENCIES)) & (df["lookback_hours"] > VTA_WINDOW_HOURS)
    df.loc[mask_vta_window, "is_flagged"] = True
    df.loc[mask_vta_window, "flag_reason"] = "VTA Window Exceeded (>6h)"
    
    mask_cta_window = (df["touch_type_normalized"] == "click") & (df["lookback_hours"] > CTA_WINDOW_DAYS * 24)
    df.loc[mask_cta_window, "is_flagged"] = True
    df.loc[mask_cta_window, "flag_reason"] = "CTA Window Exceeded (>7d)"
    
    print(f"Flagged {df['is_flagged'].sum()} events out of {len(df)}")
    return df


def apply_grant_addl_fraud_rules(df, fraud_df):
    if df.empty:
        return pd.DataFrame()
    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    event_value_col = next((col for col in ['event_value', 'event_revenue', 'revenue'] if col in df.columns), None)
    if event_value_col is None:
        print("Warning: Could not find event_value column for Grant Add'l Fraud check.")
        return pd.DataFrame()
    
    df["_event_value_str"] = df[event_value_col].fillna("").astype(str)
    addl_fraud_df = df[~df["_event_value_str"].str.contains("00}", regex=False)].copy()
    print(f"Events with event_value not containing '00}}': {len(addl_fraud_df)}")
    
    if addl_fraud_df.empty:
        return pd.DataFrame()
    
    if not fraud_df.empty:
        fraud_df.columns = fraud_df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        customer_id_col = next((col for col in addl_fraud_df.columns if 'customer' in col and 'id' in col), None)
        appsflyer_id_col = next((col for col in addl_fraud_df.columns if 'appsflyer' in col and 'id' in col), None)
        
        if customer_id_col and appsflyer_id_col:
            addl_fraud_df["_match_key"] = addl_fraud_df[customer_id_col].astype(str) + "_" + addl_fraud_df[appsflyer_id_col].astype(str)
            fraud_df["_match_key"] = fraud_df[customer_id_col].astype(str) + "_" + fraud_df[appsflyer_id_col].astype(str)
            
            fraud_keys = set(fraud_df["_match_key"].unique())
            addl_fraud_df = addl_fraud_df[~addl_fraud_df["_match_key"].isin(fraud_keys)]
            addl_fraud_df = addl_fraud_df.drop(columns=["_match_key", "_event_value_str"], errors='ignore')
            print(f"Add'l Fraud events (after removing P360 matches): {len(addl_fraud_df)}")
        else:
            addl_fraud_df = addl_fraud_df.drop(columns=["_event_value_str"], errors='ignore')
    else:
        addl_fraud_df = addl_fraud_df.drop(columns=["_event_value_str"], errors='ignore')
    
    agency_col = next((col for col in ['agency', 'partner', 'af_prt', 'media_source'] if col in addl_fraud_df.columns), None)
    addl_fraud_df["agency_normalized"] = addl_fraud_df[agency_col].fillna("unknown").str.strip().str.lower() if agency_col else "unknown"
    
    return addl_fraud_df


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_by_agency(df, value_col_name="event_count"):
    if df.empty:
        return pd.DataFrame(columns=["agency", value_col_name])
    
    if "agency_normalized" not in df.columns:
        agency_col = next((col for col in ['agency', 'partner', 'af_prt', 'media_source'] if col in df.columns), None)
        df["agency_normalized"] = df[agency_col].fillna("unknown").str.strip().str.lower() if agency_col else "unknown"
    
    df_filtered = df[df["agency_normalized"] != "unknown"]
    if df_filtered.empty:
        return pd.DataFrame(columns=["agency", value_col_name])
    
    aggregated = df_filtered.groupby("agency_normalized").size().reset_index(name=value_col_name)
    aggregated = aggregated.rename(columns={"agency_normalized": "agency"})
    return aggregated.sort_values(value_col_name, ascending=False)


# =============================================================================
# EXCEL GENERATION
# =============================================================================

def style_header(ws, row_num, num_cols):
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")


def add_dataframe_to_sheet(ws, df, start_row=1):
    if df.empty:
        ws.cell(row=start_row, column=1, value="No data")
        return
    
    for col_idx, col_name in enumerate(df.columns, 1):
        ws.cell(row=start_row, column=col_idx, value=str(col_name).replace("_", " ").title())
    style_header(ws, start_row, len(df.columns))
    
    for row_idx, row in enumerate(df.itertuples(index=False), start_row + 1):
        for col_idx, value in enumerate(row, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(horizontal="center")
    
    for col_idx, col_name in enumerate(df.columns, 1):
        try:
            header_len = len(str(col_name))
            data_len = df.iloc[:, col_idx - 1].astype(str).str.len().max() if len(df) > 0 else 0
            ws.column_dimensions[get_column_letter(col_idx)].width = min(max(header_len, data_len) + 2, 50)
        except Exception:
            ws.column_dimensions[get_column_letter(col_idx)].width = 15


def generate_kikoff_excel_report(kikoff_data, report_month):
    wb = Workbook()
    wb.remove(wb.active)
    
    kikoff_summary = kikoff_data["summary"].copy() if not kikoff_data["summary"].empty else pd.DataFrame()
    if not kikoff_summary.empty:
        desired_order = ["agency", "delivered", "fraud", "fraud_rate_%", "outside_attribution", "outside_attr_rate_%", "net_valid"]
        final_order = [col for col in desired_order if col in kikoff_summary.columns]
        final_order.extend([col for col in kikoff_summary.columns if col not in final_order])
        kikoff_summary = kikoff_summary[final_order]
    
    ws = wb.create_sheet("Summary")
    ws.cell(row=1, column=1, value=f"Kikoff Attribution Report - {report_month}")
    ws.cell(row=1, column=1).font = Font(bold=True, size=14)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=7)
    add_dataframe_to_sheet(ws, kikoff_summary, start_row=3)
    
    ws = wb.create_sheet("Delivered Events")
    add_dataframe_to_sheet(ws, kikoff_data["delivered"])
    
    ws = wb.create_sheet("Fraud Events")
    add_dataframe_to_sheet(ws, kikoff_data["fraud"])
    
    ws = wb.create_sheet("Outside Attribution Events")
    add_dataframe_to_sheet(ws, kikoff_data["flagged"])
    
    filepath = f"/tmp/Appsflyer_Kikoff_{get_report_month_yyyymm()}.xlsx"
    wb.save(filepath)
    print(f"Kikoff Excel report saved: {filepath}")
    return filepath


def generate_grant_excel_report(grant_data, report_month):
    wb = Workbook()
    wb.remove(wb.active)
    
    grant_summary = grant_data["summary"].copy() if not grant_data["summary"].empty else pd.DataFrame()
    if not grant_summary.empty:
        desired_order = ["agency", "delivered", "fraud", "fraud_rate_%", "addl_fraud", "addl_fraud_rate_%", "net_valid"]
        final_order = [col for col in desired_order if col in grant_summary.columns]
        final_order.extend([col for col in grant_summary.columns if col not in final_order])
        grant_summary = grant_summary[final_order]
    
    ws = wb.create_sheet("Summary")
    ws.cell(row=1, column=1, value=f"Grant Cash Advance Attribution Report - {report_month}")
    ws.cell(row=1, column=1).font = Font(bold=True, size=14)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=7)
    add_dataframe_to_sheet(ws, grant_summary, start_row=3)
    
    ws = wb.create_sheet("Delivered Events")
    add_dataframe_to_sheet(ws, grant_data["delivered"])
    
    ws = wb.create_sheet("Fraud Events")
    add_dataframe_to_sheet(ws, grant_data["fraud"])
    
    ws = wb.create_sheet("Add'l Fraud Events")
    add_dataframe_to_sheet(ws, grant_data["flagged"])
    
    filepath = f"/tmp/Appsflyer_Grant_{get_report_month_yyyymm()}.xlsx"
    wb.save(filepath)
    print(f"Grant Excel report saved: {filepath}")
    return filepath


# =============================================================================
# SLACK NOTIFICATION
# =============================================================================

def upload_file_to_slack(filepath, channel_id):
    if not SLACK_BOT_TOKEN or not channel_id:
        return None
    
    url = "https://slack.com/api/files.upload"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    
    with open(filepath, "rb") as f:
        response = requests.post(
            url, headers=headers,
            data={"channels": channel_id, "filename": os.path.basename(filepath), "title": os.path.basename(filepath)},
            files={"file": f}
        )
    
    if response.status_code == 200:
        result = response.json()
        if result.get("ok"):
            permalink = result.get("file", {}).get("permalink", "")
            print(f"File uploaded to Slack successfully: {permalink}")
            return permalink
        else:
            print(f"Slack file upload failed: {result.get('error')}")
    else:
        print(f"Slack file upload failed: {response.status_code}")
    return None


def send_slack_message_to_channel(blocks):
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        return False
    
    url = "https://slack.com/api/chat.postMessage"
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"}
    payload = {"channel": SLACK_CHANNEL_ID, "blocks": blocks}
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200 and response.json().get("ok"):
        print("Slack message sent to channel successfully")
        return True
    else:
        print(f"Slack channel message failed: {response.json().get('error', response.status_code)}")
        return False


def send_slack_message_to_webhook(blocks):
    if not SLACK_WEBHOOK_URL:
        return False
    
    response = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, headers={"Content-Type": "application/json"})
    
    if response.status_code == 200:
        print("Slack message sent to webhook successfully")
        return True
    else:
        print(f"Slack webhook message failed: {response.status_code} - {response.text}")
        return False


def send_combined_slack_notification(kikoff_data, grant_data, report_month, kikoff_filepath, grant_filepath):
    """
    Send Slack message with combined report summary.
    
    Priority:
    1. SLACK_BOT_TOKEN + SLACK_CHANNEL_ID → Send to channel with file uploads
    2. Only SLACK_WEBHOOK_URL → Send to webhook (test mode, no files)
    """
    use_channel = bool(SLACK_BOT_TOKEN and SLACK_CHANNEL_ID)
    use_webhook = bool(SLACK_WEBHOOK_URL) and not use_channel
    
    if not use_channel and not use_webhook:
        print("No Slack credentials configured. Skipping notification.")
        return
    
    kikoff_permalink = None
    grant_permalink = None
    
    if use_channel:
        print("Using Slack Bot Token - sending to channel...")
        if kikoff_filepath:
            kikoff_permalink = upload_file_to_slack(kikoff_filepath, SLACK_CHANNEL_ID)
        if grant_filepath:
            grant_permalink = upload_file_to_slack(grant_filepath, SLACK_CHANNEL_ID)
    else:
        print("Using Slack Webhook - sending to webhook (test mode)...")
    
    # Build summaries
    kikoff_summary = kikoff_data["summary"]
    if not kikoff_summary.empty:
        kikoff_delivered = int(kikoff_summary["delivered"].sum())
        kikoff_fraud = int(kikoff_summary["fraud"].sum())
        kikoff_outside_attr = int(kikoff_summary["outside_attribution"].sum()) if "outside_attribution" in kikoff_summary.columns else 0
        kikoff_net_valid = int(kikoff_summary["net_valid"].sum())
        kikoff_fraud_rate = (kikoff_fraud / kikoff_delivered * 100) if kikoff_delivered > 0 else 0
        kikoff_outside_attr_rate = (kikoff_outside_attr / kikoff_delivered * 100) if kikoff_delivered > 0 else 0
    else:
        kikoff_delivered = kikoff_fraud = kikoff_outside_attr = kikoff_net_valid = 0
        kikoff_fraud_rate = kikoff_outside_attr_rate = 0
    
    grant_summary = grant_data["summary"]
    if not grant_summary.empty:
        grant_delivered = int(grant_summary["delivered"].sum())
        grant_fraud = int(grant_summary["fraud"].sum())
        grant_addl_fraud = int(grant_summary["addl_fraud"].sum()) if "addl_fraud" in grant_summary.columns else 0
        grant_net_valid = int(grant_summary["net_valid"].sum())
        grant_fraud_rate = (grant_fraud / grant_delivered * 100) if grant_delivered > 0 else 0
        grant_addl_fraud_rate = (grant_addl_fraud / grant_delivered * 100) if grant_delivered > 0 else 0
    else:
        grant_delivered = grant_fraud = grant_addl_fraud = grant_net_valid = 0
        grant_fraud_rate = grant_addl_fraud_rate = 0
    
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"📊 Monthly AppsFlyer Partner Report — {report_month}", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*KIKOFF* (`{KIKOFF_EVENT_NAME}`)\n• Delivered: *{kikoff_delivered:,}*\n• Fraud (P360): *{kikoff_fraud:,}* ({kikoff_fraud_rate:.1f}%)\n• Outside Attribution: *{kikoff_outside_attr:,}* ({kikoff_outside_attr_rate:.1f}%)\n• Net Valid: *{kikoff_net_valid:,}*"}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*GRANT CASH ADVANCE* (`{GRANT_EVENT_NAME}`)\n• Delivered: *{grant_delivered:,}*\n• Fraud (P360): *{grant_fraud:,}* ({grant_fraud_rate:.1f}%)\n• Add'l Fraud: *{grant_addl_fraud:,}* ({grant_addl_fraud_rate:.1f}%)\n• Net Valid: *{grant_net_valid:,}*"}}
    ]
    
    if use_channel and (kikoff_permalink or grant_permalink):
        download_text = "📎 *Download Reports:*\n"
        if kikoff_permalink:
            download_text += f"• <{kikoff_permalink}|Kikoff Report>\n"
        if grant_permalink:
            download_text += f"• <{grant_permalink}|Grant Report>"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": download_text}})
    elif use_webhook:
        github_repo = os.environ.get("GITHUB_REPOSITORY", "")
        github_run_id = os.environ.get("GITHUB_RUN_ID", "")
        if github_repo and github_run_id:
            artifact_url = f"https://github.com/{github_repo}/actions/runs/{github_run_id}"
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"📎 <{artifact_url}|Download Reports> (requires GitHub access)\n_[Test mode - using webhook]_"}})
    
    if use_channel:
        send_slack_message_to_channel(blocks)
    else:
        send_slack_message_to_webhook(blocks)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def process_kikoff_app(from_date, to_date):
    print("\n" + "=" * 60)
    print("KIKOFF APP")
    print("=" * 60)
    
    print("\n" + "-" * 40)
    print("Pulling AppsFlyer Data")
    print("-" * 40)
    
    delivered_df, fraud_df = pull_all_reports(from_date, to_date, KIKOFF_APP_IDS, KIKOFF_EVENT_NAME)
    print(f"\nTotal delivered events: {len(delivered_df)}")
    print(f"Total fraud events: {len(fraud_df)}")
    
    print("\n" + "-" * 40)
    print("Applying Outside Attribution Rules")
    print("-" * 40)
    
    delivered_df = apply_kikoff_flagging_rules(delivered_df)
    
    flagged_events_df = delivered_df[delivered_df["is_flagged"] == True].copy() if not delivered_df.empty and "is_flagged" in delivered_df.columns else pd.DataFrame()
    print(f"Outside attribution events (before dedup): {len(flagged_events_df)}")
    
    # Dedup against P360
    if not flagged_events_df.empty and not fraud_df.empty:
        flagged_events_df.columns = flagged_events_df.columns.str.strip().str.lower().str.replace(' ', '_')
        fraud_df.columns = fraud_df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        customer_id_col = next((col for col in flagged_events_df.columns if 'customer' in col and 'id' in col), None)
        appsflyer_id_col = next((col for col in flagged_events_df.columns if 'appsflyer' in col and 'id' in col), None)
        
        if customer_id_col and appsflyer_id_col:
            flagged_events_df["_match_key"] = flagged_events_df[customer_id_col].astype(str) + "_" + flagged_events_df[appsflyer_id_col].astype(str)
            fraud_df["_match_key"] = fraud_df[customer_id_col].astype(str) + "_" + fraud_df[appsflyer_id_col].astype(str)
            
            fraud_keys = set(fraud_df["_match_key"].unique())
            flagged_events_df = flagged_events_df[~flagged_events_df["_match_key"].isin(fraud_keys)]
            flagged_events_df = flagged_events_df.drop(columns=["_match_key"])
            fraud_df = fraud_df.drop(columns=["_match_key"])
            print(f"Outside attribution events (after dedup): {len(flagged_events_df)}")
    
    print("\n" + "-" * 40)
    print("Aggregating by Agency")
    print("-" * 40)
    
    delivered_agg = aggregate_by_agency(delivered_df, "delivered")
    fraud_agg = aggregate_by_agency(fraud_df, "fraud")
    flagged_agg = aggregate_by_agency(flagged_events_df, "outside_attribution")
    
    if delivered_agg.empty:
        summary_df = pd.DataFrame(columns=["agency", "delivered", "fraud", "outside_attribution", "fraud_rate_%", "outside_attr_rate_%", "net_valid"])
    else:
        summary_df = delivered_agg.copy()
        summary_df = summary_df.merge(fraud_agg, on="agency", how="left") if not fraud_agg.empty else summary_df.assign(fraud=0)
        summary_df = summary_df.merge(flagged_agg[["agency", "outside_attribution"]], on="agency", how="left") if not flagged_agg.empty else summary_df.assign(outside_attribution=0)
        summary_df = summary_df.fillna(0)
        
        for col in ["delivered", "fraud", "outside_attribution"]:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').fillna(0)
        
        summary_df["net_valid"] = (summary_df["delivered"] - summary_df["fraud"] - summary_df["outside_attribution"]).astype(int)
        summary_df["fraud_rate_%"] = summary_df.apply(lambda row: round(row["fraud"] / row["delivered"] * 100, 1) if row["delivered"] > 0 else 0, axis=1)
        summary_df["outside_attr_rate_%"] = summary_df.apply(lambda row: round(row["outside_attribution"] / row["delivered"] * 100, 1) if row["delivered"] > 0 else 0, axis=1)
        
        for col in ["delivered", "fraud", "outside_attribution", "net_valid"]:
            summary_df[col] = summary_df[col].astype(int)
        
        summary_df = summary_df.sort_values("delivered", ascending=False)
    
    print("\nKikoff Summary:")
    if not summary_df.empty:
        print(summary_df.to_string(index=False))
    
    return {"summary": summary_df, "delivered": delivered_df, "fraud": fraud_df, "flagged": flagged_events_df}


def process_grant_app(from_date, to_date):
    print("\n" + "=" * 60)
    print("GRANT CASH ADVANCE APP")
    print("=" * 60)
    
    print("\n" + "-" * 40)
    print("Pulling AppsFlyer Data")
    print("-" * 40)
    
    delivered_df, fraud_df = pull_all_reports(from_date, to_date, GRANT_APP_IDS, GRANT_EVENT_NAME)
    print(f"\nTotal delivered events: {len(delivered_df)}")
    print(f"Total fraud events: {len(fraud_df)}")
    
    print("\n" + "-" * 40)
    print("Applying Add'l Fraud Rules")
    print("-" * 40)
    
    addl_fraud_df = apply_grant_addl_fraud_rules(delivered_df, fraud_df)
    print(f"Add'l Fraud events: {len(addl_fraud_df)}")
    
    # Normalize and filter excluded agencies
    if not delivered_df.empty:
        delivered_df.columns = delivered_df.columns.str.strip().str.lower().str.replace(' ', '_')
        agency_col = next((col for col in ['agency', 'partner', 'af_prt', 'media_source'] if col in delivered_df.columns), None)
        delivered_df["agency_normalized"] = delivered_df[agency_col].fillna("unknown").str.strip().str.lower() if agency_col else "unknown"
        
        before_count = len(delivered_df)
        delivered_df = delivered_df[~delivered_df["agency_normalized"].isin(GRANT_EXCLUDED_AGENCIES)]
        delivered_df = delivered_df[delivered_df["agency_normalized"].notna()]
        print(f"Delivered events after filtering excluded agencies: {len(delivered_df)} (removed {before_count - len(delivered_df)})")
    
    if not fraud_df.empty:
        fraud_df.columns = fraud_df.columns.str.strip().str.lower().str.replace(' ', '_')
        agency_col = next((col for col in ['agency', 'partner', 'af_prt', 'media_source'] if col in fraud_df.columns), None)
        fraud_df["agency_normalized"] = fraud_df[agency_col].fillna("unknown").str.strip().str.lower() if agency_col else "unknown"
        
        before_count = len(fraud_df)
        fraud_df = fraud_df[~fraud_df["agency_normalized"].isin(GRANT_EXCLUDED_AGENCIES)]
        fraud_df = fraud_df[fraud_df["agency_normalized"].notna()]
        print(f"Fraud events after filtering excluded agencies: {len(fraud_df)} (removed {before_count - len(fraud_df)})")
    
    if not addl_fraud_df.empty:
        before_count = len(addl_fraud_df)
        addl_fraud_df = addl_fraud_df[~addl_fraud_df["agency_normalized"].isin(GRANT_EXCLUDED_AGENCIES)]
        addl_fraud_df = addl_fraud_df[addl_fraud_df["agency_normalized"].notna()]
        print(f"Add'l Fraud events after filtering excluded agencies: {len(addl_fraud_df)} (removed {before_count - len(addl_fraud_df)})")
    
    print("\n" + "-" * 40)
    print("Aggregating by Agency")
    print("-" * 40)
    
    delivered_agg = aggregate_by_agency(delivered_df, "delivered")
    fraud_agg = aggregate_by_agency(fraud_df, "fraud")
    addl_fraud_agg = aggregate_by_agency(addl_fraud_df, "addl_fraud")
    
    if delivered_agg.empty:
        summary_df = pd.DataFrame(columns=["agency", "delivered", "fraud", "addl_fraud", "fraud_rate_%", "addl_fraud_rate_%", "net_valid"])
    else:
        summary_df = delivered_agg.copy()
        summary_df = summary_df.merge(fraud_agg, on="agency", how="left") if not fraud_agg.empty else summary_df.assign(fraud=0)
        summary_df = summary_df.merge(addl_fraud_agg[["agency", "addl_fraud"]], on="agency", how="left") if not addl_fraud_agg.empty else summary_df.assign(addl_fraud=0)
        summary_df = summary_df.fillna(0)
        
        for col in ["delivered", "fraud", "addl_fraud"]:
            summary_df[col] = pd.to_numeric(summary_df[col], errors='coerce').fillna(0)
        
        summary_df["net_valid"] = (summary_df["delivered"] - summary_df["fraud"] - summary_df["addl_fraud"]).astype(int)
        summary_df["fraud_rate_%"] = summary_df.apply(lambda row: round(row["fraud"] / row["delivered"] * 100, 1) if row["delivered"] > 0 else 0, axis=1)
        summary_df["addl_fraud_rate_%"] = summary_df.apply(lambda row: round(row["addl_fraud"] / row["delivered"] * 100, 1) if row["delivered"] > 0 else 0, axis=1)
        
        for col in ["delivered", "fraud", "addl_fraud", "net_valid"]:
            summary_df[col] = summary_df[col].astype(int)
        
        summary_df = summary_df.sort_values("delivered", ascending=False)
    
    print("\nGrant Summary:")
    if not summary_df.empty:
        print(summary_df.to_string(index=False))
    
    return {"summary": summary_df, "delivered": delivered_df, "fraud": fraud_df, "flagged": addl_fraud_df}


def main():
    print("=" * 60)
    print("AppsFlyer Monthly Partner Report")
    print("=" * 60)
    
    from_date, to_date = get_previous_month_range()
    report_month = get_report_month_name()
    
    print(f"\nReport Period: {report_month}")
    print(f"Date Range: {from_date} to {to_date}")
    
    kikoff_data = process_kikoff_app(from_date, to_date)
    grant_data = process_grant_app(from_date, to_date)
    
    print("\n" + "-" * 40)
    print("Generating Excel Reports")
    print("-" * 40)
    
    kikoff_filepath = generate_kikoff_excel_report(kikoff_data, report_month)
    grant_filepath = generate_grant_excel_report(grant_data, report_month)
    
    print("\n" + "-" * 40)
    print("Sending Slack Notification")
    print("-" * 40)
    
    send_combined_slack_notification(kikoff_data, grant_data, report_month, kikoff_filepath, grant_filepath)
    
    print("\n" + "=" * 60)
    print("Report Complete!")
    print("=" * 60)
    
    return kikoff_filepath, grant_filepath


if __name__ == "__main__":
    main()
