# hub_app.py - Final Version 4.9.4
# This version corrects the volume unit conversion from cubic feet to cubic centimeters.

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
import io
import logging
from datetime import datetime, timedelta, timezone
import re
import math

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app)

# --- Global variables & Constants ---
APP_START_TIME = datetime.now()
BACKEND_VERSION = "4.9.4_UNIT_CONVERSION_FINAL" 
TOTAL_ANALYSES_PERFORMED = 0
LAST_ANALYSIS_TIME = "Never"

# --- Definitive Mappings ---
LANE_MAP = {
    'Coimbatore_Pudhupalayam_GW': 'FTL', 'Mangalore_Katipalla_H': 'FTL', 'Hassan_Nagathavalli_I': 'FTL',
    'Gurgaon_Tauru_GW': 'FTL', 'Davangere_Industrialarea_I': 'FTL', 'Pune_Sudhwadi_GW': 'FTL',
    'Surat_Kacholi_GW': 'FTL', 'Bellary_Mundargi_I': 'FTL', 'Dharwad_Mrityunjaya_D': 'CARTING',
    'Kolhapur_HupariRd_H': 'FTL', 'Shimoga_Eshwariah_I': 'FTL', 'Anantapur_AyyavaripalliRD_H': 'FTL',
    'Bhiwandi_Lonad_GW': 'FTL', 'Belgaum_MarutiNagar_I': 'FTL', 'Sirsi_Kasturbanagar_D': 'CARTING',
    'Bangalore_Hoskote_GW': 'FTL', 'Gadag_Laxmeshwar_D': 'CARTING', 'Kalaghatgi_Gabbur_D': 'CARTING',
    'Hangal_PalaRd_D': 'CARTING', 'Gadag_VijayNagara_D': 'CARTING', 'Pune_Chimbali_H': 'FTL',
    'Goa_Hub': 'FTL', 'Ilkal_Ward4_D': 'CARTING', 'Hubli_ShanthiColony_D': 'CARTING',
    'Yelburga_SurabhiColony_D': 'CARTING', 'Rona_GadagRD_D': 'CARTING', 'Yellapur_Tatagar_DPP': 'CARTING',
    'Saundatti_Bypassroad_D': 'CARTING', 'Hubli_NehruNagar_D': 'CARTING', 'Mundgod_BankapuraRd_DPP': 'CARTING',
    'Haliyal_Anegundicolony_DPP': 'CARTING', 'Dandeli_MarutiNgr_DPP': 'CARTING',
    'Kalaghatgi_Machapur_D': 'CARTING', 'Alnavar_Gharli_DPP': 'CARTING', 'Ilkal_Kushtagi_DPP': 'CARTING',
    'Navalgund_BasveshwarNgr_DPP': 'CARTING', 'Navalgund_Nargund_DPP': 'CARTING',
    'Ramdurg_MahanteshNagar_DPP': 'CARTING', 'Alnavar_BusStndDPP_D': 'CARTING',
    'Badami_SaibabaNagar_DPP': 'CARTING', 'Sirsi_Banavasi1_DPP': 'CARTING'
}
NTC_VEHICLE_MAP = {
    'Alnavar_BusStndDPP_D': 'Truck 14Ft', 'Alnavar_Gharli_DPP': 'Truck 14Ft', 'Anantapur_AyyavaripalliRD_H': 'Truck 32Ft (Sxl)',
    'Badami_SaibabaNagar_DPP': 'Mini Truck (Ace)', 'Bangalore_Hoskote_GW': 'Truck 32Ft (Mxl)', 'Belgaum_MarutiNagar_I': 'Truck 32Ft (Sxl)',
    'Bellary_Mundargi_I': 'Truck 32Ft (Sxl)', 'Bhiwandi_Lonad_GW': 'Truck 43Ft', 'Coimbatore_Pudhupalayam_GW': 'Truck 43Ft',
    'Dandeli_MarutiNgr_DPP': 'Pickup Truck', 'Davangere_Industrialarea_I': 'Truck 32Ft (Sxl)', 'Dharwad_Mrityunjaya_D': 'Truck 14Ft',
    'Gadag_Laxmeshwar_D': 'Truck (407)', 'Gadag_VijayNagara_D': 'Truck (407)', 'Goa_Hub': 'Truck 24Ft',
    'Gurgaon_Tauru_GW': 'Truck 43Ft', 'Haliyal_Anegundicolony_DPP': 'Pickup Truck', 'Hangal_PalaRd_D': 'Pickup Truck',
    'Hassan_Nagathavalli_I': 'Truck 32Ft (Mxl)', 'Hubli_NehruNagar_D': 'Pickup Truck', 'Ilkal_Ward4_D': 'Pickup Truck',
    'Kalaghatgi_Gabbur_D': 'Pickup Truck', 'Kalaghatgi_Machapur_D': 'Pickup Truck', 'Kolhapur_HupariRd_H': 'Truck 43Ft',
    'Mangalore_Katipalla_H': 'Truck 43Ft', 'Mundgod_BankapuraRd_DPP': 'Pickup Truck', 'Navalgund_BasveshwarNgr_DPP': 'Truck (407)',
    'Navalgund_Nargund_DPP': 'Mini Truck (Ace)', 'Pune_Chimbali_H': 'Truck 32Ft (Mxl)', 'Pune_Sudhwadi_GW': 'Truck 43Ft',
    'Ramdurg_MahanteshNagar_DPP': 'Pickup Truck', 'Rona_GadagRD_D': 'Pickup Truck', 'Saundatti_Bypassroad_D': 'Pickup Truck',
    'Shimoga_Eshwariah_I': 'Truck 32Ft (Sxl)', 'Sirsi_Kasturbanagar_D': 'Truck (407)', 'Surat_Kacholi_GW': 'Truck 43Ft',
    'Yelburga_SurabhiColony_D': 'Pickup Truck', 'Yellapur_Tatagar_DPP': 'Pickup Truck'
}
VEHICLE_CAPACITY_MAP = {
    'Pickup Truck': {'wt': 1000000, 'vol': 4955475}, 'Super Ace (7/7.5Ft)': {'wt': 1000000, 'vol': 4955475},
    'Truck (407)': {'wt': 2000000, 'vol': 11185215}, 'Truck 10Ft': {'wt': 2500000, 'vol': 11185215},
    'Truck 14Ft': {'wt': 3500000, 'vol': 14271768}, 'Truck 17Ft': {'wt': 5500000, 'vol': 17330004},
    'Truck 20Ft': {'wt': 6500000, 'vol': 27750660}, 'Truck 22Ft': {'wt': 8000000, 'vol': 30525726},
    'Truck 24Ft': {'wt': 8000000, 'vol': 43494912}, 'Truck 32Ft (Mxl)': {'wt': 17200000, 'vol': 57993216},
    'Truck 32Ft (Sxl)': {'wt': 9000000, 'vol': 57993216}, 'Truck 43Ft': {'wt': 20000000, 'vol': 67700000},
    'Mini Truck (Ace)': {'wt': 850000, 'vol': 4800000}
}

SANITIZED_LANE_MAP = {key.strip().upper(): value for key, value in LANE_MAP.items()}
SANITIZED_NTC_VEHICLE_MAP = {key.strip().upper(): value for key, value in NTC_VEHICLE_MAP.items()}

# --- Helper Functions ---
def clean_bag_id(bag_id):
    if isinstance(bag_id, str): return bag_id.lstrip("'")
    return bag_id

def classify_putaway_location(location):
    if not isinstance(location, str) or location == 'nan' or pd.isna(location): return 'Put Pending'
    if 'R36' in location: return 'LM/FM Hold Area'
    if 'DS' in location: return 'Docks'
    if 'DOC' in location: return 'Doc Bin'
    if 'PST' in location: return 'PST'
    if re.match(r'Z\d+\.R\d+\.B\d+', location): return 'Standard Floor'
    return 'Other'

def parse_incoming_time(time_str, reference_date):
    if pd.isna(time_str): return pd.NaT
    time_str = str(time_str).strip()
    is_full_datetime = ('-' in time_str or '/' in time_str) and ':' in time_str
    if is_full_datetime:
        try: return pd.to_datetime(time_str)
        except (ValueError, TypeError): return pd.NaT
    else:
        try:
            time_parts = re.split(r'[:]', time_str)
            if '.' in time_parts[-1]:
                seconds_parts = time_parts[-1].split('.')
                seconds, microseconds = int(seconds_parts[0]), int(seconds_parts[1][:6].ljust(6, '0'))
            else:
                seconds, microseconds = (int(time_parts[-1]) if len(time_parts) > 2 else 0), 0
            minutes, hours = int(time_parts[1]), int(time_parts[0])
            day_offset = timedelta(days=hours // 24)
            hours %= 24
            base_date = reference_date.date()
            incoming_dt = datetime.combine(base_date, datetime.min.time()).replace(hour=hours, minute=minutes, second=seconds, microsecond=microseconds) + day_offset
            if incoming_dt > reference_date: incoming_dt -= timedelta(days=1)
            return incoming_dt
        except (ValueError, IndexError): return pd.NaT

def format_age_string(delta):
    if pd.isna(delta) or delta.total_seconds() < 0: return "Invalid"
    days = delta.days; hours, remainder = divmod(delta.seconds, 3600); minutes, _ = divmod(remainder, 60)
    return f"{days} D {int(hours)} H {int(minutes)} M"

def format_etd_string(etd, current_time):
    if pd.isna(etd): return "No ETD available"
    delta = etd - current_time
    if delta.total_seconds() < 0: return "ETD has passed"
    days, remainder = delta.days, delta.seconds; hours, remainder = divmod(remainder, 3600); minutes, _ = divmod(remainder, 60)
    total_hours = days * 24 + int(hours)
    return f"Next connection in {total_hours} hours {int(minutes)} mins"

# --- Insight Generation Functions ---
def get_ntc_breakdown(lane_type_df, current_time):
    if lane_type_df.empty: return []
    ntc_summary = lane_type_df.groupby('ntc_used').agg(total_wbns=('bag_id', 'count'), next_etd=('etd', 'min')).reset_index()
    age_breakdown_per_ntc = lane_type_df.groupby(['ntc_used', 'ageing_breakdown']).size().unstack(fill_value=0)
    docks_df = lane_type_df[lane_type_df['put_status'] == 'Docks']
    docks_counts = docks_df.groupby('ntc_used').size().reset_index(name='bags_in_docks')
    ntc_summary = ntc_summary.merge(age_breakdown_per_ntc, on='ntc_used', how='left')
    ntc_summary = ntc_summary.merge(docks_counts, on='ntc_used', how='left').fillna(0)
    if 'Invalid Age' in ntc_summary.columns: ntc_summary = ntc_summary.drop(columns=['Invalid Age'])
    ntc_summary['etd_string'] = ntc_summary['next_etd'].apply(lambda x: format_etd_string(x, current_time))
    ntc_summary = ntc_summary.drop(columns=['next_etd'])
    for col in ntc_summary.columns:
        if 'hrs' in str(col) or col in ['total_wbns', 'bags_in_docks']: ntc_summary[col] = ntc_summary[col].astype(int)
    return ntc_summary.sort_values(by='total_wbns', ascending=False).to_dict(orient='records')
    
def get_put_predictor_insights(df, current_put_compliance):
    put_pending_df = df[df['put_status'] == 'Put Pending']
    total_put_pending = len(put_pending_df)
    old_shipments = df[df['age_hours'] > 2]
    old_put_shipments = old_shipments[old_shipments['put_status'] != 'Put Pending']
    def calculate_projection(minutes):
        time_window = minutes / 60.0; at_risk_df = put_pending_df[(put_pending_df['age_hours'] >= (2.0 - time_window)) & (put_pending_df['age_hours'] < 2.0)]
        at_risk_count = len(at_risk_df); future_old_shipments = len(old_shipments) + at_risk_count
        future_put_compliance = (len(old_put_shipments) / future_old_shipments * 100) if future_old_shipments > 0 else 100
        put_drop = max(0, current_put_compliance - future_put_compliance)
        return {'count': at_risk_count, 'put_drop': f"{put_drop:.2f}%"}
    critically_old_df = put_pending_df[put_pending_df['age_hours'] > 4]
    critical_count = len(critically_old_df); target_rate = {'needed': False, 'rate': 0}
    target_put_percent = 95.0
    if current_put_compliance < target_put_percent and not old_shipments.empty:
        needed_puts_to_reach_target = math.ceil((target_put_percent / 100 * len(old_shipments)) - len(old_put_shipments))
        if needed_puts_to_reach_target > 0: target_rate = {'needed': True, 'rate': needed_puts_to_reach_target}
    return {'total_pending': total_put_pending, 'at_risk_15m': calculate_projection(15), 'at_risk_30m': calculate_projection(30), 'critical_4h_plus': {'count': critical_count}, 'target_rate': target_rate}

def get_imminent_departures_insight(df, current_time):
    two_hours_from_now = current_time + timedelta(hours=2)
    imminent_departures_df = df[(df['etd'] > current_time) & (df['etd'] <= two_hours_from_now) & (df['put_status'] == 'Put Pending')].copy()
    if imminent_departures_df.empty: return []
    summary = imminent_departures_df.groupby('ntc_used').agg(pending_count=('bag_id', 'count'), next_etd=('etd', 'min')).reset_index()
    results = []
    for _, row in summary.sort_values(by='next_etd').iterrows():
        delta = row['next_etd'] - current_time; minutes_to_departure = delta.total_seconds() / 60
        recommendation = "Monitor"
        if row['pending_count'] > 5: recommendation = "High Priority Put"
        if row['pending_count'] > 10 and minutes_to_departure < 45: recommendation = "CRITICAL: Cross-Dock"
        results.append({'ntc_used': row['ntc_used'], 'pending_count': row['pending_count'], 'departs_in_mins': int(minutes_to_departure), 'recommendation': recommendation})
    return results

def get_carting_at_docks_insight(df, current_time):
    nine_hours_from_now = current_time + timedelta(hours=9)
    actionable_df = df[(df['lane_type'] == 'CARTING') & (df['put_status'] == 'Docks') & (df['etd'] > current_time) & (df['etd'] <= nine_hours_from_now)].copy()
    if actionable_df.empty: return []
    summary = actionable_df.groupby('ntc_used').agg(count=('bag_id', 'count'), etd=('etd', 'min')).reset_index()
    summary['etd_str'] = summary['etd'].dt.strftime('%H:%M')
    return summary.sort_values(by='etd').to_dict(orient='records')

def get_load_analysis(df, current_time):
    if df.empty or 'vehicle_wt_capacity' not in df.columns: return {'utilization_table': [], 'load_alerts': [], 'adhoc_suggestions': []}
    three_hours_from_now = current_time + timedelta(hours=3)
    upcoming_df = df[(df['etd'] > current_time) & (df['etd'] <= three_hours_from_now)].copy()
    utilization_table, adhoc_suggestions, load_alerts = [], [], []
    if not upcoming_df.empty:
        ntc_groups = upcoming_df.groupby('ntc_used')
        for ntc, group in ntc_groups:
            wt_cap, vol_cap = group['vehicle_wt_capacity'].iloc[0], group['vehicle_vol_capacity'].iloc[0]
            if wt_cap == 0: continue
            put_df = group[(group['bag_status'] == 'in_center') & (group['put_status'] != 'Put Pending')]
            put_wt, put_vol = put_df['bag_wt'].sum(), put_df['bag_vol'].sum()
            not_put_df = group[(group['bag_status'] == 'in_center') & (group['put_status'] == 'Put Pending')]
            not_put_wt, not_put_vol = not_put_df['bag_wt'].sum(), not_put_df['bag_vol'].sum()
            in_center_df = group[group['bag_status'] == 'in_center']
            total_in_center_wt, total_in_center_vol = in_center_df['bag_wt'].sum(), in_center_df['bag_vol'].sum()
            max_possible_wt, max_possible_vol = group['bag_wt'].sum(), group['bag_vol'].sum()
            utilization_table.append({
                'ntc': ntc, 'vehicle_size': group['vehicle_size'].iloc[0], 'next_connection': format_etd_string(group['etd'].min(), current_time),
                'put_util_v': (put_vol / vol_cap * 100) if vol_cap > 0 else 0, 'put_util_w': (put_wt / wt_cap * 100) if wt_cap > 0 else 0,
                'not_put_util_v': (not_put_vol / vol_cap * 100) if vol_cap > 0 else 0, 'not_put_util_w': (not_put_wt / wt_cap * 100) if wt_cap > 0 else 0,
                'total_util_v': (total_in_center_vol / vol_cap * 100) if vol_cap > 0 else 0, 'total_util_w': (total_in_center_wt / wt_cap * 100) if wt_cap > 0 else 0,
                'max_util_v': (max_possible_vol / vol_cap * 100) if vol_cap > 0 else 0, 'max_util_w': (max_possible_wt / wt_cap * 100) if wt_cap > 0 else 0,
            })
            max_wt_util, max_vol_util = (max_possible_wt/wt_cap*100) if wt_cap > 0 else 0, (max_possible_vol/vol_cap*100) if vol_cap > 0 else 0
            if max_wt_util >= 150 or max_vol_util >= 150: adhoc_suggestions.append(f"NTC {ntc} has max potential utilization of {max(max_wt_util, max_vol_util):.0f}%. An ad-hoc vehicle may be required.")
    
    df['time_bucket'] = df['incoming_time_dt'].dt.floor('4H')
    for _, row in df.iterrows():
        if row['vehicle_wt_capacity'] == 0: continue
        wt_util = (row['bag_wt'] / row['vehicle_wt_capacity'] * 100)
        vol_util = (row['bag_vol'] / row['vehicle_vol_capacity'] * 100) if 'bag_vol' in row and row['vehicle_vol_capacity'] > 0 else 0
        if row['lane_type'] == 'CARTING' and (wt_util > 30 or vol_util > 30):
            load_alerts.append(f"Single shipment {row['bag_id']} for carting lane {row['ntc_used']} is using {max(wt_util, vol_util):.0f}% of vehicle capacity.")
    if 'client_name' in df.columns:
        bulk_groups = df.groupby(['ntc_used', 'client_name', pd.Grouper(key='incoming_time_dt', freq='D'), 'time_bucket'])
        for name, group in bulk_groups:
            if len(group) > 1:
                ntc, client, _, _ = name
                wt_cap, vol_cap = group['vehicle_wt_capacity'].iloc[0], group['vehicle_vol_capacity'].iloc[0]
                if wt_cap == 0: continue
                total_wt, total_vol = group['bag_wt'].sum(), group['bag_vol'].sum()
                wt_util, vol_util = (total_wt / wt_cap * 100), (total_vol / vol_cap * 100) if vol_cap > 0 else 0
                if wt_util > 20 or vol_util > 20: load_alerts.append(f"A lot of {len(group)} shipments for '{client}' to {ntc} will utilise {wt_util:.0f}% of wt & {vol_util:.0f}% of available Vol of a {group['vehicle_size'].iloc[0]}.")
    return {'utilization_table': utilization_table, 'load_alerts': list(set(load_alerts)), 'adhoc_suggestions': adhoc_suggestions}

# --- Main API Endpoint ---
@app.route('/api/hub-analytics', methods=['POST'])
def hub_analytics_api():
    global TOTAL_ANALYSES_PERFORMED, LAST_ANALYSIS_TIME
    try:
        data = request.get_json()
        csv_content, file_timestamp_str = data.get('csv_content'), data.get('file_timestamp')
        if not csv_content: return jsonify({"error": "Missing csv_content in payload."}), 400
        current_time_utc = datetime.fromisoformat(file_timestamp_str.replace('Z', '+00:00')) if file_timestamp_str else datetime.now(timezone.utc)
        current_time = current_time_utc.astimezone(timezone(timedelta(hours=5, minutes=30))).replace(tzinfo=None)
        
        separator = '\t' if '\t' in csv_content.splitlines()[0] else ','
        df = pd.read_csv(io.StringIO(csv_content), sep=separator)
        
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        possible_id_names = ['bag_id', 'bagid', 'bag_identifier']
        found_id_col = None
        for name in possible_id_names:
            if name in df.columns:
                found_id_col = name
                break
        if found_id_col and found_id_col != 'bag_id':
            df.rename(columns={found_id_col: 'bag_id'}, inplace=True)
        
        if 'bag_id' not in df.columns:
            return jsonify({"error": "Critical Error: 'Bag ID' column not found. Please check the CSV headers."}), 400

        possible_wt_names = ['bag_wt', 'bagwt', 'weight', 'weight_(grams)']
        possible_vol_names = ['bag_vol', 'bagvol', 'volume', 'volume_(cc)']
        for name in possible_wt_names:
            if name in df.columns: df.rename(columns={name: 'bag_wt'}, inplace=True)
        for name in possible_vol_names:
            if name in df.columns: df.rename(columns={name: 'bag_vol'}, inplace=True)
        
        for col in ['bag_wt', 'bag_vol']:
            if col not in df.columns: df[col] = 0
        
        df['bag_id'] = df['bag_id'].apply(clean_bag_id)
        df['incoming_time_dt'] = df['incoming_time'].apply(lambda x: parse_incoming_time(x, current_time))
        df['etd'] = pd.to_datetime(df['etd'], errors='coerce')
        for col in ['package_count', 'bag_wt', 'bag_vol']: 
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(1 if col == 'package_count' else 0)
        
        # *** CORRECTED UNIT CONVERSION FIX ***
        # Convert bag volume from cubic feet (ft^3) to cubic centimeters (cm^3)
        if 'bag_vol' in df.columns:
            df['bag_vol'] = df['bag_vol'] * 28316.8

        df['put_status'] = df['putaway_location'].apply(classify_putaway_location)
        sanitized_ntc_series = df['ntc_used'].astype(str).str.strip().str.upper()
        df['lane_type'] = sanitized_ntc_series.map(SANITIZED_LANE_MAP).fillna('Unknown')
        df['vehicle_size'] = sanitized_ntc_series.map(SANITIZED_NTC_VEHICLE_MAP).fillna('Unknown')
        default_capacity = {'wt': 0, 'vol': 0}
        vehicle_capacities = df['vehicle_size'].apply(lambda x: VEHICLE_CAPACITY_MAP.get(x, default_capacity))
        df['vehicle_wt_capacity'] = vehicle_capacities.apply(lambda x: x.get('wt', 0))
        df['vehicle_vol_capacity'] = vehicle_capacities.apply(lambda x: x.get('vol', 0))

        in_center_df = df[df['bag_status'] == 'in_center'].copy()
        if not in_center_df.empty:
            in_center_df['age_timedelta'] = current_time - in_center_df['incoming_time_dt']
            in_center_df['age_hours'] = in_center_df['age_timedelta'].dt.total_seconds() / 3600
            in_center_df['age_str'] = in_center_df['age_timedelta'].apply(format_age_string)
            age_bins, age_labels = [-1, 24, 48, 72, np.inf], ['<24 hrs', '24-48 hrs', '48-72 hrs', '72+ hrs']
            in_center_df['ageing_breakdown'] = pd.cut(in_center_df['age_hours'], bins=age_bins, labels=age_labels, right=False)
        
        total_in_center = len(in_center_df)
        put_shipments_df = in_center_df[in_center_df['put_status'] != 'Put Pending']
        truput_percentage = (len(put_shipments_df) / total_in_center * 100) if total_in_center > 0 else 100
        old_shipments_df = in_center_df[in_center_df['age_hours'] > 2] if 'age_hours' in in_center_df else pd.DataFrame()
        old_put_shipments_df = old_shipments_df[old_shipments_df['put_status'] != 'Put Pending'] if not old_shipments_df.empty else pd.DataFrame()
        put_compliance_percentage = (len(old_put_shipments_df) / len(old_shipments_df) * 100) if not old_shipments_df.empty else 100
        pdt_breakdown = in_center_df['pdt'].value_counts().to_dict()
        priority_breakdown = in_center_df['priority'].value_counts().to_dict()
        put_status_breakdown = in_center_df['put_status'].value_counts().to_dict()
        ageing_breakdown = in_center_df['ageing_breakdown'].value_counts().to_dict() if 'ageing_breakdown' in in_center_df else {}
        ftl_breakdown = get_ntc_breakdown(in_center_df[in_center_df['lane_type'] == 'FTL'], current_time)
        carting_breakdown = get_ntc_breakdown(in_center_df[in_center_df['lane_type'] == 'CARTING'], current_time)

        put_predictor_results = get_put_predictor_insights(in_center_df, put_compliance_percentage)
        imminent_departures_results = get_imminent_departures_insight(in_center_df, current_time)
        carting_at_docks_results = get_carting_at_docks_insight(in_center_df, current_time)
        load_analysis_results = get_load_analysis(df, current_time)

        cols_to_drop = [col for col in ['age_timedelta', 'time_bucket', 'vehicle_wt_capacity', 'vehicle_vol_capacity'] if col in in_center_df.columns]
        final_df_dict = in_center_df.drop(columns=cols_to_drop).replace({np.nan: None, pd.NaT: None}).to_dict(orient='records')
        
        response_data = {
            "key_metrics": { "put_compliance": f"{put_compliance_percentage:.2f}%", "truput": f"{truput_percentage:.2f}%", "load_in_center": total_in_center },
            "breakdowns": { "pdt": pdt_breakdown, "priority": priority_breakdown, "put_status": put_status_breakdown, "ageing": ageing_breakdown },
            "ntc_breakdowns": { "ftl": ftl_breakdown, "carting": carting_breakdown }, "detailed_data": final_df_dict,
            "insights": { "put_predictor": put_predictor_results, "imminent_departures": imminent_departures_results,
                "carting_at_docks": carting_at_docks_results, "load_analysis": load_analysis_results,
                "unknown_ntcs": df[df['lane_type'] == 'Unknown']['ntc_used'].unique().tolist(),
            }
        }
        TOTAL_ANALYSES_PERFORMED += 1; LAST_ANALYSIS_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(response_data)
    except Exception as e:
        logging.error(f"An unhandled error occurred in hub_analytics_api: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/api/hub-status', methods=['GET'])
def get_hub_backend_status():
    uptime_seconds = (datetime.now() - APP_START_TIME).total_seconds()
    hours, remainder = divmod(uptime_seconds, 3600); minutes, _ = divmod(remainder, 60)
    uptime_str = f"{int(hours)}h {int(minutes)}m"
    return jsonify({"status": "online", "version": BACKEND_VERSION, "uptime": uptime_str, "last_analysis_time": LAST_ANALYSIS_TIME, "total_analyses": TOTAL_ANALYSES_PERFORMED}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
