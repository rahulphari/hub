# hub_app.py - Final Version with Memory Optimization

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
CORS(app, resources={r"/api/*": {"origins": "*"}})

# --- Global variables & Constants ---
APP_START_TIME = datetime.now()
BACKEND_VERSION = "4.9.7_MEMORY_FIX" 
TOTAL_ANALYSES_PERFORMED = 0
LAST_ANALYSIS_TIME = "Never"

# --- Mappings (No changes needed) ---
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

# Insight Generation Functions are unchanged
# ... (get_ntc_breakdown, get_put_predictor_insights, etc.)

@app.route('/api/hub-analytics', methods=['POST'])
def hub_analytics_api():
    global TOTAL_ANALYSES_PERFORMED, LAST_ANALYSIS_TIME
    try:
        data = request.get_json()
        csv_content = data.get('csv_content')
        file_timestamp_str = data.get('file_timestamp')

        if not csv_content:
            return jsonify({"error": "Missing csv_content in payload."}), 400

        current_time_utc = datetime.fromisoformat(file_timestamp_str.replace('Z', '+00:00')) if file_timestamp_str else datetime.now(timezone.utc)
        current_time = current_time_utc.astimezone(timezone(timedelta(hours=5, minutes=30))).replace(tzinfo=None)
        
        separator = '\t' if '\t' in csv_content.splitlines()[0] else ','

        # --- MEMORY OPTIMIZATION ---
        # Specify data types for columns with few unique values to save memory
        dtype_mapping = {
            'bag_status': 'category',
            'pdt': 'category',
            'priority': 'category',
            'ntc_used': 'category',
            'client_name': 'category',
            'putaway_location': 'category'
        }
        
        df = pd.read_csv(io.StringIO(csv_content), sep=separator, dtype=dtype_mapping, low_memory=False)
        logging.info(f"DataFrame loaded. Shape: {df.shape}. Memory usage: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # ... (Rest of the data processing logic is the same as the last correct version) ...
        # --- Column renaming ---
        possible_id_names = ['bag_id', 'bagid', 'bag_identifier']
        found_id_col = next((name for name in possible_id_names if name in df.columns), None)
        if found_id_col and found_id_col != 'bag_id':
            df.rename(columns={found_id_col: 'bag_id'}, inplace=True)
        if 'bag_id' not in df.columns:
            return jsonify({"error": "Critical Error: 'Bag ID' column not found."}), 400

        # --- Date parsing ---
        df['incoming_time_dt'] = pd.to_datetime(df['incoming_time'], errors='coerce')
        failed_mask = df['incoming_time_dt'].isna()
        if failed_mask.any():
            base_date = current_time.date()
            def parse_time_only(time_str):
                try:
                    time_parts = re.split(r'[:]', str(time_str).strip())
                    if '.' in time_parts[-1]:
                        seconds_parts = time_parts[-1].split('.')
                        seconds, microseconds = int(seconds_parts[0]), int(seconds_parts[1][:6].ljust(6, '0'))
                    else:
                        seconds, microseconds = (int(time_parts[-1]) if len(time_parts) > 2 else 0), 0
                    minutes, hours = int(time_parts[1]), int(time_parts[0])
                    day_offset = timedelta(days=hours // 24)
                    hours %= 24
                    dt_obj = datetime.combine(base_date, datetime.min.time()).replace(hour=hours, minute=minutes, second=seconds, microsecond=microseconds) + day_offset
                    if dt_obj > current_time:
                        dt_obj -= timedelta(days=1)
                    return dt_obj
                except (ValueError, IndexError):
                    return pd.NaT
            df.loc[failed_mask, 'incoming_time_dt'] = df.loc[failed_mask, 'incoming_time'].apply(parse_time_only)

        # --- Continue with the rest of your processing ---
        df['etd'] = pd.to_datetime(df['etd'], errors='coerce')
        in_center_df = df[df['bag_status'] == 'in_center'].copy()
        
        if not in_center_df.empty:
            in_center_df['age_timedelta'] = current_time - in_center_df['incoming_time_dt']
            in_center_df['age_hours'] = in_center_df['age_timedelta'].dt.total_seconds() / 3600
            # ... and so on ...

        # The final JSON response creation remains the same
        # ...

        # This is a placeholder for the rest of your function. 
        # You need to copy the full logic from your working file here.
        # The key change is the pd.read_csv line above.
        
        final_df_dict = in_center_df.to_dict(orient='records') # Simplified for brevity
        response_data = {"detailed_data": final_df_dict} # Simplified

        TOTAL_ANALYSES_PERFORMED += 1
        LAST_ANALYSIS_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(response_data)

    except Exception as e:
        logging.error(f"An unhandled error occurred in hub_analytics_api: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/api/hub-status', methods=['GET'])
def get_hub_backend_status():
    uptime_seconds = (datetime.now() - APP_START_TIME).total_seconds()
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    uptime_str = f"{int(hours)}h {int(minutes)}m"
    return jsonify({
        "status": "online", "version": BACKEND_VERSION, "uptime": uptime_str,
        "last_analysis_time": LAST_ANALYSIS_TIME, "total_analyses": TOTAL_ANALYSES_PERFORMED
    }), 200
