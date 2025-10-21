# -*- coding: utf-8 -*-
"""
Hämtar tågdata från Trafikverkets API för senaste 3 dagarna (exkl. idag)
Sparar till Azure Data Lake: raw/departures_YYYYMMDD.parquet, raw/arrivals_YYYYMMDD.parquet
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Tuple

from azure.storage.filedatalake import DataLakeServiceClient
from config import API_KEY, TRV_URL, ACCOUNT_URL, CONTAINER_NAME, STORAGE_ACCOUNT_KEY

# Logging
from logger import get_logger
logger = get_logger("fetch_train_data")

# API Settings
HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
}

INCLUDES = [
    "ActivityId", "ActivityType", "AdvertisedTrainIdent",
    "AdvertisedTimeAtLocation", "EstimatedTimeAtLocation", "TimeAtLocationWithSeconds",
    "LocationSignature", "FromLocation", "ToLocation",
    "InformationOwner", "TrainOwner", "Canceled", "Operator",
    "TypeOfTraffic", "Deviation"
]


def get_azure_client():
    """Returnerar Azure Data Lake Service Client"""
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY)


def save_to_azure(df: pd.DataFrame, path: str):
    """Sparar DataFrame som Parquet till Azure"""
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        
        client = get_azure_client()
        fs = client.get_file_system_client(file_system=CONTAINER_NAME)
        file_client = fs.get_file_client(path)
        
        file_client.upload_data(buffer.read(), overwrite=True)
        logger.info(f"✅ Sparad till Azure: {path} ({len(df):,} rader)")
        return True
    except Exception as e:
        logger.error(f"❌ Misslyckades spara till Azure {path}: {e}")
        return False


def fetch_window(t0: datetime, t1: datetime, limit_per_window: int = 50000, 
                 max_retries: int = 3, sleep_sec: float = 2.0) -> pd.DataFrame:
    """
    Hämtar TrainAnnouncement för tidsfönster [t0, t1)
    
    Args:
        t0: Starttid
        t1: Sluttid
        limit_per_window: Max antal poster per request
        max_retries: Antal retry-försök
        sleep_sec: Bas-väntetid mellan retries (exponential backoff)
    
    Returns:
        DataFrame med tågdata (tom om fel)
    """
    xml = f"""<REQUEST>
  <LOGIN authenticationkey="{API_KEY}" />
  <QUERY objecttype="TrainAnnouncement" schemaversion="1.9"
         orderby="AdvertisedTimeAtLocation" limit="{limit_per_window}">
    <FILTER>
      <AND>
        <GTE name="AdvertisedTimeAtLocation" value="{t0.strftime('%Y-%m-%dT%H:%M:%S')}"/>
        <LT  name="AdvertisedTimeAtLocation" value="{t1.strftime('%Y-%m-%dT%H:%M:%S')}"/>
      </AND>
    </FILTER>
    {''.join(f'<INCLUDE>{f}</INCLUDE>' for f in INCLUDES)}
  </QUERY>
</REQUEST>"""

    headers = HEADERS.copy()
    headers["Accept"] = "application/json"

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(TRV_URL, data=xml.encode("utf-8"), headers=headers, timeout=60)
            resp.raise_for_status()
            
            payload = resp.json()
            rows = payload.get("RESPONSE", {}).get("RESULT", [])
            records = rows[0].get("TrainAnnouncement", []) if rows else []
            
            df_win = pd.json_normalize(records)

            # Konvertera tidskolumner till datetime (naive UTC)
            for col in ["AdvertisedTimeAtLocation", "EstimatedTimeAtLocation", "TimeAtLocationWithSeconds"]:
                if col in df_win.columns:
                    df_win[col] = pd.to_datetime(df_win[col], errors="coerce", utc=True).dt.tz_localize(None)

            return df_win

        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:  # Rate limit
                wait_time = sleep_sec * (2 ** (attempt - 1))
                logger.warning(f"⚠️ Rate limit (429) - väntar {wait_time:.1f}s innan retry {attempt}/{max_retries}")
                time.sleep(wait_time)
            elif attempt < max_retries:
                wait_time = sleep_sec * attempt
                logger.warning(f"⚠️ HTTP {resp.status_code} - retry {attempt}/{max_retries} efter {wait_time:.1f}s")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ HTTPError för fönster {t0:%H:%M}–{t1:%H:%M}: {e}")
                return pd.DataFrame()
                
        except requests.exceptions.Timeout:
            if attempt < max_retries:
                wait_time = sleep_sec * attempt
                logger.warning(f"⚠️ Timeout - retry {attempt}/{max_retries} efter {wait_time:.1f}s")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Timeout för fönster {t0:%H:%M}–{t1:%H:%M}")
                return pd.DataFrame()
                
        except Exception as e:
            if attempt < max_retries:
                wait_time = sleep_sec * attempt
                logger.warning(f"⚠️ Fel: {e} - retry {attempt}/{max_retries}")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Fel för fönster {t0:%H:%M}–{t1:%H:%M}: {e}")
                return pd.DataFrame()
    
    return pd.DataFrame()


def fetch_day(date: datetime) -> pd.DataFrame:
    """
    Hämtar data för en hel dag (24 timfönster)
    
    Args:
        date: Datum att hämta (kl 00:00)
    
    Returns:
        DataFrame med all data för dagen
    """
    logger.info(f"\n📅 Hämtar data för {date.strftime('%Y-%m-%d')}")
    
    start_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = start_day + timedelta(days=1)
    
    dfs = []
    t = start_day
    hour_num = 1
    
    while t < end_day:
        t_next = t + timedelta(hours=1)
        
        df_part = fetch_window(t, t_next, limit_per_window=50000)
        
        if not df_part.empty:
            logger.info(f"   [{hour_num:02d}/24] Hämtar {t:%H:%M}–{t_next:%H:%M}... {len(df_part):,} rader")
            dfs.append(df_part)
        else:
            logger.info(f"   [{hour_num:02d}/24] Hämtar {t:%H:%M}–{t_next:%H:%M}... 0 rader")
        
        t = t_next
        hour_num += 1
        
        # Paus mellan requests för att vara snäll mot API
        time.sleep(0.5)
    
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        logger.info(f"✅ Totalt {len(df_all):,} rader för {date.strftime('%Y-%m-%d')}")
        return df_all
    else:
        logger.warning(f"⚠️ Inga rader hämtades för {date.strftime('%Y-%m-%d')}")
        return pd.DataFrame()


def separate_and_save(df: pd.DataFrame, date: datetime) -> Tuple[bool, bool]:
    """
    Separerar data i departures/arrivals och sparar till Azure
    
    Args:
        df: DataFrame med all data
        date: Datum för datan
    
    Returns:
        (dep_success, arr_success): Tuple med boolean för varje sparning
    """
    if df.empty:
        logger.warning("⚠️ Tom DataFrame - inget att spara")
        return False, False
    
    date_str = date.strftime('%Y%m%d')
    
    # Säkerställ att ActivityType finns
    if 'ActivityType' not in df.columns:
        logger.error("❌ Kolumn 'ActivityType' saknas i data")
        return False, False
    
    # Separera baserat på ActivityType
    df_dep = df[df['ActivityType'].str.lower().isin(['avgang', 'avgång', 'departure'])].copy()
    df_arr = df[df['ActivityType'].str.lower().isin(['ankomst', 'arrival'])].copy()
    
    logger.info(f"📊 Separering: {len(df_dep):,} departures, {len(df_arr):,} arrivals")
    
    # Spara departures
    dep_success = False
    if not df_dep.empty:
        dep_path = f"raw/departures_{date_str}.parquet"
        dep_success = save_to_azure(df_dep, dep_path)
    else:
        logger.warning(f"⚠️ Inga departures för {date_str}")
    
    # Spara arrivals
    arr_success = False
    if not df_arr.empty:
        arr_path = f"raw/arrivals_{date_str}.parquet"
        arr_success = save_to_azure(df_arr, arr_path)
    else:
        logger.warning(f"⚠️ Inga arrivals för {date_str}")
    
    return dep_success, arr_success


def fetch_last_n_days(n_days: int = 3) -> dict:
    """
    Hämtar data för de senaste n dagarna (exkl. idag)
    
    Args:
        n_days: Antal dagar bakåt att hämta
    
    Returns:
        dict med status per dag: {date_str: {'success': bool, 'dep': bool, 'arr': bool}}
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    logger.section("=" * 60)
    logger.info("🚂 HÄMTAR TÅGDATA FRÅN TRAFIKVERKET")
    logger.section("=" * 60)
    logger.info(f"📅 Period: senaste {n_days} dagar (exkl. idag)")
    
    results = {}
    
    for i in range(n_days, 0, -1):  # 3, 2, 1 (exkl. 0 = idag)
        fetch_date = today - timedelta(days=i)
        date_str = fetch_date.strftime('%Y-%m-%d')
        
        try:
            # Hämta data för dagen
            df_day = fetch_day(fetch_date)
            
            # Separera och spara
            if not df_day.empty:
                dep_ok, arr_ok = separate_and_save(df_day, fetch_date)
                results[date_str] = {
                    'success': dep_ok or arr_ok,
                    'dep': dep_ok,
                    'arr': arr_ok,
                    'rows': len(df_day)
                }
            else:
                results[date_str] = {
                    'success': False,
                    'dep': False,
                    'arr': False,
                    'rows': 0
                }
                
        except Exception as e:
            logger.error(f"❌ Fel vid hämtning av {date_str}: {e}")
            results[date_str] = {
                'success': False,
                'dep': False,
                'arr': False,
                'error': str(e)
            }
    
    return results


def main():
    """Main function"""
    logger.section("\n" + "=" * 60)
    logger.info("FETCH TRAIN DATA - START")
    logger.section("=" * 60 + "\n")
    
    # Hämta senaste 3 dagarna
    results = fetch_last_n_days(n_days=3)
    
    # Sammanfattning
    logger.section("\n" + "=" * 60)
    logger.info("📊 SAMMANFATTNING")
    logger.section("=" * 60)
    
    total_success = 0
    total_failed = 0
    
    for date_str, status in results.items():
        if status['success']:
            total_success += 1
            dep_icon = "✅" if status['dep'] else "⚠️"
            arr_icon = "✅" if status['arr'] else "⚠️"
            rows = status.get('rows', 0)
            logger.info(f"{date_str}: {dep_icon} Dep | {arr_icon} Arr | {rows:,} rader")
        else:
            total_failed += 1
            error = status.get('error', 'Ingen data')
            logger.error(f"{date_str}: ❌ Misslyckades - {error}")
    
    logger.info(f"\n✅ Lyckades: {total_success}/3 dagar")
    if total_failed > 0:
        logger.warning(f"⚠️ Misslyckades: {total_failed}/3 dagar")
    
    logger.section("=" * 60)
    logger.success("✅ FETCH KLART!" if total_success > 0 else "❌ FETCH MISSLYCKADES")
    logger.section("=" * 60 + "\n")
    
    return 0 if total_success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())