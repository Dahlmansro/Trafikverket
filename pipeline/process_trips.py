# -*- coding: utf-8 -*-
"""
PROCESS TRIPS - Bygger resor från raw data
Läser raw parquet-filer från Azure → bygger trips → sparar till curated/
"""

import os
import io
import sys
import re
import argparse
from typing import List, Dict, Optional
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas import Timestamp

from azure.storage.filedatalake import DataLakeServiceClient
from config import ACCOUNT_URL, CONTAINER_NAME, STORAGE_ACCOUNT_KEY

from logger import get_logger
logger = get_logger("process_trips")

# Konstanter
RAW_PREFIX = "raw/"
CURATED_DIR = "curated"
STATION_INFO_PATH = RAW_PREFIX + "station_info.parquet"

DEPARTURE_TYPES = frozenset(["departure", "avgang", "avgång", "avgångar"])
ARRIVAL_TYPES = frozenset(["arrival", "ankomst", "ankomster"])


def az_client() -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY)


def az_list_paths(prefix: str) -> List[str]:
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    out = []
    for p in fs.get_paths(path=prefix):
        if not p.is_directory:
            name = p.name.replace("\\", "/")
            if name.lower().endswith(".parquet"):
                out.append(name)
    return out


def az_read_bytes(path: str) -> bytes:
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    fc = fs.get_file_client(path)
    return fc.download_file().readall()


def az_save_parquet(df: pd.DataFrame, path: str):
    """Sparar DataFrame som Parquet till Azure"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    fc = fs.get_file_client(path)
    fc.upload_data(buffer.read(), overwrite=True)


def read_parquet_azure(path: str) -> pd.DataFrame:
    data = az_read_bytes(path)
    return pd.read_parquet(io.BytesIO(data))


def to_dt_utc_naive(s: pd.Series) -> pd.Series:
    """Parse → tz-aware UTC → strip tz (naive)"""
    if s is None or len(s) == 0:
        return pd.Series([], dtype="datetime64[ns]")
    out = pd.to_datetime(s, errors="coerce", utc=True)
    try:
        out = out.dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass
    return out


def flatten_nested_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Hittar kolumner med listor/np.arrays → om list[dict]: ta första elementet"""
    df_out = df.copy()
    cols_with_lists = [
        col for col in df_out.columns
        if df_out[col].dropna().apply(lambda x: isinstance(x, (list, np.ndarray))).any()
    ]
    
    if not cols_with_lists:
        return df_out
    
    created_info = []
    cols_to_drop = []
    new_dfs = []
    
    for col in cols_with_lists:
        sample = next((x for x in df_out[col] if isinstance(x, (list, np.ndarray)) and len(x) > 0), None)
        if isinstance(sample, np.ndarray):
            sample = sample.tolist()
        
        if isinstance(sample, list) and len(sample) > 0 and isinstance(sample[0], dict):
            data_to_normalize = []
            for x in df_out[col]:
                if x is None:
                    data_to_normalize.append({})
                elif isinstance(x, float) and np.isnan(x):
                    data_to_normalize.append({})
                elif not isinstance(x, (list, np.ndarray)):
                    data_to_normalize.append({})
                else:
                    x_list = x.tolist() if isinstance(x, np.ndarray) else x
                    data_to_normalize.append(x_list[0] if len(x_list) > 0 else {})
            
            expanded = pd.json_normalize(data_to_normalize)
            expanded.columns = [f"{col}_{c}" for c in expanded.columns]
            expanded.index = df_out.index
            
            new_dfs.append(expanded)
            cols_to_drop.append(col)
            created_info.append({"from": col, "to": list(expanded.columns)})
    
    if cols_to_drop:
        df_out = df_out.drop(columns=cols_to_drop)
        if new_dfs:
            df_out = pd.concat([df_out] + new_dfs, axis=1)
    
    if created_info:
        try:
            total_new = sum(len(x["to"]) for x in created_info)
            logger.info(f"   🔧 Skapade {total_new} kolumner från {len(created_info)} nested-kolumner")
            
            if any("TypeOfTraffic" in x["from"] for x in created_info):
                for col_info in created_info:
                    if "TypeOfTraffic" in col_info["from"]:
                        code_cols = [c for c in col_info["to"] if "Code" in c]
                        if code_cols:
                            df_out["trip_typeoftraffic"] = df_out[code_cols[0]]
                            logger.info(f"   ✅ Extraherade trip_typeoftraffic från {code_cols[0]}")
        except Exception as e:
            logger.warning(f"   ⚠️ Varning vid kolumnskapande: {e}")
    
    return df_out


def assign_trip_date(df: pd.DataFrame) -> pd.DataFrame:
    df["TripDate"] = df["AdvertisedTimeAtLocation"].dt.date
    return df


def first_departure_last_arrival(group: pd.DataFrame) -> Optional[Dict]:
    """Hittar första avgång och sista ankomst för en grupp"""
    group = group.copy()
    
    if "ActivityType" in group.columns:
        group["ActivityType"] = group["ActivityType"].fillna("").astype(str)
    else:
        return None
    
    dep_mask = group["ActivityType"].str.lower().isin(DEPARTURE_TYPES)
    arr_mask = group["ActivityType"].str.lower().isin(ARRIVAL_TYPES)
    
    dep = group.loc[dep_mask].sort_values("AdvertisedTimeAtLocation").head(1)
    arr = group.loc[arr_mask].sort_values("AdvertisedTimeAtLocation").tail(1)
    
    if dep.empty or arr.empty:
        return None

    dep_row = dep.iloc[0]
    arr_row = arr.iloc[-1]

    start_planned = dep_row["AdvertisedTimeAtLocation"]
    start_actual = dep_row.get("TimeAtLocationWithSeconds")
    end_planned = arr_row["AdvertisedTimeAtLocation"]
    end_actual = arr_row.get("TimeAtLocationWithSeconds")

    canceled = bool(arr_row.get("Canceled", False)) or bool(dep_row.get("Canceled", False))
    
    # FILTRERA: Om NÅGON actual saknas OCH inte canceled → return None
    if (pd.isna(start_actual) or pd.isna(end_actual)) and not canceled:
        return None
    
    # FYLL: Saknade actual-tider för canceled resor
    if pd.isna(start_actual) and canceled:
        start_actual = start_planned
    if pd.isna(end_actual) and canceled:
        end_actual = end_planned

    # Beräkna försening i minuter
    delay_minutes = np.nan
    if pd.notna(end_actual) and pd.notna(end_planned):
        delay_minutes = (end_actual - end_planned).total_seconds() / 60.0


    start_station = dep_row.get("LocationSignature")
    end_station = arr_row.get("LocationSignature")

    operator_ = dep_row.get("Operator", group.get("Operator", pd.Series([np.nan])).iloc[0])
    train_owner = dep_row.get("TrainOwner", group.get("TrainOwner", pd.Series([np.nan])).iloc[0])
    
    typeof_traffic = np.nan
    if "trip_typeoftraffic" in dep_row.index:
        typeof_traffic = dep_row.get("trip_typeoftraffic")
    elif "trip_typeoftraffic" in group.columns:
        typeof_traffic = group["trip_typeoftraffic"].iloc[0]
    elif "TypeOfTraffic_Code" in dep_row.index:
        typeof_traffic = dep_row.get("TypeOfTraffic_Code")
    elif "TypeOfTraffic_Code" in group.columns:
        typeof_traffic = group["TypeOfTraffic_Code"].iloc[0]

    deviation_desc = None
    for col in group.columns:
        if col.lower().endswith("deviation_description"):
            deviation_desc = dep_row.get(col)
            break

    train_id = dep_row["AdvertisedTrainIdent"]
    trip_date = group["TripDate"].iloc[0]

    start_day_of_month = trip_date.day if hasattr(trip_date, 'day') else np.nan
    weekday = trip_date.weekday() if hasattr(trip_date, 'weekday') else np.nan
    is_weekday = int(weekday in [0, 1, 2, 3, 4]) if not pd.isna(weekday) else np.nan

    start_hour = int(start_planned.hour) if isinstance(start_planned, Timestamp) and pd.notna(start_planned) else np.nan
    start_month = int(start_planned.month) if isinstance(start_planned, Timestamp) and pd.notna(start_planned) else np.nan

    duration_min = np.nan
    if pd.notna(start_actual) and pd.notna(end_actual):
        duration_min = round((end_actual - start_actual).total_seconds() / 60.0)
    
    # Markera som försenad om ankomst sker senare än 5 min 59 sek (≈5.983 min)
    is_delayed = 1 if pd.notna(delay_minutes) and delay_minutes > (5 + 59/60) else 0

    return {
        "AdvertisedTrainIdent": train_id,
        "TripDate": trip_date,
        "start_planned": start_planned,
        "start_actual": start_actual,
        "start_station": start_station,
        "end_planned": end_planned,
        "end_actual": end_actual,
        "end_station": end_station,
        "trip_canceled": canceled,
        "delay_minutes": delay_minutes,
        "duration_min": duration_min,
        "Operator": operator_,
        "TrainOwner": train_owner,
        "trip_typeoftraffic": typeof_traffic,
        "Deviation_Description": deviation_desc,
        "start_hour": start_hour,
        "start_day_of_month": start_day_of_month,
        "start_month": start_month,
        "is_weekday": is_weekday,
        "is_delayed": is_delayed,
    }


def build_trips_from_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Bygger trips från raw data"""
    logger.info("   🔧 Flatten nested columns")
    df = flatten_nested_columns(df_raw)

    needed = ["AdvertisedTrainIdent", "ActivityType", "LocationSignature",
              "AdvertisedTimeAtLocation", "TimeAtLocationWithSeconds"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Saknar obligatoriska kolumner: {missing}")

    logger.info("   📅 Konverterar tidkolumner")
    df["AdvertisedTimeAtLocation"] = to_dt_utc_naive(df["AdvertisedTimeAtLocation"])
    df["TimeAtLocationWithSeconds"] = to_dt_utc_naive(df["TimeAtLocationWithSeconds"])

    logger.info("   🗓️ Tilldelar TripDate")
    df = assign_trip_date(df)

    logger.info("   🚆 Bygger resor per tåg-id och datum")
    trips_list = []
    n_groups = 0
    
    for (train_id, trip_date), group in df.groupby(["AdvertisedTrainIdent", "TripDate"]):
        n_groups += 1
        trip = first_departure_last_arrival(group)
        
        if trip is not None:
            trips_list.append(trip)
        
        if n_groups % 10000 == 0:
            logger.info(f"      Processerat {n_groups:,} grupper, {len(trips_list):,} resor skapade")

    if not trips_list:
        logger.warning("   ⚠️ Inga resor kunde skapas från rådata!")
        return pd.DataFrame()

    df_trips = pd.DataFrame(trips_list)
    logger.info(f"   ✅ Totalt {len(df_trips):,} resor skapade")
    
    return df_trips


def _normalize_code_series(s: pd.Series) -> pd.Series:
    """Normalisera signaturer"""
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", "", regex=True)
         .str.upper()
    )


def haversine_vectorized(lat1_arr, lon1_arr, lat2_arr, lon2_arr):
    """Beräkna avstånd mellan koordinater"""
    lat1 = np.radians(np.asarray(lat1_arr, dtype=float))
    lon1 = np.radians(np.asarray(lon1_arr, dtype=float))
    lat2 = np.radians(np.asarray(lat2_arr, dtype=float))
    lon2 = np.radians(np.asarray(lon2_arr, dtype=float))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return 6371.0 * c


def _ensure_latlon_from_geometry(si):
    """Extraherar lat/lon ur station_info Geometry"""
    si_copy = si.copy()
    
    if "Geometry" not in si_copy.columns:
        return si_copy, None, None
    
    def parse_point(val):
        if val is None or not isinstance(val, dict):
            return None, None
        
        wgs = val.get("WGS84", "")
        if wgs and "POINT" in str(wgs):
            m = re.search(r"POINT\s*\(\s*([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s*\)", str(wgs))
            if m:
                lon = float(m.group(1))
                lat = float(m.group(2))
                return lat, lon
        
        return None, None
    
    parsed = si_copy["Geometry"].apply(parse_point)
    si_copy["__lat"] = parsed.apply(lambda x: x[0] if x else None)
    si_copy["__lon"] = parsed.apply(lambda x: x[1] if x else None)
    
    si_copy["__lat"] = pd.to_numeric(si_copy["__lat"], errors="coerce")
    si_copy["__lon"] = pd.to_numeric(si_copy["__lon"], errors="coerce")
    
    return si_copy, "__lat", "__lon"


def enrich_with_station_info(df_trips: pd.DataFrame, station_info: pd.DataFrame) -> pd.DataFrame:
    """Enrich med stationsinfo och distans"""
    si = station_info.copy()

    code_col = next((c for c in ["LocationSignature", "PrimaryLocationCode", "Signature", "LocationCode"] if c in si.columns), None)
    if code_col is None:
        raise KeyError("station_info saknar kolumn för locationsignatur")
    
    logger.info(f"   🔍 Använder kolumn '{code_col}' från station_info för join")

    si, lat_col, lon_col = _ensure_latlon_from_geometry(si)
    si["code_norm"] = _normalize_code_series(si[code_col])

    trips = df_trips.copy()
    trips["start_station_norm"] = _normalize_code_series(trips["start_station"])
    trips["end_station_norm"] = _normalize_code_series(trips["end_station"])

    keep_cols = ["code_norm"]
    if "OfficialLocationName" in si.columns: keep_cols.append("OfficialLocationName")
    if "CountyName" in si.columns: keep_cols.append("CountyName")
    if lat_col and lat_col in si.columns: keep_cols.append(lat_col)
    if lon_col and lon_col in si.columns: keep_cols.append(lon_col)
    si_small = si[keep_cols].drop_duplicates(subset=["code_norm"]).copy()

    before_join = len(trips)
    
    # Join för departure_station
    if "OfficialLocationName" in si_small.columns:
        trips = trips.merge(
            si_small[["code_norm", "OfficialLocationName"]].rename(
                columns={"code_norm": "start_station_norm", "OfficialLocationName": "departure_station"}
            ),
            on="start_station_norm", how="left"
        )
        matched_start = trips["departure_station"].notna().sum()
        logger.info(f"   Departure-join: {matched_start}/{before_join} ({100*matched_start/before_join:.1f}%) matchade")
    else:
        trips["departure_station"] = np.nan

    # Join för arrival_station
    if "OfficialLocationName" in si_small.columns:
        trips = trips.merge(
            si_small[["code_norm", "OfficialLocationName"]].rename(
                columns={"code_norm": "end_station_norm", "OfficialLocationName": "arrival_station"}
            ),
            on="end_station_norm", how="left"
        )
        matched_end = trips["arrival_station"].notna().sum()
        logger.info(f"   Arrival-join: {matched_end}/{before_join} ({100*matched_end/before_join:.1f}%) matchade")
    else:
        trips["arrival_station"] = np.nan

    # Join county
    if "CountyName" in si_small.columns:
        trips = trips.merge(
            si_small[["code_norm", "CountyName"]].rename(
                columns={"code_norm": "end_station_norm", "CountyName": "end_station_county"}
            ),
            on="end_station_norm", how="left"
        )
    else:
        trips["end_station_county"] = np.nan

    # Distans
    if lat_col and lon_col and lat_col in si_small.columns and lon_col in si_small.columns:
        latlon = si_small[["code_norm", lat_col, lon_col]].copy()
        si_start = latlon.rename(columns={"code_norm": "start_station_norm", lat_col: "start_lat", lon_col: "start_lon"})
        si_end = latlon.rename(columns={"code_norm": "end_station_norm", lat_col: "end_lat", lon_col: "end_lon"})

        trips = trips.merge(si_start, on="start_station_norm", how="left")
        trips = trips.merge(si_end, on="end_station_norm", how="left")

        valid = (~trips["start_lat"].isna() & ~trips["start_lon"].isna() &
                 ~trips["end_lat"].isna() & ~trips["end_lon"].isna())

        dist = np.full(len(trips), np.nan, dtype=float)
        if valid.any():
            dist[valid.values] = haversine_vectorized(
                trips.loc[valid, "start_lat"].values, trips.loc[valid, "start_lon"].values,
                trips.loc[valid, "end_lat"].values, trips.loc[valid, "end_lon"].values
            )
        trips["distance_km"] = dist

        trips.drop(columns=["start_lat", "start_lon", "end_lat", "end_lon"], inplace=True, errors="ignore")
    else:
        trips["distance_km"] = np.nan

    trips.drop(columns=["start_station_norm", "end_station_norm"], inplace=True, errors="ignore")

    return trips


RENAME_BACKCOMP = {
    "end_planned": "ArrivalAdvertised",
    "end_actual": "ArrivalActual",
    "start_planned": "DepartureAdvertised",
    "start_actual": "DepartureActual",
    "delay_minutes": "DelayMinutes",
    "start_station": "LocationSignatureDeparture",
    "end_station": "LocationSignatureArrival",
    "TripDate": "TripStartDate",
    "trip_canceled": "Canceled",
    "duration_min": "DurationActualMinutes",
    "distance_km": "DistanceKm",
}

ALWAYS_KEEP = [
    "Operator", "TrainOwner", "trip_typeoftraffic",
    "Deviation_Description",
    "departure_station", "arrival_station",
    "end_station_county",
    "start_hour", "start_day_of_month", "start_month", "is_weekday", "is_delayed",
]


def process_date(date_str: str) -> bool:
    """Processar raw-filer för ett specifikt datum"""
    logger.info(f"\n📅 Processerar {date_str}")
    
    dep_file = f"raw/departures_{date_str}.parquet"
    arr_file = f"raw/arrivals_{date_str}.parquet"
    
    try:
        # Läs departures
        try:
            df_dep = read_parquet_azure(dep_file)
            logger.info(f"   ✅ Läste {dep_file}: {len(df_dep):,} rader")
        except Exception as e:
            logger.warning(f"   ⚠️ Kunde inte läsa {dep_file}: {e}")
            df_dep = pd.DataFrame()
        
        # Läs arrivals
        try:
            df_arr = read_parquet_azure(arr_file)
            logger.info(f"   ✅ Läste {arr_file}: {len(df_arr):,} rader")
        except Exception as e:
            logger.warning(f"   ⚠️ Kunde inte läsa {arr_file}: {e}")
            df_arr = pd.DataFrame()
        
        if df_dep.empty and df_arr.empty:
            logger.error(f"   ❌ Inga raw-filer hittades för {date_str}")
            return False
        
        # Concat
        dfs = [d for d in [df_dep, df_arr] if not d.empty]
        df_raw = pd.concat(dfs, ignore_index=True)
        logger.info(f"   📊 Totalt RAW: {len(df_raw):,} rader")
        
        # Bygg trips
        trips = build_trips_from_raw(df_raw)
        
        if trips.empty:
            logger.warning(f"   ⚠️ Inga trips kunde byggas för {date_str}")
            return False
        
        # Enrich med station_info
        logger.info("   🔍 Enrichar med station_info")
        try:
            si = read_parquet_azure(STATION_INFO_PATH)
            trips = enrich_with_station_info(trips, si)
        except Exception as e:
            logger.warning(f"   ⚠️ Kunde inte läsa station_info: {e}")
            trips["departure_station"] = np.nan
            trips["arrival_station"] = np.nan
            trips["end_station_county"] = np.nan
            if "distance_km" not in trips.columns:
                trips["distance_km"] = np.nan
        
        # Döp om enligt bakåtkomp
        logger.info("   🏷️ Döper om kolumner")
        trips_renamed = trips.rename(columns=RENAME_BACKCOMP)
        
        # Säkerställ att alla önskade kolumner finns
        for col in list(RENAME_BACKCOMP.values()) + ALWAYS_KEEP:
            if col not in trips_renamed.columns:
                trips_renamed[col] = np.nan
        
        # Slutlig kolumnordning
        main_cols = [
            "AdvertisedTrainIdent", "TripStartDate",
            "LocationSignatureDeparture", "LocationSignatureArrival",
            "DepartureAdvertised", "DepartureActual",
            "ArrivalAdvertised", "ArrivalActual",
            "DelayMinutes", "DurationActualMinutes", "DistanceKm",
            "Canceled",
            "Operator", "TrainOwner", "trip_typeoftraffic",
            "departure_station", "arrival_station",
            "end_station_county",
            "Deviation_Description",
            "start_hour", "start_day_of_month", "start_month", "is_weekday", "is_delayed",
        ]
        rest = [c for c in trips_renamed.columns if c not in main_cols]
        final = trips_renamed[main_cols + rest]
        
        # Filtrera i slutet
        fully_canceled_mask = (
            final["DepartureActual"].isna() & 
            final["ArrivalActual"].isna()
        )
        
        df_canceled = final[fully_canceled_mask].copy()
        df_main = final[~fully_canceled_mask].copy()
        
        logger.info(f"   📊 Separering: {len(df_main):,} huvudresor, {len(df_canceled):,} helt inställda")
        
        # Spara huvudfil
        if not df_main.empty:
            output_path = f"curated/trips_combined_{date_str}.parquet"
            az_save_parquet(df_main, output_path)
            logger.info(f"   ✅ Sparad: {output_path} ({len(df_main):,} resor)")
        
        # Spara canceled-fil
        if not df_canceled.empty:
            output_path_canceled = f"curated/trips_combined_{date_str}_canceled.parquet"
            az_save_parquet(df_canceled, output_path_canceled)
            logger.info(f"   ✅ Sparad: {output_path_canceled} ({len(df_canceled):,} helt inställda resor)")
        
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Fel vid processering av {date_str}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Processar raw-filer till trips")
    parser.add_argument("--dates", nargs="+", help="Specifika datum (YYYYMMDD)")
    parser.add_argument("--all", action="store_true", help="Processera alla raw-filer")
    args = parser.parse_args()
    
    logger.section("=" * 60)
    logger.info("🚆 PROCESS TRIPS - START")
    logger.section("=" * 60 + "\n")
    
    dates_to_process = []
    
    if args.dates:
        dates_to_process = args.dates
        logger.info(f"📋 Processerar specifika datum: {', '.join(dates_to_process)}")
    elif args.all:
        all_files = az_list_paths(RAW_PREFIX)
        dep_files = [f for f in all_files if "departures_" in f and f.endswith(".parquet")]
        dates_to_process = [f.split("_")[-1].replace(".parquet", "") for f in dep_files]
        logger.info(f"📋 Hittade {len(dates_to_process)} datum att processera")
    else:
        today = datetime.now()
        for i in range(2, 0, -1):
            date = today - timedelta(days=i)
            dates_to_process.append(date.strftime("%Y%m%d"))
        logger.info(f"📋 Processerar senaste 2 dagarna: {', '.join(dates_to_process)}")
    
    # Processera varje datum
    results = {}
    for date_str in dates_to_process:
        success = process_date(date_str)
        results[date_str] = success
    
    # Sammanfattning
    logger.section("\n" + "=" * 60)
    logger.info("📊 SAMMANFATTNING")
    logger.section("=" * 60)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for date_str, success in results.items():
        icon = "✅" if success else "❌"
        logger.info(f"{icon} {date_str}")
    
    logger.info(f"\n✅ Lyckades: {success_count}/{total_count}")
    
    logger.section("=" * 60)
    if success_count > 0:
        logger.success("✅ PROCESS TRIPS KLART!")
    else:
        logger.error("❌ PROCESS TRIPS MISSLYCKADES")
    logger.section("=" * 60 + "\n")
    
    return 0 if success_count > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
    