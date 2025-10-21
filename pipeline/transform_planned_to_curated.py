# -*- coding: utf-8 -*-
"""
TRANSFORM PLANNED TO CURATED - Transformerar planerad data till resor

Läser:
  - raw/planned/arrivals_YYYY-MM-DD.json
  - raw/planned/departures_YYYY-MM-DD.json
  - raw/station_info.parquet (för avståndsberäkning)

Sparar:
  - curated/planned/trips_planned_YYYY-MM-DD.parquet
  - curated/planned/trips_planned_latest.parquet
"""

import json
import sys
from datetime import datetime, timedelta
from io import BytesIO
from math import radians, sin, cos, sqrt, atan2
from typing import List, Optional, Tuple, Dict, Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

from config import ACCOUNT_URL, CONTAINER_NAME, STORAGE_ACCOUNT_KEY
from logger import get_logger

logger = get_logger("transform_planned")

# Hårdkodade paths
RAW_PLANNED_DIR = "raw/planned"
CURATED_PLANNED_DIR = "curated/planned"
STATION_INFO_PATH = "raw/station_info.parquet"


# =========================
# Azure-funktioner
# =========================

def _adls_client() -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY)


def _adls_read_text(path: str) -> str:
    """Läser textfil från Azure"""
    fs = _adls_client().get_file_system_client(CONTAINER_NAME)
    fc = fs.get_file_client(path)
    return fc.download_file().readall().decode("utf-8")


def _adls_list(dir_path: str) -> List[Tuple[str, datetime]]:
    """Listar filer i mapp"""
    fs = _adls_client().get_file_system_client(CONTAINER_NAME)
    items = []
    try:
        for p in fs.get_paths(path=dir_path):
            if not p.is_directory:
                items.append((p.name, p.last_modified))
    except Exception:
        pass
    return items


def _adls_read_parquet(path: str) -> Optional[pd.DataFrame]:
    """Läser Parquet från Azure"""
    try:
        fs = _adls_client().get_file_system_client(CONTAINER_NAME)
        fc = fs.get_file_client(path)
        data = fc.download_file().readall()
        return pd.read_parquet(BytesIO(data))
    except Exception as e:
        logger.warning(f"   ⚠️ Kunde inte läsa {path}: {e}")
        return None


def _adls_save_parquet(df: pd.DataFrame, path: str):
    """Sparar DataFrame som Parquet till Azure"""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)
    
    fs = _adls_client().get_file_system_client(CONTAINER_NAME)
    
    # Skapa mapp om den inte finns
    dir_path = "/".join(path.split("/")[:-1])
    try:
        fs.get_directory_client(dir_path).create_directory()
    except Exception:
        pass
    
    # Spara fil
    file_client = fs.get_file_client(path)
    file_client.upload_data(buffer.read(), overwrite=True)
    logger.info(f"    Sparad: {path}")


# =========================
# Datum och tid
# =========================

def _tomorrow_local_iso() -> str:
    """Returnerar morgondagens datum i ISO-format"""
    tz = ZoneInfo("Europe/Stockholm")
    d = datetime.now(tz).date() + timedelta(days=1)
    return d.isoformat()


def _as_utc_naive(ts: pd.Series) -> pd.Series:
    """Konvertera till UTC-naive datetime"""
    return pd.to_datetime(ts, errors="coerce", utc=True).dt.tz_localize(None)


def _midnight_from_date(d: pd.Series) -> pd.Series:
    """Konvertera datum till midnatt (UTC-naive)"""
    return pd.to_datetime(d.astype("datetime64[ns]").dt.date)


# =========================
# JSON-parsning
# =========================

def _extract_list_of_dict_str(values, key: str) -> Optional[str]:
    """
    Extraherar unika strängar från lista av dicts/strängar
    Returnerar kommaseparerad sträng eller None
    """
    if values is None or (isinstance(values, float) and np.isnan(values)):
        return None
    try:
        items = []
        if isinstance(values, list):
            for v in values:
                if isinstance(v, dict):
                    val = v.get(key)
                else:
                    val = v
                if val is None:
                    continue
                s = str(val).strip()
                if s and s not in items:
                    items.append(s)
        elif isinstance(values, dict):
            val = values.get(key)
            if val is not None:
                items = [str(val).strip()]
        else:
            s = str(values).strip()
            if s:
                items = [s]
        
        return ",".join(items) if items else None
    except Exception:
        return None


def _normalize_df(raw_list: List[Dict[str, Any]], source_tag: str) -> pd.DataFrame:
    """Normaliserar JSON-data till DataFrame"""
    if not raw_list:
        return pd.DataFrame(columns=[
            "ActivityId", "AdvertisedTrainIdent", "AdvertisedTimeAtLocation",
            "LocationSignature", "FromLocations", "ToLocations",
            "Operator", "TrainOwner", "Canceled", "TypeOfTraffic", "Deviation",
            "source"
        ])
    
    df = pd.json_normalize(raw_list)
    
    # Behåll relevanta fält
    keep = {
        "ActivityId", "AdvertisedTrainIdent", "AdvertisedTimeAtLocation",
        "LocationSignature", "FromLocation", "ToLocation",
        "Operator", "TrainOwner", "Canceled", "TypeOfTraffic", "Deviation"
    }
    existing = [c for c in df.columns if c in keep]
    df = df[existing].copy()
    
    # Rename
    df.rename(columns={
        "FromLocation": "FromLocations_raw",
        "ToLocation": "ToLocations_raw",
    }, inplace=True)
    
    # Trim strings
    for col in ["ActivityId", "AdvertisedTrainIdent", "LocationSignature",
                "Operator", "TrainOwner"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    
    # Uppercase LocationSignature
    if "LocationSignature" in df.columns:
        df["LocationSignature"] = df["LocationSignature"].str.upper()
    
    # Extrahera From/To locations
    if "FromLocations_raw" in df.columns:
        df["FromLocations"] = df["FromLocations_raw"].apply(
            lambda x: _extract_list_of_dict_str(x, "LocationName")
        )
    else:
        df["FromLocations"] = None
    
    if "ToLocations_raw" in df.columns:
        df["ToLocations"] = df["ToLocations_raw"].apply(
            lambda x: _extract_list_of_dict_str(x, "LocationName")
        )
    else:
        df["ToLocations"] = None
    
    # Deviation och TypeOfTraffic
    if "Deviation" in df.columns:
        df["Deviation"] = df["Deviation"].apply(
            lambda x: _extract_list_of_dict_str(x, "Description")
        )
    else:
        df["Deviation"] = None
    
    if "TypeOfTraffic" in df.columns:
        df["TypeOfTraffic"] = df["TypeOfTraffic"].apply(
            lambda x: _extract_list_of_dict_str(x, "Text")
        )
    else:
        df["TypeOfTraffic"] = None
    
    # Tider → UTC-naive
    df["AdvertisedTimeAtLocation"] = _as_utc_naive(df.get("AdvertisedTimeAtLocation"))
    
    # Canceled → int8
    df["Canceled"] = df.get("Canceled").fillna(False).astype(bool).astype("int8")
    
    # Lägg till källflagga
    df["source"] = source_tag
    
    return df[[
        "ActivityId", "AdvertisedTrainIdent", "AdvertisedTimeAtLocation",
        "LocationSignature", "FromLocations", "ToLocations",
        "Operator", "TrainOwner", "Canceled", "TypeOfTraffic", "Deviation",
        "source"
    ]]


# =========================
# Läs planned-filer
# =========================

def _find_planned_file(prefix: str) -> Optional[str]:
    """Försök hitta morgondagens fil, annars senaste"""
    tomorrow = _tomorrow_local_iso()
    expected = f"{RAW_PLANNED_DIR}/{prefix}_{tomorrow}.json"
    
    all_files = _adls_list(RAW_PLANNED_DIR)
    
    # Direktträff?
    for name, _ in all_files:
        if name == expected:
            return name
    
    # Fallback: senaste med prefix
    candidates = [
        (n, ts) for (n, ts) in all_files 
        if f"/{prefix}_" in n and n.endswith(".json")
    ]
    
    if not candidates:
        return None
    
    candidates.sort(key=lambda x: x[1])
    return candidates[-1][0]


def _read_planned_dataframe(prefix: str, source_tag: str) -> pd.DataFrame:
    """Läser och normaliserar planned-fil"""
    path = _find_planned_file(prefix)
    
    if path is None:
        logger.warning(f"   ⚠️ Hittade ingen fil för '{prefix}'")
        return pd.DataFrame(columns=[
            "ActivityId", "AdvertisedTrainIdent", "AdvertisedTimeAtLocation",
            "LocationSignature", "FromLocations", "ToLocations",
            "Operator", "TrainOwner", "Canceled", "TypeOfTraffic", "Deviation",
            "source"
        ])
    
    text = _adls_read_text(path)
    
    try:
        blob = json.loads(text)
        arr = blob["RESPONSE"]["RESULT"][0].get("TrainAnnouncement", [])
    except Exception as e:
        logger.warning(f"   ⚠️ Kunde inte parsa JSON: {e}")
        arr = []
    
    df = _normalize_df(arr, source_tag=source_tag)
    logger.info(f"    Inläst {prefix}: {len(df)} rader från {path}")
    
    return df


# =========================
# Deduplikering
# =========================

def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Ta bort dubbletter"""
    if df.empty:
        return df
    
    df = df.sort_values("AdvertisedTimeAtLocation", ascending=True).copy()
    before = len(df)
    
    df = df.drop_duplicates(subset=["ActivityId"], keep="first")
    after1 = len(df)
    
    df = df.drop_duplicates(
        subset=["AdvertisedTrainIdent", "LocationSignature", "AdvertisedTimeAtLocation"], 
        keep="first"
    )
    after2 = len(df)
    
    logger.info(f"   🧹 Deduplikering: {before} → {after1} (ActivityId) → {after2} (Train/Loc/Time)")
    
    return df


# =========================
# Avståndsberäkning
# =========================

def _parse_point_wgs84(geom) -> Tuple[Optional[float], Optional[float]]:
    """Parsar WGS84 POINT från Geometry"""
    try:
        w = geom.get("WGS84") if isinstance(geom, dict) else None
        if not w or "POINT" not in w:
            return None, None
        coords = w.replace("POINT (", "").replace(")", "").strip().split()
        lon, lat = float(coords[0]), float(coords[1])
        return lat, lon
    except Exception:
        return None, None


def _haversine_km(lat1, lon1, lat2, lon2) -> Optional[float]:
    """Beräknar avståndet mellan två koordinater (km)"""
    if any(pd.isna([lat1, lon1, lat2, lon2])):
        return None
    
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dl/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return 6371.0 * c


def _add_distance_km(df: pd.DataFrame) -> pd.DataFrame:
    """Lägger till distance_km baserat på stationskoordinater"""
    if df.empty:
        return df
    
    stations = _adls_read_parquet(STATION_INFO_PATH)
    
    if stations is None or stations.empty:
        logger.warning("   ⚠️ Kunde inte läsa station_info - distance_km blir NaN")
        df = df.copy()
        df["distance_km"] = np.nan
        return df
    
    # Normalisera LocationSignature
    if "LocationSignature" in stations.columns:
        stations["LocationSignature"] = (
            stations["LocationSignature"].astype(str).str.upper().str.strip()
        )
    else:
        logger.warning("   ⚠️ station_info saknar LocationSignature")
        df = df.copy()
        df["distance_km"] = np.nan
        return df
    
    # Bygg lookup: 'CST' -> (lat, lon)
    latlon_map: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    
    if "Geometry" in stations.columns:
        for _, row in stations.iterrows():
            sig = row.get("LocationSignature")
            geom = row.get("Geometry")
            if pd.isna(sig) or geom is None:
                continue
            lat, lon = _parse_point_wgs84(geom)
            latlon_map[str(sig).upper().strip()] = (lat, lon)
    
    def get_latlon(sig: Any) -> Tuple[Optional[float], Optional[float]]:
        if pd.isna(sig):
            return (None, None)
        return latlon_map.get(str(sig).upper().strip(), (None, None))
    
    lat1, lon1 = zip(*df["start_station"].map(get_latlon))
    lat2, lon2 = zip(*df["end_station"].map(get_latlon))
    
    distances = []
    for a, b, c, d in zip(lat1, lon1, lat2, lon2):
        km = _haversine_km(a, b, c, d)
        distances.append(round(km, 3) if km is not None else np.nan)
    
    df = df.copy()
    df["distance_km"] = distances
    
    known_ratio = (~pd.isna(df["distance_km"])).mean()
    logger.info(f"    distance_km beräknad för {known_ratio:.1%} av resorna")
    
    return df


# =========================
# Bygg resor
# =========================

def _mode_or_first(series: pd.Series):
    """Returnerar mode eller första värdet"""
    s = series.dropna()
    if s.empty:
        return None
    try:
        return s.mode(dropna=True).iloc[0]
    except Exception:
        return s.iloc[0]


def _build_trips(df_all: pd.DataFrame) -> pd.DataFrame:
    """Bygger resor från arrivals + departures"""
    if df_all.empty:
        return pd.DataFrame()
    
    df_all = df_all.copy()
    df_all["date_only"] = df_all["AdvertisedTimeAtLocation"].dt.date
    
    trips = []
    
    for (train_id, service_date), grp in df_all.groupby(
        ["AdvertisedTrainIdent", "date_only"], dropna=False
    ):
        grp = grp.sort_values("AdvertisedTimeAtLocation")
        
        # Start: prioritera departures
        dep_rows = grp[grp["source"] == "departures"]
        arr_rows = grp[grp["source"] == "arrivals"]
        
        start_row = dep_rows.iloc[0] if not dep_rows.empty else grp.iloc[0]
        end_row = arr_rows.iloc[-1] if not arr_rows.empty else grp.iloc[-1]
        
        start_time = start_row["AdvertisedTimeAtLocation"]
        end_time = end_row["AdvertisedTimeAtLocation"]
        
        # Stationer
        start_station = start_row.get("LocationSignature")
        end_station = end_row.get("LocationSignature")
        
        if pd.isna(start_station):
            fl = start_row.get("FromLocations")
            start_station = (
                fl.split(",")[0].strip() if isinstance(fl, str) and fl else "<UNKNOWN>"
            )
        
        if pd.isna(end_station):
            tl = end_row.get("ToLocations")
            end_station = (
                tl.split(",")[0].strip() if isinstance(tl, str) and tl else "<UNKNOWN>"
            )
        
        # Duration
        duration_minutes = np.nan
        if pd.notna(start_time) and pd.notna(end_time):
            duration_minutes = round((end_time - start_time).total_seconds() / 60.0, 2)
        
        # Through stations och stops
        path_stations = grp["LocationSignature"].dropna().astype(str).tolist()
        seen = []
        for s in path_stations:
            if s not in seen:
                seen.append(s)
        through_stations = ",".join(seen) if seen else None
        stops_count = len(seen)
        
        # Canceled
        any_canceled = int(grp["Canceled"].astype(int).sum() > 0)
        
        # Metadata
        operator_mode = _mode_or_first(grp["Operator"])
        trainowner_mode = _mode_or_first(grp["TrainOwner"])
        
        start_operator = start_row.get("Operator")
        start_owner = start_row.get("TrainOwner")
        start_typeoftraffic = start_row.get("TypeOfTraffic")
        start_deviation = start_row.get("Deviation")
        
        # Tidsderivat
        start_hour = int(start_time.hour) if pd.notna(start_time) else None
        start_weekday_ = int(start_time.weekday()) if pd.notna(start_time) else None
        start_mounth = int(start_time.month) if pd.notna(start_time) else None
        is_weekday = (
            int(start_weekday_ in (0, 1, 2, 3, 4)) 
            if start_weekday_ is not None else None
        )
        
        # Datumfält
        tripdate = pd.Timestamp(start_time.date()) if pd.notna(start_time) else pd.NaT
        service_date_dt = tripdate
        
        trips.append({
            "AdvertisedTrainIdent": train_id,
            "service_date": service_date_dt,
            "Tripdate": tripdate,
            "start_planned": start_time,
            "end_planned": end_time,
            "duration_minutes": duration_minutes,
            "start_station": start_station,
            "end_station": end_station,
            "stops_count": stops_count,
            "through_stations": through_stations,
            "any_canceled": np.int8(any_canceled),
            "Operator": operator_mode,
            "TrainOwner": trainowner_mode,
            "FromLocations": _mode_or_first(grp["FromLocations"]),
            "ToLocations": _mode_or_first(grp["ToLocations"]),
            "start_operator": start_operator,
            "start_owner": start_owner,
            "start_typeoftraffic": start_typeoftraffic,
            "start_deviation": start_deviation,
            "start_hour": start_hour,
            "start_weekday_": start_weekday_,
            "start_mounth": start_mounth,
            "is_weekday": is_weekday,
            "distance_km": np.nan,
        })
    
    out = pd.DataFrame(trips)
    
    # Normalisera tidstyper
    for col in ["start_planned", "end_planned"]:
        out[col] = _as_utc_naive(out[col])
    for col in ["service_date", "Tripdate"]:
        out[col] = _midnight_from_date(out[col])
    
    out["any_canceled"] = out["any_canceled"].astype("Int8")
    
    return out


# =========================
# Huvudflöde
# =========================

def main():
    """Main function"""
    logger.section("=" * 60)
    logger.info(" TRANSFORM PLANNED TO CURATED - START")
    logger.section("=" * 60 + "\n")
    
    try:
        # 1) Läs planned-filer
        logger.info(" Läser planned-filer...")
        df_arr = _read_planned_dataframe("arrivals", source_tag="arrivals")
        df_dep = _read_planned_dataframe("departures", source_tag="departures")
        
        logger.info(f"\n Råposter: arrivals={len(df_arr)}, departures={len(df_dep)}")
        
        # 2) Deduplikera
        logger.info("\n🧹 Deduplikerar...")
        df_arr = _deduplicate(df_arr)
        df_dep = _deduplicate(df_dep)
        
        # 3) Slå ihop och bygg resor
        logger.info("\n Bygger resor...")
        df_all = pd.concat([df_arr, df_dep], ignore_index=True, sort=False)
        trips = _build_trips(df_all)
        
        if trips.empty:
            logger.warning(" Inga resor kunde byggas!")
            return 1
        
        logger.info(f"    {len(trips)} resor skapade")
        
        # 4) Lägg till avståndsberäkning
        logger.info("\n Beräknar avstånd...")
        trips = _add_distance_km(trips)
        
        # 5) Filtrera bort orimliga durationer
        before = len(trips)
        trips = trips.query("duration_minutes >= 0 and duration_minutes <= 1500").copy()
        after = len(trips)
        
        if before != after:
            logger.info(f"   🧹 Filtrerade bort {before - after} resor med orimlig varaktighet")
        
        # 6) Statistik
        logger.section("\n" + "=" * 60)
        logger.info(" STATISTIK")
        logger.section("=" * 60)
        logger.info(f"Antal resor: {len(trips):,}")
        
        if "any_canceled" in trips.columns:
            canceled_share = trips["any_canceled"].fillna(0).astype(int).mean()
            logger.info(f"Andel med any_canceled=1: {canceled_share:.1%}")
        
        if "duration_minutes" in trips.columns:
            logger.info(f"Duration (min): min={trips['duration_minutes'].min():.1f}, "
                       f"median={trips['duration_minutes'].median():.1f}, "
                       f"max={trips['duration_minutes'].max():.1f}")
        
        # Topp 10 relationer
        top_rel = (
            trips.assign(rel=trips["start_station"].astype(str) + "→" + trips["end_station"].astype(str))
                 .groupby("rel").size().sort_values(ascending=False).head(10)
        )
        logger.info("\nTopp 10 start→slut:")
        for rel, count in top_rel.items():
            logger.info(f"   {rel}: {count}")
        
        # 7) Spara till Azure
        logger.section("\n" + "=" * 60)
        logger.info(" SPARAR TILL AZURE")
        logger.section("=" * 60)
        
        # Datum för filnamn (morgondagens datum)
        date_str = _tomorrow_local_iso()
        
        # Filnamn
        file_dated = f"{CURATED_PLANNED_DIR}/trips_planned_{date_str}.parquet"
        file_latest = f"{CURATED_PLANNED_DIR}/trips_planned_latest.parquet"
        
        # Spara daterad version
        _adls_save_parquet(trips, file_dated)
        
        # Spara latest-version
        _adls_save_parquet(trips, file_latest)
        
        logger.section("=" * 60)
        logger.success(" TRANSFORM PLANNED KLART!")
        logger.section("=" * 60 + "\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n Ett fel uppstod: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())