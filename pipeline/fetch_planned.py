# -*- coding: utf-8 -*-
"""
FETCH PLANNED DATA - Hämtar morgondagens planerade tåg från Trafikverkets API
Sparar till Azure Data Lake: raw/planned/arrivals_YYYY-MM-DD.json, raw/planned/departures_YYYY-MM-DD.json
"""

import json
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from azure.storage.filedatalake import DataLakeServiceClient
from config import API_KEY, TRV_URL, ACCOUNT_URL, CONTAINER_NAME, STORAGE_ACCOUNT_KEY

from logger import get_logger
logger = get_logger("fetch_planned")


RAW_PLANNED_DIR = "raw/planned"


def to_utc_z(dt_local):
    """Konvertera lokal tid till UTC ISO-format"""
    dt_utc = dt_local.astimezone(ZoneInfo("UTC"))
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def tomorrow_interval_iso():
    """Returnerar morgondagens tidsintervall i UTC och ISO-datum"""
    tz = ZoneInfo("Europe/Stockholm")
    now_local = datetime.now(tz)
    start_local = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return to_utc_z(start_local), to_utc_z(end_local), start_local.date().isoformat()


def build_xml_query(activity_type: str, start_iso: str, end_iso: str) -> str:
    """Bygger XML-query för Trafikverkets API"""
    return f"""<REQUEST>
  <LOGIN authenticationkey="{API_KEY}" />
  <QUERY objecttype="TrainAnnouncement" schemaversion="1.9">
    <FILTER>
      <AND>
        <EQ name="ActivityType" value="{activity_type}"/>
        <GT name="AdvertisedTimeAtLocation" value="{start_iso}"/>
        <LT name="AdvertisedTimeAtLocation" value="{end_iso}"/>
      </AND>
    </FILTER>
    <INCLUDE>ActivityId</INCLUDE>
    <INCLUDE>AdvertisedTrainIdent</INCLUDE>
    <INCLUDE>AdvertisedTimeAtLocation</INCLUDE>
    <INCLUDE>LocationSignature</INCLUDE>
    <INCLUDE>FromLocation</INCLUDE>
    <INCLUDE>ToLocation</INCLUDE>
    <INCLUDE>Operator</INCLUDE>
    <INCLUDE>InformationOwner</INCLUDE>
    <INCLUDE>TrainOwner</INCLUDE>
    <INCLUDE>Canceled</INCLUDE>
    <INCLUDE>TypeOfTraffic</INCLUDE>
    <INCLUDE>Deviation</INCLUDE>
  </QUERY>
</REQUEST>"""


def fetch_planned(activity_type: str, start_iso: str, end_iso: str) -> dict:
    """Hämtar planerad data från Trafikverkets API"""
    xml = build_xml_query(activity_type, start_iso, end_iso)
    headers = {"Content-Type": "text/xml", "Accept": "application/json"}
    
    logger.info(f"   Hämtar {activity_type} för {start_iso} → {end_iso}")
    
    try:
        r = requests.post(TRV_URL, data=xml.encode("utf-8"), headers=headers, timeout=60)
        r.raise_for_status()
        
        payload = r.json()
        records = payload.get("RESPONSE", {}).get("RESULT", [])
        count = len(records[0].get("TrainAnnouncement", [])) if records else 0
        logger.info(f"   ✅ {count} poster hämtade")
        
        return payload
        
    except Exception as e:
        logger.error(f"   ❌ Fel vid hämtning: {e}")
        raise


def get_azure_client():
    """Returnerar Azure Data Lake Service Client"""
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY)


def ensure_directory(fs_client, path: str):
    """Säkerställer att mapp finns"""
    parts = path.strip("/").split("/")
    if len(parts) <= 1:
        return
    
    current = ""
    for p in parts[:-1]:
        current = f"{current}/{p}" if current else p
        try:
            fs_client.get_directory_client(current).create_directory()
        except Exception:
            pass  # Mappen finns redan


def save_json(fs_client, path: str, payload: dict):
    """Sparar JSON till Azure"""
    ensure_directory(fs_client, path)
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    
    file_client = fs_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    
    logger.info(f"   ✅ Sparad till Azure: {path}")


def main():
    """Main function"""
    logger.section("=" * 60)
    logger.info("🚂 HÄMTA PLANERAD DATA - START")
    logger.section("=" * 60 + "\n")
    
    try:
        # Hämta morgondagens tidsintervall
        start_iso, end_iso, date_str = tomorrow_interval_iso()
        
        logger.info(f"📅 Hämtar planerad data för: {date_str}")
        logger.info(f"   Tidsintervall (UTC): {start_iso} → {end_iso}\n")
        
        # Azure-klient
        service = get_azure_client()
        fs = service.get_file_system_client(CONTAINER_NAME)
        
        # Filnamn
        arrivals_path = f"{RAW_PLANNED_DIR}/arrivals_{date_str}.json"
        departures_path = f"{RAW_PLANNED_DIR}/departures_{date_str}.json"
        
        # Hämta och spara ARRIVALS
        logger.info("📥 Hämtar Arrivals...")
        arrivals_json = fetch_planned("Ankomst", start_iso, end_iso)
        save_json(fs, arrivals_path, arrivals_json)
        
        # Hämta och spara DEPARTURES
        logger.info("\n📥 Hämtar Departures...")
        departures_json = fetch_planned("Avgang", start_iso, end_iso)
        save_json(fs, departures_path, departures_json)
        
        # Sammanfattning
        logger.section("\n" + "=" * 60)
        logger.info("📊 SAMMANFATTNING")
        logger.section("=" * 60)
        logger.info(f"✅ Arrivals:   {arrivals_path}")
        logger.info(f"✅ Departures: {departures_path}")
        logger.info(f"📅 Datum:      {date_str}")
        logger.section("=" * 60)
        logger.success("✅ FETCH PLANNED KLART!")
        logger.section("=" * 60 + "\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ Ett fel uppstod: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())