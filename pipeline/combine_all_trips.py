# -*- coding: utf-8 -*-
"""
COMBINE ALL TRIPS - Slår ihop alla trips_combined_*.parquet till en total-fil
Filtrerar dubbletter baserat på (AdvertisedTrainIdent, TripStartDate)
"""

import io
import sys
from typing import List

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from config import ACCOUNT_URL, CONTAINER_NAME, STORAGE_ACCOUNT_KEY

from logger import get_logger
logger = get_logger("combine_all_trips")

CURATED_PREFIX = "curated/"
OUTPUT_FILE = "curated/trips_combined_total.parquet"


def az_client() -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_ACCOUNT_KEY)


def az_list_paths(prefix: str) -> List[str]:
    """Listar alla filer under ett prefix"""
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    out = []
    for p in fs.get_paths(path=prefix):
        if not p.is_directory:
            name = p.name.replace("\\", "/")
            if name.lower().endswith(".parquet"):
                out.append(name)
    return out


def az_read_parquet(path: str) -> pd.DataFrame:
    """Läser Parquet från Azure"""
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    fc = fs.get_file_client(path)
    data = fc.download_file().readall()
    return pd.read_parquet(io.BytesIO(data))


def az_save_parquet(df: pd.DataFrame, path: str):
    """Sparar DataFrame som Parquet till Azure"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    
    dl = az_client()
    fs = dl.get_file_system_client(file_system=CONTAINER_NAME)
    fc = fs.get_file_client(path)
    fc.upload_data(buffer.read(), overwrite=True)
    logger.info(f"✅ Sparad till Azure: {path}")


def combine_all_trips() -> pd.DataFrame:
    """
    Läser alla trips_combined_*.parquet, slår ihop och filtrerar dubbletter
    
    Returns:
        DataFrame med alla unika trips
    """
    logger.section("=" * 60)
    logger.info("🔗 COMBINE ALL TRIPS - START")
    logger.section("=" * 60 + "\n")
    
    # Hitta alla trips_combined_*.parquet filer (UTAN _canceled suffix)
    logger.info("🔍 Söker efter trips_combined_*.parquet filer...")
    all_files = az_list_paths(CURATED_PREFIX)
    
    trips_files = [
        f for f in all_files 
        if f.startswith("curated/trips_combined_") 
        and f != OUTPUT_FILE 
        and f.endswith(".parquet")
        and "_canceled" not in f  # Exkludera canceled-filer
    ]
    
    if not trips_files:
        logger.error("❌ Inga trips_combined_*.parquet filer hittades!")
        return pd.DataFrame()
    
    logger.info(f"✅ Hittade {len(trips_files)} filer:")
    for f in sorted(trips_files):
        logger.info(f"   - {f}")
    
    # Läs alla filer
    logger.info(f"\n📥 Läser {len(trips_files)} filer...")
    dfs = []
    total_rows = 0
    
    for i, file_path in enumerate(sorted(trips_files), 1):
        try:
            df = az_read_parquet(file_path)
            rows = len(df)
            total_rows += rows
            dfs.append(df)
            logger.info(f"   [{i}/{len(trips_files)}] ✅ {file_path.split('/')[-1]}: {rows:,} rader")
        except Exception as e:
            logger.error(f"   [{i}/{len(trips_files)}] ❌ Kunde inte läsa {file_path}: {e}")
    
    if not dfs:
        logger.error("❌ Inga filer kunde läsas!")
        return pd.DataFrame()
    
    # Slå ihop alla
    logger.info(f"\n🔗 Concatenerar {len(dfs)} DataFrames...")
    df_all = pd.concat(dfs, ignore_index=True)
    logger.info(f"✅ Total före filtrering: {len(df_all):,} rader")
    
    # Filtrera dubbletter
    logger.info("\n🧹 Filtrerar dubbletter på (AdvertisedTrainIdent, TripStartDate)...")
    
    # Kontrollera att nödvändiga kolumner finns
    required_cols = ["AdvertisedTrainIdent", "TripStartDate"]
    missing = [c for c in required_cols if c not in df_all.columns]
    if missing:
        logger.error(f"❌ Saknar kolumner för deduplicering: {missing}")
        return df_all
    
    before_dedup = len(df_all)
    
    # Sortera så nyaste data behålls (om samma resa finns i flera filer)
    # Prioritera rader med mer komplett data (färre NaN)
    df_all['_nan_count'] = df_all.isnull().sum(axis=1)
    df_all = df_all.sort_values('_nan_count')
    
    # Ta bort dubbletter (behåll första = minst NaN)
    df_unique = df_all.drop_duplicates(
        subset=["AdvertisedTrainIdent", "TripStartDate"],
        keep='first'
    ).copy()
    
    # Ta bort hjälpkolumn
    df_unique.drop(columns=['_nan_count'], inplace=True, errors='ignore')
    
    after_dedup = len(df_unique)
    duplicates_removed = before_dedup - after_dedup
    
    logger.info(f"✅ Efter filtrering: {after_dedup:,} rader")
    logger.info(f"🗑️  Borttagna dubbletter: {duplicates_removed:,} ({100*duplicates_removed/before_dedup:.1f}%)")
    
    # Sortera efter datum och tåg-id för bättre översikt
    if "TripStartDate" in df_unique.columns:
        df_unique = df_unique.sort_values(["TripStartDate", "AdvertisedTrainIdent"])
        logger.info("✅ Sorterad på TripStartDate och AdvertisedTrainIdent")
    
    return df_unique


def main():
    """Main function"""
    try:
        # Kombinera alla trips
        df_total = combine_all_trips()
        
        if df_total.empty:
            logger.error("\n❌ Ingen data att spara!")
            return 1
        
        # Spara total-fil
        logger.info(f"\n💾 Sparar till {OUTPUT_FILE}...")
        az_save_parquet(df_total, OUTPUT_FILE)
        
        # Statistik
        logger.section("\n" + "=" * 60)
        logger.info("📊 STATISTIK")
        logger.section("=" * 60)
        logger.info(f"Totalt antal resor: {len(df_total):,}")
        
        if "TripStartDate" in df_total.columns:
            date_range = df_total["TripStartDate"].agg(['min', 'max'])
            logger.info(f"Datumspann: {date_range['min']} till {date_range['max']}")
            
            unique_dates = df_total["TripStartDate"].nunique()
            logger.info(f"Antal unika dagar: {unique_dates}")
        
        if "AdvertisedTrainIdent" in df_total.columns:
            unique_trains = df_total["AdvertisedTrainIdent"].nunique()
            logger.info(f"Antal unika tåg-id: {unique_trains:,}")
        
        if "is_delayed" in df_total.columns:
            delayed = df_total["is_delayed"].sum()
            delayed_pct = 100 * delayed / len(df_total) if len(df_total) > 0 else 0
            logger.info(f"Antal försenade resor: {delayed:,} ({delayed_pct:.1f}%)")
        
        logger.section("=" * 60)
        logger.success("✅ COMBINE ALL TRIPS KLART!")
        logger.section("=" * 60 + "\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ Ett fel uppstod: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())