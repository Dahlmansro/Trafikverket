# -*- coding: utf-8 -*-
"""
PRODUCTION PIPELINE ORCHESTRATOR

Kör hela datapipelinen:
1. Hämta ny data från Trafikverkets API → raw/
2. Processera raw → curated/trips_combined_YYYYMMDD.parquet
3. Kombinera alla trips → curated/trips_combined_total.parquet

Kan köras manuellt eller schemaläggas.
"""

import sys
import argparse
from datetime import datetime
import traceback

from logger import get_logger

# Importera pipeline-steg
import fetch_train_data
import process_trips
import combine_all_trips

logger = get_logger("production_pipeline")


def run_step(step_name: str, step_func, *args, **kwargs) -> bool:
    """
    Kör ett pipeline-steg med error handling
    
    Args:
        step_name: Namn på steget (för logging)
        step_func: Funktion att köra
        *args, **kwargs: Argument till funktionen
    
    Returns:
        True om lyckades, False annars
    """
    logger.section("=" * 60)
    logger.info(f"🚀 STEG: {step_name}")
    logger.section("=" * 60 + "\n")
    
    try:
        result = step_func(*args, **kwargs)
        
        # Hantera olika return-typer
        if result is None or result == 0 or result is True:
            logger.success(f"\n✅ {step_name} SLUTFÖRT")
            return True
        else:
            logger.error(f"\n❌ {step_name} MISSLYCKADES (returnerade {result})")
            return False
            
    except KeyboardInterrupt:
        logger.warning(f"\n⚠️ {step_name} AVBRUTET av användare")
        raise
        
    except Exception as e:
        logger.error(f"\n❌ {step_name} MISSLYCKADES")
        logger.error(f"Fel: {str(e)}")
        logger.error("Traceback:\n" + traceback.format_exc())
        return False


def main():
    """Main orchestrator"""
    parser = argparse.ArgumentParser(
        description="Kör production pipeline för tågdata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exempel:
  python run_production_pipeline.py                    # Kör hela pipelinen
  python run_production_pipeline.py --skip-fetch       # Hoppa över API-hämtning
  python run_production_pipeline.py --only process     # Kör bara processering
  python run_production_pipeline.py --only combine     # Kör bara kombination
        """
    )
    
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Hoppa över API-hämtning (steg 1)"
    )
    parser.add_argument(
        "--skip-process",
        action="store_true",
        help="Hoppa över processering (steg 2)"
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help="Hoppa över kombination (steg 3)"
    )
    parser.add_argument(
        "--only",
        choices=["fetch", "process", "combine"],
        help="Kör endast ett specifikt steg"
    )
    parser.add_argument(
        "--process-dates",
        nargs="+",
        help="Specifika datum att processera (YYYYMMDD), används med --only process"
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help="Processera alla tillgängliga raw-filer, används med --only process"
    )
    
    args = parser.parse_args()
    
    # Banner
    logger.section("\n" + "=" * 60)
    logger.section("🚂 TÅGDATA PRODUCTION PIPELINE")
    logger.section("=" * 60)
    logger.info(f"Starttid: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if args.only:
        logger.info(f"Mode: Kör endast {args.only.upper()}\n")
    else:
        logger.info("Mode: Full pipeline (3 steg)\n")
    
    # Håll koll på vad som lyckats
    results = {
        "fetch": None,
        "process": None,
        "combine": None
    }
    
    start_time = datetime.now()
    
    try:
        # ========== STEG 1: FETCH ==========
        if args.only == "fetch" or (not args.only and not args.skip_fetch):
            results["fetch"] = run_step(
                "1/3 - HÄMTA DATA FRÅN API",
                fetch_train_data.main
            )
            
            if not results["fetch"] and not args.only:
                logger.error("\n❌ Fetch misslyckades - avbryter pipeline")
                raise Exception("Fetch-steget misslyckades")
        
        elif not args.only:
            logger.info("ℹ️ Steg 1 (Fetch) hoppades över\n")
        
        # Om vi bara ska köra fetch, avsluta här
        if args.only == "fetch":
            return 0 if results["fetch"] else 1
        
        # ========== STEG 2: PROCESS ==========
        if args.only == "process" or (not args.only and not args.skip_process):
            # Sätt upp argument för process_trips
            process_args = []
            
            if args.process_dates:
                process_args.extend(["--dates"] + args.process_dates)
            elif args.process_all:
                process_args.append("--all")
            
            # Temporärt sätt sys.argv för process_trips.main()
            old_argv = sys.argv
            sys.argv = ["process_trips.py"] + process_args
            
            try:
                results["process"] = run_step(
                    "2/3 - PROCESSERA RAW → TRIPS",
                    process_trips.main
                )
            finally:
                sys.argv = old_argv
            
            if not results["process"] and not args.only:
                logger.error("\n❌ Process misslyckades - avbryter pipeline")
                raise Exception("Process-steget misslyckades")
        
        elif not args.only:
            logger.info("ℹ️ Steg 2 (Process) hoppades över\n")
        
        # Om vi bara ska köra process, avsluta här
        if args.only == "process":
            return 0 if results["process"] else 1
        
        # ========== STEG 3: COMBINE ==========
        if args.only == "combine" or (not args.only and not args.skip_combine):
            results["combine"] = run_step(
                "3/3 - KOMBINERA ALLA TRIPS",
                combine_all_trips.main
            )
            
            if not results["combine"] and not args.only:
                logger.warning("\n⚠️ Combine misslyckades men pipeline fortsätter")
        
        elif not args.only:
            logger.info("ℹ️ Steg 3 (Combine) hoppades över\n")
        
        # Om vi bara ska köra combine, avsluta här
        if args.only == "combine":
            return 0 if results["combine"] else 1
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️ PIPELINE AVBRUTEN AV ANVÄNDARE")
        return 1
        
    except Exception as e:
        logger.error(f"\n\n❌ PIPELINE AVBRÖTS PÅ GRUND AV FEL: {str(e)}")
        return 1
    
    # ========== SLUTSAMMANFATTNING ==========
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.section("\n" + "=" * 60)
    logger.info("📊 PIPELINE SAMMANFATTNING")
    logger.section("=" * 60)
    
    # Visa resultat per steg
    steps_status = []
    
    if results["fetch"] is not None:
        icon = "✅" if results["fetch"] else "❌"
        status = "LYCKADES" if results["fetch"] else "MISSLYCKADES"
        logger.info(f"{icon} Steg 1 (Fetch):   {status}")
        steps_status.append(results["fetch"])
    
    if results["process"] is not None:
        icon = "✅" if results["process"] else "❌"
        status = "LYCKADES" if results["process"] else "MISSLYCKADES"
        logger.info(f"{icon} Steg 2 (Process): {status}")
        steps_status.append(results["process"])
    
    if results["combine"] is not None:
        icon = "✅" if results["combine"] else "❌"
        status = "LYCKADES" if results["combine"] else "MISSLYCKADES"
        logger.info(f"{icon} Steg 3 (Combine): {status}")
        steps_status.append(results["combine"])
    
    # Total status
    logger.info(f"\nTid: {duration.total_seconds():.1f} sekunder")
    logger.info(f"Sluttid: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    logger.section("=" * 60)
    
    # Avgör om hela pipelinen lyckades
    if all(steps_status):
        logger.success("✅ HELA PIPELINE SLUTFÖRD FRAMGÅNGSRIKT!")
        logger.section("=" * 60 + "\n")
        return 0
    elif any(steps_status):
        logger.warning("⚠️ PIPELINE SLUTFÖRD MED VARNINGAR")
        logger.section("=" * 60 + "\n")
        return 0
    else:
        logger.error("❌ PIPELINE MISSLYCKADES")
        logger.section("=" * 60 + "\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())