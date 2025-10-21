"""
Centraliserad logging-modul för tågdata-pipeline.
Skapar loggar både till konsol och fil.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


class PipelineLogger:
    """
    Logger-klass för tågdata-pipeline.
    Skapar strukturerade loggar med timestamps.
    """
    
    def __init__(self, script_name, data_type=None):
        """
        Initiera logger.
        
        Args:
            script_name: Namn på scriptet (t.ex. 'explore', 'feature_engineering')
            data_type: Optional - 'arrivals' eller 'departures'
        """
        self.script_name = script_name
        self.data_type = data_type
        self.log_file = self._setup_logging()
        
    def _setup_logging(self):
        """Sätt upp logging till både konsol och fil"""
        # Skapa logs mapp relativt till där logger.py ligger
        script_dir = Path(__file__).parent
        log_dir = script_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Skapa logfilnamn med timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.data_type:
            log_file = log_dir / f"{self.script_name}_{self.data_type}_{timestamp}.log"
        else:
            log_file = log_dir / f"{self.script_name}_{timestamp}.log"
        
        # Ta bort tidigare handlers om de finns
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # Konfigurera logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Stäng av verbose logging från Azure SDK
        logging.getLogger('azure').setLevel(logging.WARNING)
        logging.getLogger('azure.core').setLevel(logging.WARNING)
        logging.getLogger('azure.storage').setLevel(logging.WARNING)
        
        # Stäng av urllib3 och andra dependencies
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        return log_file
    
    def info(self, message):
        """Logga info-meddelande"""
        logging.info(message)
    
    def warning(self, message):
        """Logga varning"""
        logging.warning(f"⚠️  {message}")
    
    def error(self, message):
        """Logga error"""
        logging.error(f"❌ {message}")
    
    def success(self, message):
        """Logga success"""
        logging.info(f"✅ {message}")
    
    def section(self, title):
        """Logga section header"""
        logging.info(f"\n{'='*60}")
        logging.info(title)
        logging.info('='*60 + '\n')
    
    def subsection(self, title):
        """Logga subsection"""
        logging.info(f"\n{title}")
    
    def get_log_path(self):
        """Returnera sökväg till loggfilen"""
        return str(self.log_file)
    
    def summary(self, stats_dict):
        """
        Logga sammanfattning med statistik.
        
        Args:
            stats_dict: Dictionary med statistik, t.ex. {'Rader': 1000, 'Kolumner': 25}
        """
        self.subsection("📊 Sammanfattning:")
        for key, value in stats_dict.items():
            if isinstance(value, (int, float)):
                self.info(f"   {key}: {value:,}")
            else:
                self.info(f"   {key}: {value}")


def get_logger(script_name, data_type=None):
    """
    Factory function för att skapa logger.
    
    Args:
        script_name: Namn på scriptet
        data_type: Optional - 'arrivals' eller 'departures'
    
    Returns:
        PipelineLogger instance
    
    Example:
        logger = get_logger('explore', 'arrivals')
        logger.info("Starting process...")
        logger.success("Process complete!")
    """
    return PipelineLogger(script_name, data_type)