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

        self._logger_instance = logging.getLogger(self.script_name)
        self._logger_instance.setLevel(logging.INFO)

        if self._logger_instance.hasHandlers():
            for handler in list(self._logger_instance.handlers):
                self._logger_instance.removeHandler(handler)

        self.log_file = self._setup_logging()

                # Stäng av verbose logging från Azure SDK
        logging.getLogger('azure').setLevel(logging.WARNING)
        logging.getLogger('azure.core').setLevel(logging.WARNING)
        logging.getLogger('azure.storage').setLevel(logging.WARNING)
        
        # Stäng av urllib3 och andra dependencies
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
    def _setup_logging(self):
        """Sätt upp logging till både konsol och fil"""
        # Skapa logs mapp relativt till där logger.py ligger
        script_dir = Path(__file__).parent
        log_dir = script_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Skapa format
        formatter = logging.Formatter('%(message)s')

        # Skapa logfilnamn med timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.data_type:
            log_file = log_dir / f"{self.script_name}_{self.data_type}_{timestamp}.log"
        else:
            log_file = log_dir / f"{self.script_name}_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self._logger_instance.addHandler(file_handler)
        
        # Stream Handler
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        self._logger_instance.addHandler(stream_handler)
        
        return log_file
    
    def info(self, message):
        """Logga info-meddelande"""
        self._logger_instance.info(message)
    
    def warning(self, message):
        """Logga varning"""
        self._logger_instance.warning(f"⚠️  {message}")
    
    # ... (upprepa för error, success, section, subsection, summary)
    
    def error(self, message):
        """Logga error"""
        self._logger_instance.error(f"❌ {message}")
    
    def success(self, message):
        """Logga success"""
        self._logger_instance.error(f"✅ {message}")
    
    def section(self, title):
        """Logga section header"""
        self._logger_instance.info(f"\n{'='*60}")
        self._logger_instance.info(title)
        self._logger_instance.info('='*60 + '\n')
    
    def subsection(self, title):
        """Logga subsection"""
        self._logger_instance.error(f"\n{title}")
    
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