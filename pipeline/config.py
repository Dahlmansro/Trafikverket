import os

# Försök läsa in .env om python-dotenv finns installerat
try:
    from dotenv import load_dotenv
    load_dotenv()  # läser .env i projektroten
except ImportError:
    pass

#TRAFIKVERKET
TRV_URL = 'https://api.trafikinfo.trafikverket.se/v2/data.json'
API_KEY = os.getenv("API_KEY")

# Hämta hemligheter från miljövariabler / .env
#AZURE
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
ACCOUNT_URL = os.getenv("ACCOUNT_URL")
CONTAINER_NAME = "data"

