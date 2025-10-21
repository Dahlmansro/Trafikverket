# Tågdata Pipeline

---

## Översikt

Denna pipeline hämtar realtidsdata från Trafikverkets öppna API, processerar den till strukturerade resor (trips) och lagrar resultatet i Azure Data Lake.

**Vad pipelinen gör:**
1. ✅ Hämtar tågdata för senaste 3 dagarna från Trafikverket
2. ✅ Separerar departures och arrivals
3. ✅ Bygger kompletta resor (första avgång → sista ankomst)
4. ✅ Utvidgar med stationsnamn, län och avstånd (hämtar från station_info.parquet)
5. ✅ Filtrerar bort dubbletter
6. ✅ Sparar allt i Azure som Parquet-filer

---

### Kör hela pipelinen

```bash
python run_production_pipeline.py
```

Detta kör alla 3 steg i ordning:
1. Hämtar senaste 3 dagarna från API
2. Processerar till trips
3. Kombinerar till total-fil

Filerna sparas i Azure

```
┌─────────────────────────────────────────────────────────────┐
│  1. FETCH DATA (fetch_train_data.py)                        │
│  Trafikverkets API → Azure/raw/                             │
│  ├─ departures_YYYYMMDD.parquet                             │
│  └─ arrivals_YYYYMMDD.parquet                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  2. PROCESS TRIPS (process_trips.py)                        │
│  Azure/raw/ → Azure/curated/                                │
│  └─ trips_combined_YYYYMMDD.parquet                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  3. COMBINE ALL (combine_all_trips.py)                      │
│  Alla trips → En total-fil                                  │
│  └─ trips_combined_total.parquet (utan dubbletter)          │
└─────────────────────────────────────────────────────────────┘

```

### Plannerade resor, dvs morgondagens resor för ML

Punkt 1 och 2 måste köras "manuellt". 

1. Hämtas med fetch_planned.py
2. Processesas med transform_planned_to_curated.py
3. Sparas till Azure/curated/planned

```
n                                     
### Inspektera filerna
Alla historiska resor -> inspect_combined.total.ipynb
Plannerade resor -> inspect_planned.ipynb

```
projekt/
├── config.py                          # Konfiguration (API keys, Azure credentials)
├── logger.py                          # Logger-modul
├── fetch_train_data.py                # Steg 1: Hämta från API
├── fetch_planned.py                   # Hämta planerad data
├── process_trips.py                   # Steg 2: Bygg trips
├── combine_all_trips.py               # Steg 3: Kombinera allt
├── run_production_pipeline.py         # Huvudfil
├── transform_planned_to_curated.py    # Transformering av planerad data
├── inspect_combined_total.ipynb       # Jupyter notebook för inspektion
├── inspect_planned.ipynb              # Jupyter notebook för planerad data
├── __init__.py                        # Python package
├── README.md
├── .funcignore
├── .gitignore
└── station_info.parquet               # Stationsdata

```

### Loggning

Loggar sparas automatiskt till:
- Loggfil via `logger.py`
Loggnigen är inte bra, har inte gått igenom den efter det att jag gjorde om hela piplinen 
---

### Trips (efter processering)

**Identifierare & Datum:**
```
AdvertisedTrainIdent         # Tåg-ID (t.ex. "1240", "8182")
TripStartDate                # Resans datum (YYYY-MM-DD)
```

**Stationer:**
```
LocationSignatureDeparture   # Startstationskod (t.ex. "Cst", "G", "Mr")
LocationSignatureArrival     # Slutstationskod (t.ex. "U", "Hb", "Åbe")
departure_station            # Startstationsnamn (t.ex. "Stockholm Central", "Göteborg Central")
arrival_station              # Slutstationsnamn (t.ex. "Uppsala C", "Helsingborg C")
end_station_county           # Län för slutstation (t.ex. "Stockholms län", "Skåne län")
```

**Tider - Avgång:**
```
DepartureAdvertised          # Planerad avgångstid (datetime)
DepartureActual              # Faktisk avgångstid (datetime)
```

**Tider - Ankomst:**
```
ArrivalAdvertised            # Planerad ankomsttid (datetime)
ArrivalActual                # Faktisk ankomsttid (datetime)
```

**Prestanda & Försening:**

DurationActualMinutes        # Faktisk restid i minuter (från DepartureActual till ArrivalActual)
DistanceKm                   # Avstånd i kilometer (beräknat med haversine-formel)
is_delayed                   # 1 om >3 min sen, 0 annars (boolean som int)
```

**Features (för ML):**
```
start_hour                   # Avgångstimme (0-23)
start_day_of_month           # Dag i månaden (1-31)
start_month                  # Månad (1-12)
is_weekday                   # 1 om måndag-fredag, 0 om lördag-söndag
```

**Status & Operatör:**
```
Canceled                     # True om resa inställd, False annars
Operator                     # Operatör (t.ex. "SJ", "MTR", "Öresundståg")
TrainOwner                   # Tågägare (t.ex. "SJ", "SLL", "SKANE", "Ö-TÅG")
trip_typeoftraffic           # Trafiktyp kod 
```

**Avvikelser:**
```
Deviation_Description        # Beskrivning av avvikelse (t.ex. "Sent från tidigare resa", "Tekniskt fel")
```

#  Hantering av Saknade Värden

| Scenario | Canceled | DepartureActual | ArrivalActual | Resultat |
|----------|----------|-----------------|---------------|----------|
| **Normal resa** | False | ✅ Finns | ✅ Finns | ✅ Huvudfil |
| **Helt inställd** | True | ❌ Saknas | ❌ Saknas | 📋 Canceled-fil |
| **Delvis inställd** | True | ❌ Saknas | ✅ Finns | ✅ Huvudfil (fylld) |
| **Delvis inställd** | True | ✅ Finns | ❌ Saknas | ✅ Huvudfil (fylld) |
| **Ofullständig** | False | ❌ Saknas | ✅ Finns | ❌ Filtreras bort |
| **Ofullständig** | False | ✅ Finns | ❌ Saknas | ❌ Filtreras bort |

##  ML-motivering

Saknade `DepartureActual` eller `ArrivalActual` fylls **endast** för inställda resor (`Canceled=True`), eftersom vi då vet att avvikelsen är dokumenterad (True i canceled kolumnen). 



