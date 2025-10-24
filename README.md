````markdown
# 🚆 Tågdata-pipeline

En komplett datapipeline som hämtar, bearbetar och lagrar realtidsdata från **Trafikverkets öppna API**.  
Resultatet blir strukturerade resor (“trips”) som lagras i **Azure Data Lake** för vidare analys och maskininlärning.

---

## 🧭 Översikt

**Pipeline-flöde:**
1. Hämtar tågdata för de senaste 3 dagarna från Trafikverket  
2. Delar upp i avgångar (`departures`) och ankomster (`arrivals`)  
3. Bygger kompletta resor (första avgång → sista ankomst)  
4. Lägger till stationsnamn, län och avstånd (från `station_info.parquet`)  
5. Filtrerar bort dubbletter  
6. Sparar resultatet i **Azure Data Lake** som Parquet-filer  

---

## ⚙️ Kör hela pipelinen

```bash
python run_production_pipeline.py
````

Detta kör alla tre steg i ordning:

1. Hämtar senaste 3 dagarna från API
2. Processerar till trips
3. Kombinerar allt till en total-fil

Resultaten sparas i Azure.

```
┌─────────────────────────────────────────────┐
│  1. FETCH DATA (fetch_train_data.py)        │
│  Trafikverket API → Azure/raw/              │
│  ├─ departures_YYYYMMDD.parquet             │
│  └─ arrivals_YYYYMMDD.parquet               │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│  2. PROCESS TRIPS (process_trips.py)        │
│  Azure/raw/ → Azure/curated/                │
│  └─ trips_combined_YYYYMMDD.parquet         │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│  3. COMBINE ALL (combine_all_trips.py)      │
│  Alla trips → En total-fil                  │
│  └─ trips_combined_total.parquet            │
└─────────────────────────────────────────────┘
```

---

##  Planerade resor (för ML-förutsägelser)

1. **Hämta data:**
   `fetch_planned.py`
2. **Bearbeta data:**
   `transform_planned_to_curated.py`
3. **Spara till:**
   `Azure/curated/planned/`

**Inspektion av hämtad data kan göras med:**

* Historiska resor → `inspect_combined_total.ipynb`
* Planerade resor → `inspect_planned.ipynb`

---

## 📁 Projektstruktur

```
projekt/
├── config.py                          # Konfiguration (API-nycklar, Azure credentials)
├── logger.py                          # Logger-modul
├── fetch_train_data.py                # Steg 1: Hämta från API
├── fetch_planned.py                   # Hämta planerad data
├── process_trips.py                   # Steg 2: Bygg trips
├── combine_all_trips.py               # Steg 3: Kombinera allt
├── run_production_pipeline.py         # Huvudfil för körning
├── transform_planned_to_curated.py    # Transformering av planerad data
├── inspect_combined_total.ipynb       # Notebook för historisk data
├── inspect_planned.ipynb              # Notebook för planerad data
├── station_info.parquet               # Stationsdata
├── README.md
├── .gitignore
└── .funcignore
```

---

## 🧾 Loggning

Loggar hanteras via `logger.py` och sparas automatiskt till fil.

---

##  Datamodell

| Kategori                  | Kolumn(er)                                                      | Beskrivning                            |
| ------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| **Identifierare & datum** | `AdvertisedTrainIdent`, `TripStartDate`                         | Tåg-ID och datum                       |
| **Stationer**             | `departure_station`, `arrival_station`, `end_station_county`    | Namn och län                           |
| **Tider – avgång**        | `DepartureAdvertised`, `DepartureActual`                        | Planerad och faktisk avgång            |
| **Tider – ankomst**       | `ArrivalAdvertised`, `ArrivalActual`                            | Planerad och faktisk ankomst           |
| **Mätvärden**             | `DurationActualMinutes`, `DistanceKm`, `is_delayed`             | Restid, avstånd och försening (>3 min) |
| **Features (för ML)**     | `start_hour`, `start_day_of_month`, `start_month`, `is_weekday` | Tidsbaserade variabler                 |
| **Status & operatör**     | `Canceled`, `Operator`, `TrainOwner`, `trip_typeoftraffic`      | Status och operatör                    |
| **Avvikelser**            | `Deviation_Description`                                         | Orsak till avvikelse                   |

---

## 🧮 Hantering av saknade värden

| Scenario        | Canceled | DepartureActual | ArrivalActual | Resultat         |
| --------------- | -------- | --------------- | ------------- | ---------------- |
| Normal resa     | False    | ✅               | ✅             | ✅ Huvudfil       |
| Helt inställd   | True     | ❌               | ❌             | 📋 Canceled-fil  |
| Delvis inställd | True     | ❌               | ✅             | ✅ Fylls          |
| Delvis inställd | True     | ✅               | ❌             | ✅ Fylls          |
| Ofullständig    | False    | ❌               | ✅             | ❌ Filtreras bort |
| Ofullständig    | False    | ✅               | ❌             | ❌ Filtreras bort |

> Saknade tider (`DepartureActual`, `ArrivalActual`) fylls **endast** för inställda resor (`Canceled=True`) eftersom dessa har dokumenterad avvikelse.

---

## 🧠 ML-motivering

Pipelinen är förberedd för **prediktiv modellering**.
Exempel på användningsområden:

* Förutsäga **tågförseningar**
* Identifiera vilka resor som löper **störst risk att bli sena**
* Träna modeller baserat på features såsom tid, veckodag, operatör och sträcka

---
