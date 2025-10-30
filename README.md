# 🚆 Trafikverket – Bygga och analysera tågresor

![Python](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-MIT-green)


## 🧭 Projektöversikt

Detta projekt analyserar Trafikverkets öppna tågtrafikdata med syftet att identifiera vilka faktorer som påverkar förseningar samt att bygga prediktiva modeller som kan förutsäga sannolikheten att ett tåg blir försenat. 

Projektet består av en dataprocesspipeline som körs lokalt eller i Azure, och resultatet visualiseras i en Power BI-rapport. 
Fokus ligger på punktlighet enligt **RT+5** – dvs. andelen tåg som ankommer inom 5 minuter och 59 sekunder efter planerad tid.

---

## 📋 Krav före installation

### Systemkrav
- **Python 3.12** eller senare
- **Conda** eller **venv** för virtuell miljö
- **Git** för att klona repositoryt
- **Power BI Desktop** (för att öppna visualiseringar)

### Azure-krav
- Ett aktivt **Azure-konto**
- **Azure Data Lake Storage Gen2** (för att lagra och hantera data)
- **Storage Account Key** med läs- och skrivbehörigheter

### API-åtkomst
- **Trafikverkets API-nyckel** https://api.trafikinfo.trafikverket.se/

---

## 🧩 Datakällor

- **Trafikverket Open API** – data om avgångar och ankomster (TrainAnnouncement)
- **Azure Data Lake Storage** – lagring av rå-, bearbetad och berikad data (parquet-format)
- **station_info.parquet** – metadata om stationer, län och koordinater

---

## ⚙️ Installation och konfiguration

### Skapa miljö
```bash
conda create -n trafikverket_env python=3.12 -y
conda activate trafikverket_env
```

### Installera beroenden
```bash
pip install -r requirements.txt
```

### Miljövariabler (.env)
Skapa en fil `.env` i projektroten med följande nycklar:
```
ACCOUNT_URL=https://<ditt_storagekonto>.dfs.core.windows.net
CONTAINER_NAME=trafikdata
STORAGE_ACCOUNT_KEY=<din_azure_nyckel>
TRAFIKVERKET_API_KEY=<din_trafikverket_api_nyckel>
```

## ▶️ Körning av pipeline

Pipeline-skriptet `process_trips.py` hämtar, bearbetar och sparar resdata till Azure Data Lake.

### Exempel: kör senaste två dagarna
```bash
cd pipeline
python process_trips.py
```

### Exempel: kör specifika datum
```bash
python process_trips.py --dates 20241020 20241021
```

### Exempel: kör alla tillgängliga raw-filer
```bash
python process_trips.py --all
```
### Träna och utvärdera maskininlärningsmodeller

**Öppna notebooks i `ml_modell/`:**

```bash
cd ml_modell
jupyter notebook
```

**Kör notebooks i följande ordning:**

1. **`1_0_Load_and_Clean_Data.ipynb`**  
   Laddar data från Azure och förbereder den för modellträning.

2. **`2_0_Delay_Prediction_Model.ipynb`**  
   Tränar fyra klassificeringsmodeller: Logistic Regression, Random Forest, Gradient Boosting och XGBoost.  
   Utvärderar modellerna med accuracy, precision, recall, F1-score och ROC-AUC.

3. **`3_0_Evaluate_predictions.ipynb`**  
   Validerar den bästa modellen på planerade resor och analyserar feature importance.

```

## Filstruktur

Trafikverket/
│
├── ml_modell/                                # Maskininlärningsmodeller och analys
│   ├── data/                                # Lokal datakatalog
│   ├── predictions/                         # Modellprediktioner
│   ├── validation/                          # Valideringsdata
│   ├── 1_0_Load_and_Clean_Data.ipynb        # Notebook: Dataförbearbetning
│   ├── 2_0_Delay_Prediction_Model.ipynb     # Notebook: Modellträning
│   ├── 3_0_Evaluate_predictions.ipynb       # Notebook: Modellutvärdering
│   ├── train_delay_encoder.pkl              # Tränad encoder
│   ├── train_delay_model.pkl                # Tränad modell
│   ├── train_delay_scaler.pkl               # Tränad scaler
│   └── training_feature_names.json          # Feature-namn för träning
│
├── pipeline/                                # Data-pipeline för ETL
│   ├── logs/                                # Automatiskt genererade loggar
│   │
│   ├── config.py                            # Konfiguration (API, Azure)
│   ├── logger.py                            # Centraliserad loggningsmodul
│   │
│   ├── fetch_train_data.py                  # Steg 1: Hämta historisk data
│   ├── fetch_planned.py                     # Steg 2: Hämta planerad data
│   ├── process_trips.py                     # Steg 3: Processera trips
│   ├── transform_planned_to_curated.py      # Steg 4: Transformera planerad
│   ├── combine_all_trips.py                 # Steg 5: Kombinera alla trips
│   ├── run_production_pipeline.py           # Huvudscript för pipeline
│   │
│   ├── inspect_combined_total.ipynb         # Inspektion: Historisk data
│   ├── inspect_planned.ipynb                # Inspektion: Planerad data
│   │
│   ├── station_info.parquet                 # Stationsmetadata
│   ├── requirements.txt                     # Python-beroenden
│   ├── .gitignore                           # Git ignore-regler
│   └── .funcignore                          # Azure Functions ignore
│
├── README.md                                # Huvuddokumentation
├── requirements.txt                         # Projektets dependencies
├── .gitignore                               # Git ignore för hela projektet
└── trafikverket.pbix                        # Power BI-rapport
```
### Pipeline-flöde

```
┌─────────────────────────────────────────────────────────────┐
│  1️⃣ HÄMTA DATA (fetch_train_data.py)                        │
│     Trafikverket API → Azure/raw/                           │
│     ├─ departures_YYYYMMDD.parquet                          │
│     └─ arrivals_YYYYMMDD.parquet                            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  2️⃣ HÄMTA PLANERAD (fetch_planned.py)                       │
│     Trafikverket API → Azure/raw/planned/                   │
│     ├─ arrivals_YYYY-MM-DD.json                             │
│     └─ departures_YYYY-MM-DD.json                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  3️⃣ PROCESSERA RESOR (process_trips.py)                     │
│     Azure/raw/ → Azure/curated/                             │
│     ├─ Platta ut nästlade JSON-strukturer                   │
│     ├─ Gruppera per tåg + datum                             │
│     ├─ Matcha avgångar med ankomster                        │
│     ├─ Beräkna förseningar & avstånd                        │
│     └─ trips_combined_YYYYMMDD.parquet                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  4️⃣ TRANSFORMERA PLANERAD (transform_planned_to_curated.py)│
│     Azure/raw/planned/ → Azure/curated/planned/             │
│     └─ planned_trips_YYYY-MM-DD.parquet                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  5️⃣ KOMBINERA ALLA (combine_all_trips.py)                   │
│     Slå ihop alla resor → En huvudfil                       │
│     ├─ Deduplicera på (TrainIdent, Date)                    │
│     └─ trips_combined_total.parquet                         │
└─────────────────────────────────────────────────────────────┘
```

## 🔄 Dataprocess och skapande av resor

Resorna skapas automatiskt utifrån Trafikverkets öppna trafikdata. Projektet är uppbyggt som en tydlig pipeline som hämtar, bearbetar och lagrar data i Azure Data Lake innan den visualiseras i Power BI.

```
RAW-data (ankomster + avgångar)
       ↓
Läs in Parquet-filer från Azure Data Lake
   → raw/departures_YYYYMMDD.parquet
   → raw/arrivals_YYYYMMDD.parquet
       ↓
Platta ut nästlade kolumner (FromLocation, ToLocation, TypeOfTraffic)
Konvertera tidkolumner till UTC-format
Tilldela datum (TripDate)
       ↓
Gruppera per tåg och datum
   → AdvertisedTrainIdent + TripDate
       ↓
För varje grupp:
   → Identifiera första avgången  (ActivityType = departure)
   → Identifiera sista ankomsten  (ActivityType = arrival)
       ↓
Beräkna:
   → Försening i minuter
   → Restid, operatör, typ av trafik, veckodag, månad, timme
       ↓
Berika med stationsdata (station_info.parquet)
   → Namn, län, koordinater, avstånd i km (haversine)
       ↓
Spara resultatet till Azure Data Lake (curated/)
   → trips_combined_YYYYMMDD.parquet
   → trips_combined_YYYYMMDD_canceled.parquet
```

### Definition av is_delayed
En resa markeras som försenad (`is_delayed = 1`) om tåget anländer **mer än 5 minuter och 59 sekunder (RT+5)** efter planerad ankomsttid. Annars markeras resan som punktlig (`is_delayed = 0`).

---

## 📊 Power BI-visualisering

Nyckeltal och statistik över det färdiga datamaterialet visualiseras i en **Power BI-rapport** som även finns publicerad i projektets Git-repo. Rapporten visar bland annat:
- Punktlighet (RT+5)
- Förseningar per operatör och län
- Tidsmönster (dygn, veckodag, månad)
- Geografisk spridning av förseningar

För att visa rapporten:
1. Öppna `trafikverket.pbix` i Power BI Desktop
2. Kontrollera att datakällan pekar mot rätt mapp (`curated/`)
3. Uppdatera data med **Refresh**

---

## 🤖 Maskininlärning

Den bearbetade datan används som underlag för maskininlärningsmodeller som tränas för att förutsäga sannolikheten att ett tåg blir försenat. Modellerna testas med flera olika algoritmer, bland annat:
- **Logistic Regression**
- **Random Forest**
- **Gradient Boosting**
- **XGBoost**

Varje modell utvärderas med mått som *accuracy*, *precision*, *recall*, *F1-score* och *ROC-AUC* för att bedöma hur väl de identifierar faktiska förseningar. Resultaten jämförs sedan för att välja den modell som ger bäst balans mellan precision och generaliseringsförmåga.

**Resultat:**
- XGBoost uppnådde bäst prestanda med 81% accuracy och 0.53 recall på historisk testdata
- Vid validering på planerade resor sjönk recall till 0.18, vilket visar begränsningar i generaliseringsförmågan
- Analysen bekräftar att längre distanser, dagtid och vardagar ökar förseningsrisken

---

## 👥 Projektteam

**Camilla Dahlman**, **Karl Tengström**, **Gustav Jeansson**  
Data Scientists (EC Utbildning)  

För kontakt, se respektive LinkedIn-profil.

---

Detta projekt använder Trafikverkets öppna data enligt gällande användarvillkor.