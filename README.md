# ASVSP — Analiza Evropskih Klimatskih Podataka

## Osnovne informacije

Projekat je izrađen za potrebe predmeta **Arhitekture Sistema Velikih Skupova Podataka** na Fakultetu Tehničkih Nauka u Novom Sadu. Sistem demonstrira kompletnu arhitekturu za obradu velikih skupova podataka kroz paketnu i streaming obradu klimatskih podataka za Evropu.

## Skupovi podataka

### Primarni (batch) — dva izvora
| Izvor | Opis | Veličina | Period |
|-------|------|----------|--------|
| **Kaggle Berkeley Earth** | Globalne temperature po gradovima (CSV) | ~509 MB | 1743–2013 |
| **Copernicus ERA5-Land** | Mesečni klimatski parametri za Evropu (NetCDF) | ~2.5 GB | 1950–2024 |

ERA5-Land parametri uključuju: temperaturu vazduha (2m), temperaturu tla, padavine, snežni pokrivač, brzinu vetra, solarnu radijaciju, evapotranspiraciju i druge.

### Streaming — AQICN Air Quality API
Podaci o kvalitetu vazduha u realnom vremenu za Beograd putem [AQICN API](https://aqicn.org/api/). Uključuje AQI indeks, PM2.5, PM10, O₃, NO₂, SO₂, CO, temperaturu, vlažnost, pritisak i brzinu vetra.

## Arhitektura sistema

```
┌─────────────────┐    ┌──────────────┐    ┌────────────────────┐
│  Kaggle CSV     │───▶│              │    │                    │
│  ERA5 NetCDF    │───▶│  HDFS        │───▶│  Apache Spark      │
└─────────────────┘    │  (raw zona)  │    │  (transformacije)  │
                       │              │    │                    │
                       │  HDFS        │◀───│                    │
                       │  (transform  │    └────────┬───────────┘
                       │   zona)      │             │
                       └──────────────┘             ▼
                                            ┌───────────────┐
                                            │  Apache Spark  │
                                            │  (analitički   │
                                            │   upiti)       │
                                            └───────┬───────┘
                                                    │
                                                    ▼
                                            ┌───────────────┐
                                            │  MongoDB       │──▶ Metabase
                                            │  (curated zona │    (vizualizacija)
                                            │   — batch)     │
                                            └───────────────┘

┌─────────────────┐    ┌──────────────┐    ┌────────────────────┐
│  AQICN API      │───▶│  Kafka       │───▶│  Spark Structured  │
│  (producer)     │    │  (Zookeeper) │    │  Streaming         │
└─────────────────┘    └──────────────┘    └────────┬───────────┘
                                                    │
                                                    ▼
                                            ┌───────────────┐
                                            │ Elasticsearch  │──▶ Kibana
                                            │ (curated zona  │    (vizualizacija)
                                            │  — streaming)  │
                                            └───────────────┘
```

### Jezero podataka — 3 zone
| Zona | Skladište | Putanja / Baza | Sadržaj |
|------|-----------|---------------|---------|
| **Sirova (raw)** | HDFS | `/data/raw/climate/` | Originalni CSV i NetCDF fajlovi |
| **Transformacija** | HDFS | `/data/transformed/climate/` | Parquet fajlovi (ERA5 + temperatures) |
| **Curated** | MongoDB / Elasticsearch | `analytical` DB / ES indeksi | Pročišćeni rezultati analitičkih i streaming upita |

### Kontejnerizovane komponente
| Komponenta | Port | Opis |
|------------|------|------|
| Apache Airflow | [localhost:8080](http://localhost:8080) | Orkestracija — DAG-ovi za ingestion, transformaciju i analitiku |
| HDFS (NameNode) | [localhost:9870](http://localhost:9870) | Distribuirani fajl sistem — raw i transform zone |
| Apache Spark | [localhost:8000](http://localhost:8000) | Klaster za paketnu obradu (master + 2 worker-a) |
| MongoDB | localhost:27017 | Baza za rezultate analitičkih upita (curated zona — batch) |
| Metabase | [localhost:3000](http://localhost:3000) | Vizualizacija batch rezultata |
| Kafka + Zookeeper | localhost:9092 / 2181 | Message broker za streaming podatke |
| Elasticsearch | [localhost:9200](http://localhost:9200) | Skladište za streaming rezultate (curated zona — streaming) |
| Kibana | [localhost:5601](http://localhost:5601) | Vizualizacija streaming rezultata |

## Pokretanje

### Preduslovi
1. Docker i Docker Compose
2. Kaggle dataset `GlobalLandTemperaturesByCity.csv` — postaviti u `Airflow/files/data/`
3. ERA5-Land NetCDF fajl — postaviti u `Airflow/files/data/climate/`
4. AQICN API ključ — postaviti u `Streaming/docker-compose.yml` kao `AQICN_TOKEN`

### Podizanje klastera
```bash
./scripts/cluster_up.sh
```
Skripta će podići sve kontejnere i postaviti potrebne konekcije. Na pitanje o restore-u Metabase baze, odgovoriti sa `y` za učitavanje postojećeg dashboard-a.

### Gašenje klastera
```bash
./scripts/cluster_down.sh
```

## Apache Airflow — DAG-ovi

Pokretati DAG-ove u sledećem redosledu:

1. **`load_raw_data`** — učitava Kaggle CSV i ERA5 NetCDF na HDFS u sirovu zonu (`/data/raw/climate/`)
2. **`transform_raw_data`** — Spark poslovi koji transformišu sirove podatke u Parquet format (`/data/transformed/climate/`)
3. **`analytical_queries`** — 10 analitičkih Spark poslova koji čitaju iz transform zone i upisuju rezultate u MongoDB (curated zona)

**Kredencijali:**
```
username: airflow
password: airflow
```

## Paketna obrada — 10 analitičkih upita

Svi upiti koriste analitičke window funkcije (LAG, LEAD, ROW_NUMBER, RANK, itd.):

| # | Upit | Opis | Window funkcije |
|---|------|------|-----------------|
| Q1 | Continental Warming Gradient | Dekadna stopa zagrevanja po širinskim pojasevima — arktička amplifikacija | LAG, FIRST, ROW_NUMBER |
| Q2 | ERA5 vs Berkeley Earth Cross-Validation | RMSE validacija ERA5 reanaliza vs stanice | PERCENT_RANK, NTILE |
| Q3 | Snow Cover Decline | Opadanje snežnog pokrivača po dekadama i regionima | LAG, AVG OVER |
| Q4 | Compound Drought Index | Trendovi indeksa suše — Mediteran vs Kontinentalna klima | LAG, SUM OVER |
| Q5 | Precipitation Regime Shift | Promena sezonske distribucije padavina | LAG, SUM OVER |
| Q6 | European Wind Energy Atlas | Gustina energije vetra po evropskim grid ćelijama | ROW_NUMBER, PERCENT_RANK |
| Q7 | Solar Radiation Trends | Trendovi solarne radijacije po klimatskim periodima | LAG, AVG OVER |
| Q8 | Urban Heat Island | UHI efekat u 30 evropskih gradova (skin temp vs ruralno) | ROW_NUMBER, LAG |
| Q9 | Climate Extremes Acceleration | Ubrzanje ekstrema (vreli/hladni/vlažni/suvi dani) | LAG, SUM OVER |
| Q10 | Surface Energy Balance | Bowen-ov odnos i evaporativna frakcija po dekadama | LAG, FIRST |

Rezultati se vizualizuju kroz **Metabase** dashboard: [localhost:3000](http://localhost:3000)

**Kredencijali za Metabase:**
```
email:    asvsp@ftn.uns.ac.rs
password: asvsp25
```

## Obrada u realnom vremenu — 5 streaming upita

| # | Upit | Tip | Windowing |
|---|------|-----|-----------|
| S1 | AQI-Climate Correlation | Stream-batch join (ERA5) | 10-min tumbling |
| S2 | Wind Stagnation Alert | Stream-only | 30-min sliding (5-min slide) |
| S3 | Precipitation Washout Effect | Stream-batch join (ERA5) | 1-hour tumbling |
| S4 | Historical Warming Context | Stream-batch double join (Berkeley Earth) | 15-min tumbling |
| S5 | Seasonal AQI Anomaly | Stream-only (rolling z-score) | 2-hour sliding (10-min slide) |

Streaming pipeline: **AQICN API → Kafka → Spark Structured Streaming → Elasticsearch → Kibana**

Dashboard za streaming rezultate: [localhost:5601](http://localhost:5601)

## Pristup alatima

| Alat | URL | Kredencijali |
|------|-----|-------------|
| Airflow | [localhost:8080](http://localhost:8080) | airflow / airflow |
| HDFS | [localhost:9870](http://localhost:9870) | — |
| Spark Master | [localhost:8000](http://localhost:8000) | — |
| Metabase | [localhost:3000](http://localhost:3000) | asvsp@ftn.uns.ac.rs / asvsp25 |
| Kibana | [localhost:5601](http://localhost:5601) | — |
| MongoDB | localhost:27017 | admin / admin |