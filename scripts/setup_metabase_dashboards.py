#!/usr/bin/env python3
"""
Automatically set up Metabase dashboards for climate analytical query results.
Creates saved questions with visualizations and organizes them into a dashboard.
"""
import requests
import json
import sys
import time

BASE = "http://localhost:3000/api"
DB_ID = 2  # MongoDB

def login():
    r = requests.post(f"{BASE}/session", json={
        "username": "asvsp@ftn.uns.ac.rs", "password": "asvsp25"
    })
    r.raise_for_status()
    return r.json()["id"]

def api(method, path, session, **kwargs):
    headers = {"X-Metabase-Session": session, "Content-Type": "application/json"}
    r = getattr(requests, method)(f"{BASE}{path}", headers=headers, **kwargs)
    if r.status_code >= 400:
        print(f"  API ERROR {r.status_code}: {r.text[:200]}")
    return r

def get_tables(session):
    """Get table name→id and field mappings."""
    r = api("get", f"/database/{DB_ID}/metadata?include_hidden=true", session)
    tables = {}
    for t in r.json().get("tables", []):
        fields = {f["name"]: f["id"] for f in t.get("fields", [])}
        tables[t["name"]] = {"id": t["id"], "fields": fields}
    return tables

def create_native_question(session, name, description, collection_name, query, viz_type, viz_settings, collection_id=None):
    """Create a saved question using a native MongoDB query."""
    payload = {
        "name": name,
        "description": description,
        "dataset_query": {
            "type": "native",
            "native": {"query": query, "collection": collection_name, "template-tags": {}},
            "database": DB_ID,
        },
        "display": viz_type,
        "visualization_settings": viz_settings,
    }
    if collection_id:
        payload["collection_id"] = collection_id
    r = api("post", "/card", session, json=payload)
    if r.status_code < 300:
        card_id = r.json()["id"]
        print(f"  ✓ Created question '{name}' (id={card_id})")
        return card_id
    return None

def create_collection(session, name, description=""):
    """Create a collection (folder) in Metabase."""
    r = api("post", "/collection", session, json={
        "name": name, "description": description, "color": "#509EE3"
    })
    if r.status_code < 300:
        cid = r.json()["id"]
        print(f"  ✓ Created collection '{name}' (id={cid})")
        return cid
    return None

def create_dashboard(session, name, description, collection_id=None):
    payload = {"name": name, "description": description}
    if collection_id:
        payload["collection_id"] = collection_id
    r = api("post", "/dashboard", session, json=payload)
    if r.status_code < 300:
        did = r.json()["id"]
        print(f"  ✓ Created dashboard '{name}' (id={did})")
        return did
    return None

def add_card_to_dashboard(session, dashboard_id, card_id, row, col, size_x, size_y):
    r = api("post", f"/dashboard/{dashboard_id}", session, json={
        "dashcards": [{"id": -1, "card_id": card_id,
                       "row": row, "col": col,
                       "size_x": size_x, "size_y": size_y}]
    })
    return r.status_code < 300

def add_text_to_dashboard(session, dashboard_id, text, row, col, size_x=18, size_y=2):
    """Add a text card to a dashboard."""
    r = api("post", f"/dashboard/{dashboard_id}", session, json={
        "dashcards": [{"id": -1, "card_id": None,
                       "row": row, "col": col,
                       "size_x": size_x, "size_y": size_y,
                       "visualization_settings": {
                           "virtual_card": {
                               "name": None, "display": "text",
                               "dataset_query": {}, "archived": False
                           },
                           "text": text
                       }}]
    })
    return r.status_code < 300

def build_dashboard_cards(card_ids):
    """Build dashcard layout: 2 columns, each card 9 wide, 8 tall."""
    cards = []
    for i, cid in enumerate(card_ids):
        if cid is None:
            continue
        row = (i // 2) * 8
        col = (i % 2) * 9
        cards.append({
            "id": -(i + 1),
            "card_id": cid,
            "row": row,
            "col": col,
            "size_x": 9,
            "size_y": 8,
        })
    return cards


def main():
    print("=" * 60)
    print("Metabase Dashboard Setup for Climate Analysis")
    print("=" * 60)

    session = login()
    print(f"✓ Logged in")

    tables = get_tables(session)
    print(f"✓ Found {len(tables)} collections\n")

    # Create collection
    print("Creating collection...")
    coll_id = create_collection(session, "Climate Analysis",
                                "Analytical query results from ERA5 + Berkeley Earth climate data")

    # ── Define questions ─────────────────────────────────────────────
    print("\nCreating saved questions with visualizations...")

    questions = []

    # Q1: Warming Gradient — line chart by decade per lat_band
    questions.append(create_native_question(
        session,
        "Q1: Continental Warming Gradient",
        "Decadal warming rate (°C/decade) per latitude band — Arctic amplification across Europe",
        "q1_warming_gradient",
        json.dumps([
            {"$project": {
                "decade": 1, "lat_band": 1,
                "decadal_avg_temp": 1, "warming_rate_per_decade": 1,
                "cumulative_warming": 1
            }},
            {"$sort": {"lat_band": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Avg Temperature (°C)",
            "graph.dimensions": ["decade", "lat_band"],
            "graph.metrics": ["decadal_avg_temp"],
        },
        coll_id
    ))

    # Q1b: Cumulative warming per lat_band
    questions.append(create_native_question(
        session,
        "Q1b: Cumulative Warming by Latitude",
        "Total cumulative warming since 1950s per latitude band",
        "q1_warming_gradient",
        json.dumps([
            {"$project": {
                "decade": 1, "lat_band": 1,
                "cumulative_warming": 1
            }},
            {"$sort": {"lat_band": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Cumulative Warming (°C)",
            "graph.dimensions": ["decade", "lat_band"],
            "graph.metrics": ["cumulative_warming"],
        },
        coll_id
    ))

    # Q2: Cross-validation — scatter of cities by RMSE
    questions.append(create_native_question(
        session,
        "Q2: ERA5 vs Berkeley Earth — City Accuracy",
        "RMSE of ERA5 reanalysis vs Berkeley Earth station observations per city",
        "q2_cross_validation",
        json.dumps([
            {"$project": {
                "city": 1, "country": 1,
                "rmse": 1, "mae": 1, "pearson_r": 1,
                "mean_bias": 1, "accuracy_category": 1,
                "n_observations": 1
            }},
            {"$sort": {"rmse": 1}}
        ]),
        "bar",
        {
            "graph.x_axis.title_text": "City",
            "graph.y_axis.title_text": "RMSE (°C)",
            "graph.dimensions": ["city"],
            "graph.metrics": ["rmse"],
        },
        coll_id
    ))

    # Q3: Snow decline — grouped bar chart
    questions.append(create_native_question(
        session,
        "Q3: Snow Cover Decline",
        "Winter snow cover fraction and snow depth by decade and region",
        "q3_snow_decline",
        json.dumps([
            {"$project": {
                "decade": 1, "region": 1,
                "avg_snow_fraction": 1, "avg_snow_depth": 1,
                "avg_winter_temp": 1
            }},
            {"$sort": {"region": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Snow Fraction",
            "graph.dimensions": ["decade", "region"],
            "graph.metrics": ["avg_snow_fraction"],
        },
        coll_id
    ))

    # Q4: Drought index trends
    questions.append(create_native_question(
        session,
        "Q4: Drought Severity — Mediterranean vs Continental",
        "Compound drought index trends per climate zone",
        "q4_drought_index",
        json.dumps([
            {"$project": {
                "climate_zone": 1, "decade": 1,
                "avg_drought_index": 1, "drought_frequency": 1,
                "worst_drought_severity": 1
            }},
            {"$sort": {"climate_zone": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Drought Frequency",
            "graph.dimensions": ["decade", "climate_zone"],
            "graph.metrics": ["drought_frequency"],
        },
        coll_id
    ))

    # Q5: Precipitation regime shift
    questions.append(create_native_question(
        session,
        "Q5: Precipitation Regime Shift",
        "Seasonal precipitation share shift: early period vs late period",
        "q5_precip_regime",
        json.dumps([
            {"$project": {
                "lat_band": 1, "season": 1,
                "early_share_pct": 1, "late_share_pct": 1,
                "share_shift_pct": 1, "precip_change_mm": 1
            }},
            {"$sort": {"lat_band": 1, "season": 1}}
        ]),
        "bar",
        {
            "graph.x_axis.title_text": "Latitude Band / Season",
            "graph.y_axis.title_text": "Share Shift (%)",
            "graph.dimensions": ["lat_band", "season"],
            "graph.metrics": ["share_shift_pct"],
        },
        coll_id
    ))

    # Q5b: Precipitation decadal trends
    questions.append(create_native_question(
        session,
        "Q5b: Precipitation Decadal Trends",
        "Seasonal precipitation share trends per decade per latitude band",
        "q5_precip_decadal",
        json.dumps([
            {"$match": {"season": "Summer"}},
            {"$project": {
                "decade": 1, "lat_band": 1,
                "avg_precip_mm": 1, "avg_share_pct": 1,
                "avg_soil_moisture": 1
            }},
            {"$sort": {"lat_band": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Summer Precipitation (mm)",
            "graph.dimensions": ["decade", "lat_band"],
            "graph.metrics": ["avg_precip_mm"],
        },
        coll_id
    ))

    # Q6: Wind energy atlas — map-like scatter
    questions.append(create_native_question(
        session,
        "Q6: European Wind Energy Atlas",
        "Wind power density across European grid cells — ranked by potential",
        "q6_wind_atlas",
        json.dumps([
            {"$project": {
                "latitude": 1, "longitude": 1,
                "mean_wind_speed": 1, "mean_wpd": 1,
                "wpd_rank": 1, "reliability_class": 1,
                "coefficient_of_variation": 1
            }},
            {"$sort": {"wpd_rank": 1}},
            {"$limit": 50}
        ]),
        "bar",
        {
            "graph.x_axis.title_text": "WPD Rank",
            "graph.y_axis.title_text": "Mean Wind Power Density (W/m²)",
            "graph.dimensions": ["wpd_rank"],
            "graph.metrics": ["mean_wpd"],
        },
        coll_id
    ))

    # Q7: Solar radiation trends
    questions.append(create_native_question(
        session,
        "Q7: Solar Radiation Trends",
        "Surface solar radiation by climate period and latitude band",
        "q7_solar_radiation",
        json.dumps([
            {"$project": {
                "lat_band": 1, "climate_period": 1,
                "period_avg_ssrd": 1, "period_avg_ssr": 1,
                "period_avg_absorptivity": 1
            }},
            {"$sort": {"lat_band": 1, "climate_period": 1}}
        ]),
        "bar",
        {
            "graph.x_axis.title_text": "Latitude Band",
            "graph.y_axis.title_text": "Solar Radiation (W/m²)",
            "graph.dimensions": ["lat_band", "climate_period"],
            "graph.metrics": ["period_avg_ssrd"],
        },
        coll_id
    ))

    # Q8: Urban Heat Island
    questions.append(create_native_question(
        session,
        "Q8: Urban Heat Island — 30 European Cities",
        "Summer UHI intensity per city per decade — skin temperature vs rural surroundings",
        "q8_urban_heat_island",
        json.dumps([
            {"$project": {
                "city_name": 1, "decade": 1,
                "avg_summer_uhi": 1, "avg_skin_air_diff": 1,
                "max_uhi": 1, "uhi_rank": 1
            }},
            {"$sort": {"decade": 1, "uhi_rank": 1}},
            {"$limit": 100}
        ]),
        "bar",
        {
            "graph.x_axis.title_text": "City",
            "graph.y_axis.title_text": "UHI Intensity (°C)",
            "graph.dimensions": ["city_name", "decade"],
            "graph.metrics": ["avg_summer_uhi"],
        },
        coll_id
    ))

    # Q9: Climate extremes acceleration
    questions.append(create_native_question(
        session,
        "Q9: Climate Extremes Acceleration",
        "Hot and cold extreme event frequency per decade per latitude band",
        "q9_climate_extremes",
        json.dumps([
            {"$project": {
                "decade": 1, "lat_band": 1,
                "hot_extreme_freq": 1, "cold_extreme_freq": 1,
                "wet_extreme_freq": 1, "dry_extreme_freq": 1
            }},
            {"$sort": {"lat_band": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Extreme Event Frequency",
            "graph.dimensions": ["decade", "lat_band"],
            "graph.metrics": ["hot_extreme_freq"],
        },
        coll_id
    ))

    # Q10: Energy balance
    questions.append(create_native_question(
        session,
        "Q10: Surface Energy Balance Trends",
        "Summer Bowen ratio and evaporative fraction trends per latitude band",
        "q10_energy_balance",
        json.dumps([
            {"$project": {
                "decade": 1, "lat_band": 1,
                "avg_net_rad": 1, "avg_bowen_ratio": 1,
                "avg_evap_fraction": 1, "avg_summer_temp": 1
            }},
            {"$sort": {"lat_band": 1, "decade": 1}}
        ]),
        "line",
        {
            "graph.x_axis.title_text": "Decade",
            "graph.y_axis.title_text": "Bowen Ratio",
            "graph.dimensions": ["decade", "lat_band"],
            "graph.metrics": ["avg_bowen_ratio"],
        },
        coll_id
    ))

    valid_questions = [q for q in questions if q is not None]
    print(f"\n✓ Created {len(valid_questions)} questions")

    # ── Create Dashboard ─────────────────────────────────────────────
    print("\nCreating dashboard...")
    dash_id = create_dashboard(
        session,
        "European Climate Analysis Dashboard",
        "Comprehensive analysis of 75 years of European climate data (ERA5 + Berkeley Earth)",
        coll_id
    )

    if dash_id and valid_questions:
        # Build card layout: 2 columns of cards
        dashcards = build_dashboard_cards(valid_questions)
        # PUT the full dashboard with all cards at once
        r = api("put", f"/dashboard/{dash_id}", session, json={
            "dashcards": dashcards
        })
        if r.status_code < 300:
            print(f"  ✓ Added {len(dashcards)} cards to dashboard")
        else:
            print(f"  ✗ Failed to add cards")

    print(f"""
{'=' * 60}
✓ Metabase setup complete!

  Dashboard: http://localhost:3000/dashboard/{dash_id}
  Collection: http://localhost:3000/collection/{coll_id}

  Login:
    Email:    asvsp@ftn.uns.ac.rs
    Password: asvsp25
{'=' * 60}
""")


if __name__ == "__main__":
    main()
