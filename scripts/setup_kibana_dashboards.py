#!/usr/bin/env python3
"""
Kibana Dashboard Setup — Streaming Air Quality Visualizations.

Creates index patterns, visualizations, and a dashboard for
the 5 streaming queries (S1–S5) written to Elasticsearch.

Usage:
    python3 scripts/setup_kibana_dashboards.py
"""

import json
import time
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

KIBANA_URL = "http://localhost:5601"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# ═══════════════════════════════════════════════════════════════════════
# Helper
# ═══════════════════════════════════════════════════════════════════════

def kibana_api(method, path, body=None):
    """Make a Kibana API request."""
    url = f"{KIBANA_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = Request(url, data=data, headers=HEADERS, method=method)
    try:
        resp = urlopen(req, timeout=30)
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}
    except HTTPError as e:
        raw = e.read().decode("utf-8")
        print(f"  ⚠ {method} {path} → {e.code}: {raw[:200]}")
        return None
    except URLError as e:
        print(f"  ⚠ {method} {path} → {e}")
        return None


def wait_for_kibana():
    """Wait until Kibana is ready."""
    print("Waiting for Kibana...")
    for _ in range(30):
        try:
            resp = kibana_api("GET", "/api/status")
            if resp and resp.get("status", {}).get("overall", {}).get("state") in ("green", "yellow"):
                print("✓ Kibana is ready")
                return True
        except Exception:
            pass
        time.sleep(2)
    print("✗ Kibana not ready after 60s")
    return False


def wait_for_index(index_name, max_wait=120, interval=10):
    """Wait for an ES index to have at least 1 document."""
    print(f"  Waiting for index '{index_name}' to have data...")
    for i in range(max_wait // interval):
        try:
            req = Request(f"http://localhost:9200/{index_name}/_count",
                          headers={"Content-Type": "application/json"})
            resp = urlopen(req, timeout=10)
            data = json.loads(resp.read().decode())
            count = data.get("count", 0)
            if count > 0:
                print(f"  ✓ '{index_name}' has {count} docs")
                return True
            print(f"    ... {index_name}: 0 docs (waiting {interval}s)")
        except Exception:
            pass
        time.sleep(interval)
    print(f"  ⚠ '{index_name}' still empty after {max_wait}s — will create pattern anyway")
    return False


# ═══════════════════════════════════════════════════════════════════════
# Index Pattern Definitions
# ═══════════════════════════════════════════════════════════════════════

INDEX_PATTERNS = [
    {"id": "s1-aqi-climate-correlation", "title": "s1-aqi-climate-correlation", "timeFieldName": "window_start"},
    {"id": "s2-wind-stagnation",         "title": "s2-wind-stagnation",         "timeFieldName": "window_start"},
    {"id": "s3-precipitation-washout",   "title": "s3-precipitation-washout",   "timeFieldName": "window_start"},
    {"id": "s4-warming-context",         "title": "s4-warming-context",         "timeFieldName": "window_start"},
    {"id": "s5-seasonal-anomaly",        "title": "s5-seasonal-anomaly",        "timeFieldName": "window_start"},
]


# ═══════════════════════════════════════════════════════════════════════
# Reusable chart-param builders for Kibana 7.17
# Line / area charts REQUIRE every sub-field or they show "loading".
# ═══════════════════════════════════════════════════════════════════════

def _category_axis():
    return {
        "id": "CategoryAxis-1",
        "type": "category",
        "position": "bottom",
        "show": True,
        "style": {},
        "scale": {"type": "linear"},
        "labels": {"show": True, "filter": True, "truncate": 100},
        "title": {},
    }


def _value_axis(axis_id, name, position, title_text):
    return {
        "id": axis_id,
        "name": name,
        "type": "value",
        "position": position,
        "show": True,
        "style": {},
        "scale": {"type": "linear", "mode": "normal"},
        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
        "title": {"text": title_text},
    }


def _series(label, agg_id, axis_id, chart_type="line", line_width=2):
    return {
        "show": True,
        "type": chart_type,
        "mode": "normal",
        "data": {"label": label, "id": agg_id},
        "valueAxis": axis_id,
        "drawLinesBetweenPoints": True,
        "lineWidth": line_width,
        "interpolate": "linear",
        "showCircles": True,
    }


def _date_hist_agg(agg_id="2"):
    return {
        "id": agg_id, "enabled": True, "type": "date_histogram",
        "params": {
            "field": "window_start",
            "useNormalizedEsInterval": True,
            "scaleMetricValues": False,
            "interval": "auto",
            "drop_partials": False,
            "min_doc_count": 1,
            "extended_bounds": {},
        },
        "schema": "segment",
    }


def _common_line_params():
    return {
        "addTooltip": True,
        "addLegend": True,
        "legendPosition": "right",
        "times": [],
        "addTimeMarker": False,
        "labels": {},
        "thresholdLine": {
            "show": False, "value": 10, "width": 1,
            "style": "full", "color": "#E7664C",
        },
    }


def _search_source(index_pattern_id):
    return json.dumps({
        "index": index_pattern_id,
        "query": {"query": "", "language": "kuery"},
        "filter": [],
    })


def _ref(index_pattern_id):
    return [{"id": index_pattern_id,
             "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
             "type": "index-pattern"}]


# ═══════════════════════════════════════════════════════════════════════
# Visualization Definitions
# ═══════════════════════════════════════════════════════════════════════

def build_visualizations():
    visuals = []

    # ── S1: AQI vs Temperature Anomaly (line — dual axis) ────────────
    visuals.append({
        "id": "vis-s1-aqi-temp-anomaly",
        "type": "visualization",
        "attributes": {
            "title": "S1: AQI vs Temperature Anomaly (ERA5)",
            "visState": json.dumps({
                "title": "S1: AQI vs Temperature Anomaly (ERA5)",
                "type": "line",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "avg_aqi"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "temp_anomaly_vs_era5"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Avg AQI"),
                        _value_axis("ValueAxis-2", "RightAxis-1", "right", "Temp Anomaly (°C)"),
                    ],
                    "seriesParams": [
                        _series("Avg AQI", "1", "ValueAxis-1"),
                        _series("Temp Anomaly", "3", "ValueAxis-2"),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S1: Real-time AQI correlated with ERA5 historical climate anomaly",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s1-aqi-climate-correlation")},
        },
        "references": _ref("s1-aqi-climate-correlation"),
    })

    # ── S1: Wind Deficit & Z-Score (metric) ──────────────────────────
    visuals.append({
        "id": "vis-s1-wind-deficit",
        "type": "visualization",
        "attributes": {
            "title": "S1: Wind Deficit & Z-Score",
            "visState": json.dumps({
                "title": "S1: Wind Deficit & Z-Score",
                "type": "metric",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "wind_deficit"}, "schema": "metric"},
                    {"id": "2", "enabled": True, "type": "avg",
                     "params": {"field": "temp_z_score"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "event_count"}, "schema": "metric"},
                ],
                "params": {"metric": {"percentageMode": False}},
            }),
            "uiStateJSON": "{}",
            "description": "S1: Average wind deficit and temperature z-score",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s1-aqi-climate-correlation")},
        },
        "references": _ref("s1-aqi-climate-correlation"),
    })

    # ── S2: Wind Stagnation Alerts (table) ───────────────────────────
    visuals.append({
        "id": "vis-s2-stagnation-table",
        "type": "visualization",
        "attributes": {
            "title": "S2: Wind Stagnation Alerts",
            "visState": json.dumps({
                "title": "S2: Wind Stagnation Alerts",
                "type": "table",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "avg_aqi"}, "schema": "metric"},
                    {"id": "4", "enabled": True, "type": "avg",
                     "params": {"field": "avg_wind_speed"}, "schema": "metric"},
                    {"id": "2", "enabled": True, "type": "terms",
                     "params": {"field": "stagnation_severity.keyword", "size": 10,
                                "order": "desc", "orderBy": "1"},
                     "schema": "bucket"},
                    {"id": "3", "enabled": True, "type": "date_histogram",
                     "params": {"field": "window_start", "interval": "auto",
                                "min_doc_count": 1},
                     "schema": "bucket"},
                ],
                "params": {"perPage": 10, "showPartialRows": False, "showTotal": False},
            }),
            "uiStateJSON": "{}",
            "description": "S2: Stagnation alerts grouped by severity",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s2-wind-stagnation")},
        },
        "references": _ref("s2-wind-stagnation"),
    })

    # ── S2: AQI vs Wind Speed (line — dual axis) ────────────────────
    visuals.append({
        "id": "vis-s2-aqi-wind",
        "type": "visualization",
        "attributes": {
            "title": "S2: AQI vs Wind Speed Over Time",
            "visState": json.dumps({
                "title": "S2: AQI vs Wind Speed Over Time",
                "type": "line",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "avg_aqi"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "avg_wind_speed"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Avg AQI"),
                        _value_axis("ValueAxis-2", "RightAxis-1", "right", "Wind Speed (m/s)"),
                    ],
                    "seriesParams": [
                        _series("Avg AQI", "1", "ValueAxis-1"),
                        _series("Wind Speed", "3", "ValueAxis-2"),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S2: AQI inversely correlated with wind speed (30min sliding window)",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s2-wind-stagnation")},
        },
        "references": _ref("s2-wind-stagnation"),
    })

    # ── S3: Precipitation Washout (line — dual axis) ─────────────────
    visuals.append({
        "id": "vis-s3-washout-aqi",
        "type": "visualization",
        "attributes": {
            "title": "S3: Precipitation Washout — AQI Change",
            "visState": json.dumps({
                "title": "S3: Precipitation Washout — AQI Change",
                "type": "line",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "avg_aqi"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "aqi_change"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Avg AQI"),
                        _value_axis("ValueAxis-2", "RightAxis-1", "right", "AQI Change"),
                    ],
                    "seriesParams": [
                        _series("Avg AQI", "1", "ValueAxis-1"),
                        _series("AQI Change", "3", "ValueAxis-2"),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S3: Pressure-based rain detection vs AQI drop (1-hour tumbling window)",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s3-precipitation-washout")},
        },
        "references": _ref("s3-precipitation-washout"),
    })

    # ── S3: Washout Detection Counts (metric) ────────────────────────
    visuals.append({
        "id": "vis-s3-washout-metric",
        "type": "visualization",
        "attributes": {
            "title": "S3: Washout & Rain Detection Counts",
            "visState": json.dumps({
                "title": "S3: Washout & Rain Detection Counts",
                "type": "metric",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "sum",
                     "params": {"field": "event_count"}, "schema": "metric"},
                    {"id": "2", "enabled": True, "type": "count",
                     "params": {}, "schema": "metric"},
                ],
                "params": {"metric": {"percentageMode": False}},
            }),
            "uiStateJSON": "{}",
            "description": "S3: Total events and window count",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s3-precipitation-washout")},
        },
        "references": _ref("s3-precipitation-washout"),
    })

    # ── S4: Current vs Historical Temperature (line) ─────────────────
    visuals.append({
        "id": "vis-s4-warming-context",
        "type": "visualization",
        "attributes": {
            "title": "S4: Current vs Historical Temperature",
            "visState": json.dumps({
                "title": "S4: Current vs Historical Temperature",
                "type": "line",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "current_temp"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "historical_avg_temp"}, "schema": "metric"},
                    {"id": "4", "enabled": True, "type": "avg",
                     "params": {"field": "anomaly_vs_all_time"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Temperature (°C)"),
                    ],
                    "seriesParams": [
                        _series("Current Temp", "1", "ValueAxis-1"),
                        _series("Historical Avg", "3", "ValueAxis-1"),
                        _series("Anomaly", "4", "ValueAxis-1", "area", 1),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S4: Real-time temp vs Berkeley Earth historical average (15-min window)",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s4-warming-context")},
        },
        "references": _ref("s4-warming-context"),
    })

    # ── S4: Extreme Classification Pie ───────────────────────────────
    visuals.append({
        "id": "vis-s4-extreme-pie",
        "type": "visualization",
        "attributes": {
            "title": "S4: Temperature Extreme Classification",
            "visState": json.dumps({
                "title": "S4: Temperature Extreme Classification",
                "type": "pie",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count",
                     "params": {}, "schema": "metric"},
                    {"id": "2", "enabled": True, "type": "terms",
                     "params": {"field": "extreme_classification.keyword",
                                "size": 5, "order": "desc", "orderBy": "1"},
                     "schema": "segment"},
                ],
                "params": {"type": "pie", "addTooltip": True,
                           "addLegend": True, "isDonut": True},
            }),
            "uiStateJSON": "{}",
            "description": "S4: NORMAL / UNUSUAL / EXTREME classification distribution",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s4-warming-context")},
        },
        "references": _ref("s4-warming-context"),
    })

    # ── S5: AQI Anomaly Detection (line — dual axis) ────────────────
    #  NOTE: aqi_z_score is TEXT in ES (can hold NaN/Infinity) — use
    #  latest_aqi (long) + rolling_avg_aqi (float) for numeric aggs.
    visuals.append({
        "id": "vis-s5-zscore",
        "type": "visualization",
        "attributes": {
            "title": "S5: AQI Anomaly Z-Score Over Time",
            "visState": json.dumps({
                "title": "S5: AQI Anomaly Z-Score Over Time",
                "type": "line",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "latest_aqi"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "rolling_avg_aqi"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Latest AQI"),
                        _value_axis("ValueAxis-2", "RightAxis-1", "right", "Rolling Avg AQI"),
                    ],
                    "seriesParams": [
                        _series("Latest AQI", "1", "ValueAxis-1"),
                        _series("Rolling Avg AQI", "3", "ValueAxis-2"),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S5: Latest AQI vs rolling average — anomaly detection (2h window, 10-min slide)",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s5-seasonal-anomaly")},
        },
        "references": _ref("s5-seasonal-anomaly"),
    })

    # ── S5: Anomaly Direction Pie ────────────────────────────────────
    visuals.append({
        "id": "vis-s5-anomaly-pie",
        "type": "visualization",
        "attributes": {
            "title": "S5: Anomaly Direction (Spike / Drop / Normal)",
            "visState": json.dumps({
                "title": "S5: Anomaly Direction (Spike / Drop / Normal)",
                "type": "pie",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count",
                     "params": {}, "schema": "metric"},
                    {"id": "2", "enabled": True, "type": "terms",
                     "params": {"field": "anomaly_direction.keyword",
                                "size": 5, "order": "desc", "orderBy": "1"},
                     "schema": "segment"},
                ],
                "params": {"type": "pie", "addTooltip": True,
                           "addLegend": True, "isDonut": True},
            }),
            "uiStateJSON": "{}",
            "description": "S5: Distribution of AQI anomaly directions",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s5-seasonal-anomaly")},
        },
        "references": _ref("s5-seasonal-anomaly"),
    })

    # ── S5: AQI Volatility (area chart) ──────────────────────────────
    visuals.append({
        "id": "vis-s5-volatility",
        "type": "visualization",
        "attributes": {
            "title": "S5: AQI Volatility (Max - Min Range)",
            "visState": json.dumps({
                "title": "S5: AQI Volatility (Max - Min Range)",
                "type": "area",
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "params": {"field": "aqi_volatility"}, "schema": "metric"},
                    {"id": "3", "enabled": True, "type": "avg",
                     "params": {"field": "rolling_avg_aqi"}, "schema": "metric"},
                    _date_hist_agg("2"),
                ],
                "params": {
                    "type": "area",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [_category_axis()],
                    "valueAxes": [
                        _value_axis("ValueAxis-1", "LeftAxis-1", "left", "Value"),
                    ],
                    "seriesParams": [
                        _series("AQI Volatility", "1", "ValueAxis-1", "area"),
                        _series("Rolling Avg AQI", "3", "ValueAxis-1", "line", 2),
                    ],
                    **_common_line_params(),
                },
            }),
            "uiStateJSON": "{}",
            "description": "S5: AQI max-min range (volatility) in the 2-hour sliding window",
            "kibanaSavedObjectMeta": {"searchSourceJSON": _search_source("s5-seasonal-anomaly")},
        },
        "references": _ref("s5-seasonal-anomaly"),
    })

    # ── Markdown: Dashboard Description ──────────────────────────────
    visuals.append({
        "id": "vis-dashboard-info",
        "type": "visualization",
        "attributes": {
            "title": "Dashboard Info",
            "visState": json.dumps({
                "title": "Dashboard Info",
                "type": "markdown",
                "aggs": [],
                "params": {
                    "markdown": (
                        "## 🌍 Air Quality Streaming Dashboard\n\n"
                        "Real-time air quality monitoring for Belgrade with "
                        "5 streaming processors:\n\n"
                        "| Query | Type | Window | Description |\n"
                        "|-------|------|--------|-------------|\n"
                        "| **S1** | Stream-Batch Join | 10 min tumbling | AQI vs ERA5 climate anomaly |\n"
                        "| **S2** | Windowed Agg | 30 min sliding / 5 min | Wind stagnation alerts |\n"
                        "| **S3** | Stream-Batch Join | 1 hour tumbling | Precipitation washout effect |\n"
                        "| **S4** | Stream-Batch Join | 15 min tumbling | Current vs historical temp |\n"
                        "| **S5** | Rolling Z-Score | 2 hour sliding / 10 min | AQI anomaly detection |"
                    ),
                    "fontSize": 12,
                },
            }),
            "uiStateJSON": "{}",
            "description": "Dashboard overview markdown",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": [],
    })

    return visuals


# ═══════════════════════════════════════════════════════════════════════
# Dashboard definition
# ═══════════════════════════════════════════════════════════════════════

def build_dashboard(vis_ids):
    panels = []
    col_w, row_h = 24, 15
    x, y = 0, 0

    for i, vid in enumerate(vis_ids):
        if vid == "vis-dashboard-info":
            panels.append({
                "panelIndex": str(i),
                "gridData": {"x": 0, "y": 0, "w": 48, "h": 8, "i": str(i)},
                "version": "7.17.16",
                "panelRefName": f"panel_{i}",
            })
            y = 8
            continue

        panels.append({
            "panelIndex": str(i),
            "gridData": {"x": x, "y": y, "w": col_w, "h": row_h, "i": str(i)},
            "version": "7.17.16",
            "panelRefName": f"panel_{i}",
        })
        x += col_w
        if x >= 48:
            x = 0
            y += row_h

    references = [
        {"id": vid, "name": f"panel_{i}", "type": "visualization"}
        for i, vid in enumerate(vis_ids)
    ]

    return {
        "id": "streaming-air-quality-dashboard",
        "type": "dashboard",
        "attributes": {
            "title": "Air Quality Streaming Dashboard",
            "description": "Real-time air quality monitoring — 5 streaming queries (S1–S5)",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"hidePanelTitles": False, "useMargins": True}),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "refreshInterval": {"pause": False, "value": 30000},
        },
        "references": references,
    }


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  Kibana Dashboard Setup — Streaming Air Quality")
    print("=" * 60)

    if not wait_for_kibana():
        sys.exit(1)

    # 1. Create index patterns (field-aware API — auto-discovers mappings)
    print("\n── Creating Index Patterns ──")
    for ip in INDEX_PATTERNS:
        kibana_api("DELETE", f"/api/index_patterns/index_pattern/{ip['id']}")
        body = {
            "index_pattern": {
                "id": ip["id"],
                "title": ip["title"],
                "timeFieldName": ip.get("timeFieldName", ""),
            }
        }
        resp = kibana_api("POST", "/api/index_patterns/index_pattern", body)
        if resp and resp.get("index_pattern"):
            nfields = len(resp["index_pattern"].get("fields", {}))
            print(f"  ✓ {ip['title']} ({nfields} fields)")
        else:
            print(f"  ✗ Failed: {ip['title']}")

    kibana_api("POST", "/api/kibana/settings", {
        "changes": {"defaultIndex": "s2-wind-stagnation"}
    })

    # 2. Build all objects
    print("\n── Creating Visualizations & Dashboard ──")
    visuals = build_visualizations()
    vis_ids = [v["id"] for v in visuals]
    ordered = ["vis-dashboard-info"] + [v for v in vis_ids if v != "vis-dashboard-info"]
    dashboard = build_dashboard(ordered)

    bulk_objects = visuals + [dashboard]

    resp = kibana_api("POST",
                      "/api/saved_objects/_bulk_create?overwrite=true",
                      bulk_objects)
    if resp:
        saved = resp.get("saved_objects", [])
        errors = [o for o in saved if o.get("error")]
        ok = [o for o in saved if not o.get("error")]
        for o in ok:
            print(f"  ✓ {o['type']}: {o['attributes'].get('title', o['id'])}")
        for o in errors:
            print(f"  ✗ {o['id']}: {o['error']}")
        print(f"\n  Created: {len(ok)}, Errors: {len(errors)}")
    else:
        print("  ✗ Bulk import failed")

    print("\n" + "=" * 60)
    print(f"  Dashboard: {KIBANA_URL}/app/dashboards#/view/streaming-air-quality-dashboard")
    print(f"  Time range: Last 24 hours, auto-refresh: 30s")
    print("=" * 60)


if __name__ == "__main__":
    main()
