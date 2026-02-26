#!/usr/bin/env python3
"""
Generate Architecture Diagram for European Climate & AQI Analysis Project
Matches the class reference layout (single unified flow).
"""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

fig, ax = plt.subplots(1, 1, figsize=(18, 10))
ax.set_xlim(0, 18)
ax.set_ylim(0, 10)
ax.axis('off')

# Color scheme (matches reference)
C_SRC = '#F5E6C8'       # source circles
C_ING = '#C8DBC8'       # ingestion (python)
C_BATCH = '#C8DBC8'     # batch scripts/DAGs (green)
C_MSG = '#B8CBB8'       # kafka/streaming (darker green)
C_PROC = '#A8C8A8'      # processing header (green)
C_HDFS1 = '#9BB8C8'     # HDFS raw (light blue)
C_HDFS2 = '#7BA0B8'     # HDFS transform (medium blue)
C_CUR = '#5A8898'        # curated zone (teal)
C_VIZ = '#C8B8D8'       # visualization (purple)
C_INFRA = '#D8A898'      # docker (red/salmon)
C_ORCH = '#D8B8B0'       # orchestration (pink)

def box(x, y, w, h, color, lines):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08",
                       edgecolor='#666', facecolor=color, linewidth=1.5)
    ax.add_patch(p)
    cx = x + w / 2
    n = len(lines)
    total_h = n * 0.32
    start_y = y + h / 2 + total_h / 2 - 0.16
    for i, (txt, fs, fw) in enumerate(lines):
        ax.text(cx, start_y - i * 0.32, txt, ha='center', va='center',
                fontsize=fs, fontweight=fw, color='#333')

def circle(cx, cy, r, color, lines):
    c = plt.Circle((cx, cy), r, facecolor=color, edgecolor='#666', linewidth=1.5)
    ax.add_patch(c)
    n = len(lines)
    for i, (txt, fs, fw) in enumerate(lines):
        offset = (n / 2 - i - 0.5) * 0.28
        ax.text(cx, cy + offset, txt, ha='center', va='center',
                fontsize=fs, fontweight=fw, color='#333')

def arrow(x1, y1, x2, y2, color='#555', lw=1.8, rad=0.0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle='->', color=color,
                                 lw=lw, connectionstyle=f"arc3,rad={rad}"))

def biarrow(x1, y1, x2, y2, color='#555', lw=1.8):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle='<->', color=color,
                                 lw=lw, connectionstyle="arc3,rad=0"))

# ═══════════════════════════════════════════════════════════
# TITLE
# ═══════════════════════════════════════════════════════════
ax.text(9, 9.4, 'Implementacioni detalji', ha='center', va='center',
        fontsize=22, fontweight='normal', color='#444', style='italic')

# ═══════════════════════════════════════════════════════════
# SOURCES (circles, left)
# ═══════════════════════════════════════════════════════════
circle(0.9, 6.2, 0.65, C_SRC, [
    ('SOURCE', 9, 'bold'),
    ('I', 9, 'bold'),
])
circle(0.9, 3.8, 0.65, C_SRC, [
    ('SOURCE', 9, 'bold'),
    ('II', 9, 'bold'),
])

# Source labels
ax.text(0.9, 7.15, 'Kaggle CSV +', ha='center', fontsize=7, color='#666')
ax.text(0.9, 6.95, 'ERA5 NetCDF', ha='center', fontsize=7, color='#666')
ax.text(0.9, 3.0, 'AQICN API', ha='center', fontsize=7, color='#666')

# ═══════════════════════════════════════════════════════════
# PYTHON INGESTION (center-left)
# ═══════════════════════════════════════════════════════════
box(2.0, 4.2, 1.4, 2.5, C_ING, [
    ('PYTHON', 10, 'bold'),
    ('/', 9, 'normal'),
    ('AIRFLOW', 10, 'bold'),
])

# ═══════════════════════════════════════════════════════════
# BATCH PATH (top) — Airflow DAGs
# ═══════════════════════════════════════════════════════════
box(4.0, 5.8, 2.8, 1.8, C_BATCH, [
    ('AIRFLOW DAGs', 10, 'bold'),
    ('load_raw_data', 8, 'normal'),
    ('transform_raw_data', 8, 'normal'),
    ('analytical_queries', 8, 'normal'),
])

# ═══════════════════════════════════════════════════════════
# STREAMING PATH (bottom) — Kafka → Spark Streaming
# ═══════════════════════════════════════════════════════════
box(4.0, 3.3, 1.3, 1.8, C_MSG, [
    ('KAFKA', 10, 'bold'),
])

box(5.5, 3.3, 1.3, 1.8, C_MSG, [
    ('S.S.', 10, 'bold'),
    ('STREAMING', 9, 'bold'),
    ('(Spark)', 7, 'normal'),
])

# ═══════════════════════════════════════════════════════════
# PROCESSING HEADER — Hadoop / Spark
# ═══════════════════════════════════════════════════════════
box(7.5, 7.8, 4.8, 0.8, C_PROC, [
    ('HADOOP / SPARK', 11, 'bold'),
])

# ═══════════════════════════════════════════════════════════
# HDFS ZONES (two boxes side by side)
# ═══════════════════════════════════════════════════════════
box(7.5, 3.8, 1.6, 3.5, C_HDFS1, [
    ('HDFS', 12, 'bold'),
    ('', 6, 'normal'),
    ('Raw', 10, 'normal'),
    ('Zone', 10, 'normal'),
])

box(9.3, 3.8, 1.6, 3.5, C_HDFS2, [
    ('HDFS', 12, 'bold'),
    ('', 6, 'normal'),
    ('Transform', 10, 'normal'),
    ('Zone', 10, 'normal'),
])

# ═══════════════════════════════════════════════════════════
# CURATED ZONE — MongoDB + Elasticsearch
# ═══════════════════════════════════════════════════════════
box(11.2, 3.8, 1.8, 3.5, C_CUR, [
    ('MongoDB', 10, 'bold'),
    ('/', 9, 'normal'),
    ('Elastic-', 10, 'bold'),
    ('search', 10, 'bold'),
    ('', 5, 'normal'),
    ('(curated)', 8, 'normal'),
])

# ═══════════════════════════════════════════════════════════
# VISUALIZATION — Metabase + Kibana
# ═══════════════════════════════════════════════════════════
box(14.0, 3.8, 1.8, 3.5, C_VIZ, [
    ('METABASE', 10, 'bold'),
    ('/', 9, 'normal'),
    ('KIBANA', 10, 'bold'),
])

# ═══════════════════════════════════════════════════════════
# DOCKER BAR
# ═══════════════════════════════════════════════════════════
box(0.3, 1.6, 15.7, 0.7, C_INFRA, [
    ('DOCKER', 12, 'bold'),
])

# ═══════════════════════════════════════════════════════════
# AIRFLOW BAR
# ═══════════════════════════════════════════════════════════
box(0.3, 0.6, 15.7, 0.7, C_ORCH, [
    ('AIRFLOW', 12, 'bold'),
])

# ═══════════════════════════════════════════════════════════
# ARROWS
# ═══════════════════════════════════════════════════════════

# Sources → Python/Airflow ingestion
arrow(1.55, 6.2, 2.0, 5.8)         # Source I → Python
arrow(1.55, 3.8, 2.0, 4.6)         # Source II → Python

# Python → batch path
arrow(3.4, 5.8, 4.0, 6.2)          # Python → DAGs

# Python → streaming path
arrow(3.4, 4.4, 4.0, 4.2)          # Python → Kafka

# Kafka → Spark Streaming
arrow(5.3, 4.2, 5.5, 4.2)

# Batch DAGs → HDFS Raw
arrow(6.8, 6.2, 7.5, 5.8)

# Streaming → HDFS Raw (stream raw data goes to HDFS too)
arrow(6.8, 4.2, 7.5, 4.8)

# Processing header ↔ HDFS zones (bidirectional)
biarrow(8.3, 7.8, 8.3, 7.3)
biarrow(10.1, 7.8, 10.1, 7.3)

# HDFS Raw → HDFS Transform (internal flow)
arrow(9.1, 5.5, 9.3, 5.5)

# HDFS Transform → Curated
arrow(10.9, 5.5, 11.2, 5.5)

# Streaming → Curated (ES)
arrow(6.8, 3.8, 11.2, 4.2, rad=0.15)

# Curated → Viz
arrow(13.0, 5.5, 14.0, 5.5)

# ═══════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'architecture_diagram.png')
plt.tight_layout()
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"✓ Diagram saved to: {output_path}")

output_path_hires = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'architecture_diagram_hires.png')
plt.savefig(output_path_hires, dpi=600, bbox_inches='tight', facecolor='white')
print(f"✓ High-res diagram saved to: {output_path_hires}")

plt.close()
