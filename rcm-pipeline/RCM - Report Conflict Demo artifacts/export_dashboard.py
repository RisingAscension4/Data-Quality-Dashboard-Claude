"""
Export the 'Two Reports, One Truth' dashboard into this Git-tracked folder.

Run this script from a Databricks notebook or local environment with workspace access.
It copies the .lvdash.json file from its workspace location into this directory.

Usage (from a notebook cell):
    %run ./export_dashboard

Or manually copy:
    cp "/Workspace/Users/bryan.goodliffe@databricks.com/Two Reports, One Truth — Pinnacle Radiology Partners.lvdash.json" \
       "/Workspace/Users/bryan.goodliffe@databricks.com/Data-Quality-Dashboard-Claude/rcm-pipeline/two_reports_dashboard.lvdash.json"
"""

import shutil
import os

DASHBOARD_SOURCE = "/Workspace/Users/bryan.goodliffe@databricks.com/Two Reports, One Truth — Pinnacle Radiology Partners.lvdash.json"
DASHBOARD_DEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "two_reports_dashboard.lvdash.json")

if os.path.exists(DASHBOARD_SOURCE):
    shutil.copy2(DASHBOARD_SOURCE, DASHBOARD_DEST)
    print(f"Dashboard exported to: {DASHBOARD_DEST}")
    print(f"File size: {os.path.getsize(DASHBOARD_DEST):,} bytes")
else:
    print(f"Dashboard not found at: {DASHBOARD_SOURCE}")
    print("If the dashboard was renamed or moved, update DASHBOARD_SOURCE above.")
