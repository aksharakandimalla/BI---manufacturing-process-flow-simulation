#!/usr/bin/env python3
"""
Generate SCADA Manufacturing Data
===================================
Produces CSV files for Power BI import.

Usage:
    python generate_data.py
"""

import os
import sys

import pandas as pd

from simulation.scada_sim import ScadaSimulator

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def export_tables(tables: dict[str, pd.DataFrame]):
    """Write every table to its own CSV inside data/."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for name, df in tables.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  ✓ {path}  ({len(df):,} rows)")


def main():
    sim = ScadaSimulator()
    tables = sim.run()

    print("\nExporting CSVs...")
    export_tables(tables)

    # quick sanity print
    total_rows = sum(len(df) for df in tables.values())
    print(f"\nTotal: {total_rows:,} rows across {len(tables)} tables")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    print("\nLoad these CSVs into Power BI Desktop → Get Data → Text/CSV")


if __name__ == "__main__":
    main()
