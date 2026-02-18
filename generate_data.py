#!/usr/bin/env python3
"""
Generate Manufacturing Simulation Data
========================================
Run this script to produce all CSV and Excel files for the Power BI dashboard.

Usage:
    python generate_data.py                  # Default: 2400 job orders
    python generate_data.py --orders 5000    # Custom order count
    python generate_data.py --scenario lean  # Run a what-if scenario
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

from simulation.job_shop import JobShopSimulator

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def apply_scenario(tables: dict[str, pd.DataFrame], scenario: str) -> dict[str, pd.DataFrame]:
    """Apply what-if scenario modifications to the base simulation data."""

    if scenario == "baseline":
        return tables

    elif scenario == "lean":
        # Lean manufacturing: reduce queue times by 40%, improve FPY by 3%
        prod = tables["fact_production"].copy()
        prod["queue_time_min"] = (prod["queue_time_min"] * 0.6).round(1)
        prod["total_time_min"] = (
            prod["setup_time_min"] + prod["cycle_time_min"] * prod["quantity_in"] + prod["queue_time_min"]
        ).round(1)
        prod["first_pass_yield"] = prod["first_pass_yield"].apply(lambda x: min(1.0, x * 1.03)).round(3)
        tables["fact_production"] = prod

        # Reduce downtime by 30%
        dt = tables["fact_downtime"].copy()
        dt["duration_hours"] = (dt["duration_hours"] * 0.7).round(2)
        dt["downtime_cost"] = (dt["downtime_cost"] * 0.7).round(2)
        tables["fact_downtime"] = dt

    elif scenario == "high_demand":
        # High demand: more rush/critical orders, higher volumes
        jobs = tables["fact_job_orders"].copy()
        # Shift priorities toward Rush/Critical
        jobs["priority"] = jobs["priority"].apply(
            lambda p: "Rush" if p == "Standard" and np.random.random() < 0.3 else p
        )
        jobs["quantity"] = (jobs["quantity"] * 1.4).astype(int).clip(lower=1)
        tables["fact_job_orders"] = jobs

    elif scenario == "aging_equipment":
        # Aging equipment: more breakdowns, higher cycle times
        machines = tables["dim_machines"].copy()
        machines["condition_score"] = (machines["condition_score"] * 0.85).round(2)
        machines["age_years"] = (machines["age_years"] + 3).round(1)
        tables["dim_machines"] = machines

        prod = tables["fact_production"].copy()
        prod["cycle_time_min"] = (prod["cycle_time_min"] * 1.12).round(1)
        prod["total_time_min"] = (
            prod["setup_time_min"] + prod["cycle_time_min"] * prod["quantity_in"] + prod["queue_time_min"]
        ).round(1)
        prod["machine_cost"] = (prod["machine_cost"] * 1.12).round(2)
        tables["fact_production"] = prod

        dt = tables["fact_downtime"].copy()
        dt["duration_hours"] = (dt["duration_hours"] * 1.35).round(2)
        dt["downtime_cost"] = (dt["downtime_cost"] * 1.35).round(2)
        tables["fact_downtime"] = dt

    else:
        print(f"Warning: Unknown scenario '{scenario}', using baseline data.")

    return tables


def export_tables(tables: dict[str, pd.DataFrame], scenario: str):
    """Export all tables to CSV and a combined Excel workbook."""
    scenario_dir = os.path.join(OUTPUT_DIR, scenario)
    os.makedirs(scenario_dir, exist_ok=True)

    # Individual CSVs
    for name, df in tables.items():
        path = os.path.join(scenario_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  Exported {path} ({len(df):,} rows)")

    # Combined Excel workbook (what you load into Power BI)
    excel_path = os.path.join(scenario_dir, "manufacturing_data.xlsx")
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for name, df in tables.items():
            sheet_name = name[:31]  # Excel sheet name limit
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"\n  Combined workbook: {excel_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate manufacturing simulation data")
    parser.add_argument("--orders", type=int, default=2400, help="Number of job orders to simulate")
    parser.add_argument(
        "--scenario",
        choices=["baseline", "lean", "high_demand", "aging_equipment"],
        default="baseline",
        help="What-if scenario to apply",
    )
    parser.add_argument("--all-scenarios", action="store_true", help="Generate data for all scenarios")
    args = parser.parse_args()

    scenarios = ["baseline", "lean", "high_demand", "aging_equipment"] if args.all_scenarios else [args.scenario]

    # Run base simulation once
    print(f"Running simulation with {args.orders:,} job orders...\n")
    sim = JobShopSimulator()
    base_tables = sim.run(n_orders=args.orders)

    for scenario in scenarios:
        print(f"\n{'='*50}")
        print(f"Exporting scenario: {scenario}")
        print(f"{'='*50}")
        tables = apply_scenario(
            {k: v.copy() for k, v in base_tables.items()},
            scenario,
        )
        export_tables(tables, scenario)

    print("\nDone! Load the Excel file(s) into Power BI Desktop to get started.")
    print("See powerbi_guide/DASHBOARD_GUIDE.md for step-by-step instructions.")


if __name__ == "__main__":
    main()
