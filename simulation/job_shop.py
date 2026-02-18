"""
Job Shop Manufacturing Simulation Engine
=========================================
Generates realistic manufacturing data for a job shop environment with:
- Multiple machine types (CNC, Lathe, Drill Press, Grinder, Assembly, Inspection)
- Job orders with routing through multiple machines
- Operator assignments with skill levels
- Quality events (defects, rework, scrap)
- Machine downtime (planned maintenance, breakdowns)
- Cost tracking (labor, material, overhead, rework)

The output is a star schema suitable for Power BI analysis.
"""

import random
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

RANDOM_SEED = 42

# Simulation date range
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Machine definitions: (machine_type, count, avg_cycle_min, variability)
MACHINE_SPECS = {
    "CNC Mill": {"count": 4, "avg_cycle_min": 45, "variability": 0.15, "hourly_cost": 85},
    "CNC Lathe": {"count": 3, "avg_cycle_min": 35, "variability": 0.12, "hourly_cost": 75},
    "Drill Press": {"count": 3, "avg_cycle_min": 20, "variability": 0.10, "hourly_cost": 45},
    "Surface Grinder": {"count": 2, "avg_cycle_min": 30, "variability": 0.18, "hourly_cost": 65},
    "Assembly Station": {"count": 4, "avg_cycle_min": 60, "variability": 0.20, "hourly_cost": 40},
    "Inspection Station": {"count": 2, "avg_cycle_min": 15, "variability": 0.08, "hourly_cost": 55},
}

# Product families and their routings
PRODUCT_CATALOG = {
    "Hydraulic Valve Body": {
        "family": "Hydraulic Components",
        "routing": ["CNC Mill", "Drill Press", "Surface Grinder", "Inspection Station"],
        "material_cost": 45.00,
        "priority_weights": {"Standard": 0.6, "Rush": 0.25, "Critical": 0.15},
    },
    "Gear Assembly": {
        "family": "Drivetrain",
        "routing": ["CNC Lathe", "Surface Grinder", "Assembly Station", "Inspection Station"],
        "material_cost": 62.00,
        "priority_weights": {"Standard": 0.7, "Rush": 0.2, "Critical": 0.1},
    },
    "Pump Housing": {
        "family": "Hydraulic Components",
        "routing": ["CNC Mill", "Drill Press", "CNC Lathe", "Inspection Station"],
        "material_cost": 38.00,
        "priority_weights": {"Standard": 0.5, "Rush": 0.3, "Critical": 0.2},
    },
    "Motor Shaft": {
        "family": "Drivetrain",
        "routing": ["CNC Lathe", "Surface Grinder", "Inspection Station"],
        "material_cost": 28.00,
        "priority_weights": {"Standard": 0.65, "Rush": 0.25, "Critical": 0.1},
    },
    "Control Panel Frame": {
        "family": "Electrical Enclosures",
        "routing": ["CNC Mill", "Drill Press", "Assembly Station", "Inspection Station"],
        "material_cost": 55.00,
        "priority_weights": {"Standard": 0.55, "Rush": 0.3, "Critical": 0.15},
    },
    "Bearing Cap": {
        "family": "Drivetrain",
        "routing": ["CNC Lathe", "Drill Press", "Surface Grinder", "Inspection Station"],
        "material_cost": 18.00,
        "priority_weights": {"Standard": 0.75, "Rush": 0.2, "Critical": 0.05},
    },
    "Manifold Block": {
        "family": "Hydraulic Components",
        "routing": ["CNC Mill", "Drill Press", "Drill Press", "Surface Grinder", "Inspection Station"],
        "material_cost": 72.00,
        "priority_weights": {"Standard": 0.5, "Rush": 0.3, "Critical": 0.2},
    },
    "Sensor Bracket": {
        "family": "Electrical Enclosures",
        "routing": ["CNC Mill", "Drill Press", "Assembly Station", "Inspection Station"],
        "material_cost": 12.00,
        "priority_weights": {"Standard": 0.8, "Rush": 0.15, "Critical": 0.05},
    },
}

# Operator pool
OPERATOR_NAMES = [
    "Maria Santos", "James Chen", "Aisha Patel", "Robert Kowalski",
    "Yuki Tanaka", "Carlos Rivera", "Emma Johansson", "David Okafor",
    "Sarah Mitchell", "Ahmed Hassan", "Lisa Nguyen", "Michael Brown",
    "Ana Garcia", "Tomasz Nowak", "Priya Sharma", "Kevin O'Brien",
]

SHIFTS = ["Morning (6AM-2PM)", "Afternoon (2PM-10PM)", "Night (10PM-6AM)"]

# Quality defect types
DEFECT_TYPES = [
    "Dimensional Out-of-Spec", "Surface Finish Defect", "Material Defect",
    "Assembly Error", "Tool Wear Damage", "Alignment Error",
    "Contamination", "Crack/Fracture",
]

DEFECT_SEVERITIES = {"Minor": 0.55, "Major": 0.35, "Critical": 0.10}
DEFECT_DISPOSITIONS = {
    "Minor": {"Rework": 0.8, "Use As-Is": 0.15, "Scrap": 0.05},
    "Major": {"Rework": 0.5, "Scrap": 0.4, "Use As-Is": 0.1},
    "Critical": {"Scrap": 0.7, "Rework": 0.25, "Use As-Is": 0.05},
}

# Downtime categories
DOWNTIME_CATEGORIES = {
    "Planned Maintenance": {"avg_duration_hr": 2.0, "frequency_per_month": 2},
    "Unplanned Breakdown": {"avg_duration_hr": 3.5, "frequency_per_month": 0.8},
    "Tooling Change": {"avg_duration_hr": 0.5, "frequency_per_month": 6},
    "Material Shortage": {"avg_duration_hr": 4.0, "frequency_per_month": 0.3},
    "Quality Hold": {"avg_duration_hr": 1.5, "frequency_per_month": 0.5},
    "Calibration": {"avg_duration_hr": 1.0, "frequency_per_month": 1},
}

CUSTOMERS = [
    "Apex Manufacturing", "BlueStar Industries", "CrestLine Engineering",
    "Delta Precision Corp", "EagleTech Solutions", "ForgeMaster Inc",
    "GlobalDrive Systems", "HorizonMech Ltd",
]


class JobShopSimulator:
    """Simulates a job shop manufacturing environment and produces tabular data."""

    def __init__(self, seed: int = RANDOM_SEED):
        self.rng = np.random.default_rng(seed)
        random.seed(seed)

        # Generated data containers
        self.dim_machines: Optional[pd.DataFrame] = None
        self.dim_products: Optional[pd.DataFrame] = None
        self.dim_operators: Optional[pd.DataFrame] = None
        self.dim_customers: Optional[pd.DataFrame] = None
        self.dim_date: Optional[pd.DataFrame] = None
        self.fact_production: Optional[pd.DataFrame] = None
        self.fact_quality: Optional[pd.DataFrame] = None
        self.fact_downtime: Optional[pd.DataFrame] = None
        self.fact_job_orders: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Dimension generators
    # ------------------------------------------------------------------

    def _build_dim_machines(self) -> pd.DataFrame:
        rows = []
        machine_id = 1
        for mtype, spec in MACHINE_SPECS.items():
            for i in range(1, spec["count"] + 1):
                age_years = round(self.rng.uniform(1, 15), 1)
                rows.append({
                    "machine_id": f"MCH-{machine_id:03d}",
                    "machine_name": f"{mtype} #{i}",
                    "machine_type": mtype,
                    "machine_department": self._department_for(mtype),
                    "avg_cycle_time_min": spec["avg_cycle_min"],
                    "cycle_variability_pct": spec["variability"] * 100,
                    "hourly_operating_cost": spec["hourly_cost"],
                    "age_years": age_years,
                    "condition_score": round(max(0.5, 1.0 - age_years * 0.03 + self.rng.normal(0, 0.05)), 2),
                    "install_date": (START_DATE - timedelta(days=int(age_years * 365))).strftime("%Y-%m-%d"),
                })
                machine_id += 1
        return pd.DataFrame(rows)

    @staticmethod
    def _department_for(mtype: str) -> str:
        mapping = {
            "CNC Mill": "Machining", "CNC Lathe": "Machining",
            "Drill Press": "Machining", "Surface Grinder": "Finishing",
            "Assembly Station": "Assembly", "Inspection Station": "Quality",
        }
        return mapping.get(mtype, "General")

    def _build_dim_products(self) -> pd.DataFrame:
        rows = []
        for pid, (name, spec) in enumerate(PRODUCT_CATALOG.items(), start=1):
            rows.append({
                "product_id": f"PRD-{pid:03d}",
                "product_name": name,
                "product_family": spec["family"],
                "routing_steps": len(spec["routing"]),
                "routing_sequence": " → ".join(spec["routing"]),
                "base_material_cost": spec["material_cost"],
                "target_cycle_time_min": sum(
                    MACHINE_SPECS[m]["avg_cycle_min"] for m in spec["routing"]
                ),
            })
        return pd.DataFrame(rows)

    def _build_dim_operators(self) -> pd.DataFrame:
        rows = []
        for oid, name in enumerate(OPERATOR_NAMES, start=1):
            primary_shift = random.choice(SHIFTS)
            hire_date = START_DATE - timedelta(days=int(self.rng.integers(180, 3650)))
            experience_years = round((START_DATE - hire_date).days / 365, 1)
            skill = (
                "Expert" if experience_years > 7
                else "Advanced" if experience_years > 3
                else "Intermediate" if experience_years > 1
                else "Junior"
            )
            certifications = self.rng.integers(1, 6)
            rows.append({
                "operator_id": f"OPR-{oid:03d}",
                "operator_name": name,
                "primary_shift": primary_shift,
                "hire_date": hire_date.strftime("%Y-%m-%d"),
                "experience_years": experience_years,
                "skill_level": skill,
                "certifications_count": int(certifications),
                "efficiency_rating": round(
                    min(1.0, 0.7 + experience_years * 0.03 + self.rng.normal(0, 0.04)), 2
                ),
            })
        return pd.DataFrame(rows)

    def _build_dim_customers(self) -> pd.DataFrame:
        rows = []
        for cid, name in enumerate(CUSTOMERS, start=1):
            rows.append({
                "customer_id": f"CUS-{cid:03d}",
                "customer_name": name,
                "customer_tier": random.choices(
                    ["Platinum", "Gold", "Silver"], weights=[0.2, 0.4, 0.4]
                )[0],
                "region": random.choice(["North America", "Europe", "Asia-Pacific"]),
                "on_time_delivery_target_pct": random.choice([95, 97, 98, 99]),
            })
        return pd.DataFrame(rows)

    def _build_dim_date(self) -> pd.DataFrame:
        dates = pd.date_range(START_DATE, END_DATE, freq="D")
        rows = []
        for d in dates:
            rows.append({
                "date": d.strftime("%Y-%m-%d"),
                "year": d.year,
                "quarter": f"Q{d.quarter}",
                "month_num": d.month,
                "month_name": d.strftime("%B"),
                "week_num": d.isocalendar()[1],
                "day_of_week": d.strftime("%A"),
                "is_weekend": d.weekday() >= 5,
                "is_working_day": d.weekday() < 5,
                "fiscal_year": d.year if d.month >= 7 else d.year - 1,
                "fiscal_quarter": f"FQ{((d.month - 7) % 12) // 3 + 1}",
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Fact table generators
    # ------------------------------------------------------------------

    def _build_fact_job_orders(self, n_orders: int = 2400) -> pd.DataFrame:
        """Generate job orders spread across the simulation period."""
        products = list(PRODUCT_CATALOG.keys())
        product_ids = {name: f"PRD-{i:03d}" for i, name in enumerate(products, 1)}
        customer_ids = [f"CUS-{i:03d}" for i in range(1, len(CUSTOMERS) + 1)]

        working_days = pd.bdate_range(START_DATE, END_DATE)
        rows = []

        for jid in range(1, n_orders + 1):
            product_name = random.choice(products)
            spec = PRODUCT_CATALOG[product_name]

            # Pick priority
            priorities = list(spec["priority_weights"].keys())
            weights = list(spec["priority_weights"].values())
            priority = random.choices(priorities, weights=weights)[0]

            order_date = random.choice(working_days)
            lead_days = {"Standard": 14, "Rush": 7, "Critical": 3}[priority]
            due_date = order_date + timedelta(days=lead_days)
            quantity = int(self.rng.choice([1, 2, 5, 10, 20, 50], p=[0.1, 0.15, 0.25, 0.25, 0.15, 0.1]))

            rows.append({
                "job_order_id": f"JOB-{jid:05d}",
                "product_id": product_ids[product_name],
                "product_name": product_name,
                "customer_id": random.choice(customer_ids),
                "order_date": order_date.strftime("%Y-%m-%d"),
                "due_date": due_date.strftime("%Y-%m-%d"),
                "priority": priority,
                "quantity": quantity,
                "status": "Completed",  # historical data
            })

        return pd.DataFrame(rows)

    def _build_fact_production(self) -> pd.DataFrame:
        """Generate production step records for each job order."""
        machines_by_type = {}
        for _, row in self.dim_machines.iterrows():
            machines_by_type.setdefault(row["machine_type"], []).append(row["machine_id"])

        operator_ids = self.dim_operators["operator_id"].tolist()
        operator_efficiency = dict(
            zip(self.dim_operators["operator_id"], self.dim_operators["efficiency_rating"])
        )

        rows = []
        prod_id = 1

        for _, job in self.fact_job_orders.iterrows():
            product_name = job["product_name"]
            spec = PRODUCT_CATALOG[product_name]
            routing = spec["routing"]
            order_date = pd.Timestamp(job["order_date"])
            current_time = order_date + timedelta(hours=self.rng.uniform(1, 8))

            for step_num, machine_type in enumerate(routing, start=1):
                machine_spec = MACHINE_SPECS[machine_type]
                machine_id = random.choice(machines_by_type[machine_type])
                operator_id = random.choice(operator_ids)
                eff = operator_efficiency[operator_id]

                # Cycle time with variability, affected by operator efficiency
                base_cycle = machine_spec["avg_cycle_min"]
                variability = machine_spec["variability"]
                actual_cycle = max(
                    base_cycle * 0.5,
                    base_cycle * (1 / eff) + self.rng.normal(0, base_cycle * variability),
                )
                actual_cycle = round(actual_cycle, 1)

                # Queue/wait time before this step (higher for bottleneck machines)
                queue_time = round(max(0, self.rng.exponential(30) * (1 if machine_type != "Surface Grinder" else 2.5)), 1)

                # Setup time
                setup_time = round(max(3, self.rng.normal(10, 3)), 1)

                start_time = current_time + timedelta(minutes=queue_time)
                end_time = start_time + timedelta(minutes=setup_time + actual_cycle * job["quantity"])

                # Determine shift based on start hour
                hour = start_time.hour
                if 6 <= hour < 14:
                    shift = "Morning (6AM-2PM)"
                elif 14 <= hour < 22:
                    shift = "Afternoon (2PM-10PM)"
                else:
                    shift = "Night (10PM-6AM)"

                # Cost calculation
                total_minutes = setup_time + actual_cycle * job["quantity"]
                machine_cost = round((total_minutes / 60) * machine_spec["hourly_cost"], 2)
                labor_cost = round((total_minutes / 60) * 35, 2)  # avg $35/hr labor

                # First pass yield (slightly lower for complex operations)
                complexity_factor = 1.0 if step_num <= 2 else 0.98
                fpy = round(min(1.0, max(0.85, self.rng.normal(0.96, 0.03) * eff * complexity_factor)), 3)

                rows.append({
                    "production_id": f"PRD-RUN-{prod_id:06d}",
                    "job_order_id": job["job_order_id"],
                    "product_id": job["product_id"],
                    "machine_id": machine_id,
                    "operator_id": operator_id,
                    "step_number": step_num,
                    "step_machine_type": machine_type,
                    "date": start_time.strftime("%Y-%m-%d"),
                    "shift": shift,
                    "start_time": start_time.strftime("%Y-%m-%d %H:%M"),
                    "end_time": end_time.strftime("%Y-%m-%d %H:%M"),
                    "setup_time_min": setup_time,
                    "cycle_time_min": actual_cycle,
                    "queue_time_min": queue_time,
                    "total_time_min": round(setup_time + actual_cycle * job["quantity"] + queue_time, 1),
                    "quantity_in": job["quantity"],
                    "quantity_good": max(1, int(job["quantity"] * fpy)),
                    "first_pass_yield": fpy,
                    "machine_cost": machine_cost,
                    "labor_cost": labor_cost,
                })

                current_time = end_time + timedelta(minutes=self.rng.uniform(5, 30))
                prod_id += 1

        return pd.DataFrame(rows)

    def _build_fact_quality(self) -> pd.DataFrame:
        """Generate quality events (defects) from production runs with low FPY."""
        rows = []
        qid = 1

        for _, prod in self.fact_production.iterrows():
            defect_count = prod["quantity_in"] - prod["quantity_good"]
            if defect_count <= 0:
                continue

            for _ in range(defect_count):
                severity = random.choices(
                    list(DEFECT_SEVERITIES.keys()),
                    weights=list(DEFECT_SEVERITIES.values()),
                )[0]
                disposition_options = DEFECT_DISPOSITIONS[severity]
                disposition = random.choices(
                    list(disposition_options.keys()),
                    weights=list(disposition_options.values()),
                )[0]

                rework_cost = 0.0
                if disposition == "Rework":
                    rework_cost = round(self.rng.uniform(15, 120), 2)
                elif disposition == "Scrap":
                    rework_cost = round(
                        PRODUCT_CATALOG.get(
                            self._product_name_from_id(prod["product_id"]), {}
                        ).get("material_cost", 30) * self.rng.uniform(0.5, 1.0),
                        2,
                    )

                rows.append({
                    "quality_event_id": f"QE-{qid:06d}",
                    "production_id": prod["production_id"],
                    "job_order_id": prod["job_order_id"],
                    "product_id": prod["product_id"],
                    "machine_id": prod["machine_id"],
                    "operator_id": prod["operator_id"],
                    "date": prod["date"],
                    "defect_type": random.choice(DEFECT_TYPES),
                    "severity": severity,
                    "disposition": disposition,
                    "rework_cost": rework_cost,
                    "root_cause": random.choice([
                        "Tool Wear", "Operator Error", "Material Variation",
                        "Machine Calibration", "Process Drift", "Environmental",
                    ]),
                    "corrective_action_taken": random.choice([True, False]),
                })
                qid += 1

        return pd.DataFrame(rows)

    def _product_name_from_id(self, product_id: str) -> str:
        products = list(PRODUCT_CATALOG.keys())
        idx = int(product_id.split("-")[1]) - 1
        if 0 <= idx < len(products):
            return products[idx]
        return ""

    def _build_fact_downtime(self) -> pd.DataFrame:
        """Generate machine downtime events across the simulation period."""
        rows = []
        dt_id = 1
        total_months = (END_DATE.year - START_DATE.year) * 12 + (END_DATE.month - START_DATE.month)

        for _, machine in self.dim_machines.iterrows():
            for category, spec in DOWNTIME_CATEGORIES.items():
                # Scale breakdown frequency by machine condition (worse condition = more breakdowns)
                freq = spec["frequency_per_month"]
                if category == "Unplanned Breakdown":
                    condition = machine["condition_score"]
                    freq *= max(0.5, 2.0 - condition)

                n_events = int(self.rng.poisson(freq * total_months))

                for _ in range(n_events):
                    event_date = START_DATE + timedelta(
                        days=int(self.rng.uniform(0, (END_DATE - START_DATE).days))
                    )
                    if event_date.weekday() >= 5:
                        continue  # skip weekends

                    duration = max(0.25, self.rng.normal(spec["avg_duration_hr"], spec["avg_duration_hr"] * 0.3))
                    duration = round(duration, 2)

                    cost = round(
                        duration * machine["hourly_operating_cost"] * 0.3  # downtime cost factor
                        + (self.rng.uniform(100, 500) if category == "Unplanned Breakdown" else 0),
                        2,
                    )

                    rows.append({
                        "downtime_id": f"DT-{dt_id:06d}",
                        "machine_id": machine["machine_id"],
                        "date": event_date.strftime("%Y-%m-%d"),
                        "downtime_category": category,
                        "duration_hours": duration,
                        "downtime_cost": cost,
                        "shift_affected": random.choice(SHIFTS),
                        "was_scheduled": category in ("Planned Maintenance", "Calibration"),
                        "impact_description": self._downtime_impact(category),
                    })
                    dt_id += 1

        return pd.DataFrame(rows)

    @staticmethod
    def _downtime_impact(category: str) -> str:
        impacts = {
            "Planned Maintenance": "Scheduled service - production rerouted",
            "Unplanned Breakdown": "Emergency repair required - jobs delayed",
            "Tooling Change": "Tool replacement for new job setup",
            "Material Shortage": "Waiting for material delivery",
            "Quality Hold": "Production paused pending quality investigation",
            "Calibration": "Periodic calibration per quality standard",
        }
        return impacts.get(category, "Unknown")

    # ------------------------------------------------------------------
    # Run simulation
    # ------------------------------------------------------------------

    def run(self, n_orders: int = 2400) -> dict[str, pd.DataFrame]:
        """Execute the full simulation and return all tables."""
        print("Building dimension tables...")
        self.dim_machines = self._build_dim_machines()
        self.dim_products = self._build_dim_products()
        self.dim_operators = self._build_dim_operators()
        self.dim_customers = self._build_dim_customers()
        self.dim_date = self._build_dim_date()

        print("Generating job orders...")
        self.fact_job_orders = self._build_fact_job_orders(n_orders)

        print("Simulating production runs...")
        self.fact_production = self._build_fact_production()

        print("Generating quality events...")
        self.fact_quality = self._build_fact_quality()

        print("Generating downtime events...")
        self.fact_downtime = self._build_fact_downtime()

        tables = {
            "dim_machines": self.dim_machines,
            "dim_products": self.dim_products,
            "dim_operators": self.dim_operators,
            "dim_customers": self.dim_customers,
            "dim_date": self.dim_date,
            "fact_job_orders": self.fact_job_orders,
            "fact_production": self.fact_production,
            "fact_quality": self.fact_quality,
            "fact_downtime": self.fact_downtime,
        }

        print("\n--- Simulation Summary ---")
        for name, df in tables.items():
            print(f"  {name}: {len(df):,} rows × {len(df.columns)} columns")

        return tables
