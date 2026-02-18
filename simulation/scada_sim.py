"""
SCADA Assembly Line Simulation Engine – Robotic Arm Component
==============================================================
Generates 6 months of realistic SCADA sensor data for a 6-station
robotic-arm assembly line.  Every signal is built from 7 stacked layers
so that the resulting CSVs contain discoverable BI stories:

  1. Base signal      – physically realistic mean + Gaussian / log-normal noise
  2. Shift effects    – night shift runs noisier
  3. Degradation      – tool / bearing wear trends that reset on maintenance
  4. Correlated faults – humidity → solder defects, torque drift → failures
  5. Rush orders      – faster cycle ⇒ wider variance ⇒ more scrap
  6. Bottleneck       – Station 4 is deliberately constrained
  7. Seasonality      – summer heat / Q4 demand surge

Output tables (star-schema, Power BI ready):
  dim_stations, dim_sensors, dim_operators, dim_shifts, dim_date,
  dim_products, fact_sensor_readings, fact_production, fact_quality_events,
  fact_downtime, fact_alarms
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

# ── reproducibility ──────────────────────────────────────────────────────────
SEED = 42

# ── time range ───────────────────────────────────────────────────────────────
SIM_START = datetime(2024, 4, 1)
SIM_END = datetime(2024, 9, 30)
MINUTES_PER_READING = 1  # sensor sample interval

# ── station definitions ──────────────────────────────────────────────────────
STATIONS = [
    {
        "station_id": "STN-01",
        "station_name": "Precision CNC Machining",
        "description": "Joint housings & arm segment milling",
        "position": 1,
        "num_machines": 3,
        "cycle_time_mean_min": 45,
        "cycle_time_std_min": 4,
        "base_defect_rate": 0.02,
    },
    {
        "station_id": "STN-02",
        "station_name": "Electronic Board Assembly",
        "description": "Control PCB & sensor board SMT",
        "position": 2,
        "num_machines": 2,
        "cycle_time_mean_min": 35,
        "cycle_time_std_min": 3,
        "base_defect_rate": 0.03,
    },
    {
        "station_id": "STN-03",
        "station_name": "Servo Motor & Actuator",
        "description": "Motor winding, gear train assembly",
        "position": 3,
        "num_machines": 2,
        "cycle_time_mean_min": 50,
        "cycle_time_std_min": 5,
        "base_defect_rate": 0.025,
    },
    {
        "station_id": "STN-04",
        "station_name": "Robotic Arm Integration",
        "description": "Mechanical assembly, wiring, joint fit",
        "position": 4,
        "num_machines": 1,          # ← bottleneck: single machine
        "cycle_time_mean_min": 65,  # ← longest cycle
        "cycle_time_std_min": 8,
        "base_defect_rate": 0.035,
    },
    {
        "station_id": "STN-05",
        "station_name": "Cleanroom Assembly",
        "description": "End-effector sterile packaging & assembly",
        "position": 5,
        "num_machines": 2,
        "cycle_time_mean_min": 40,
        "cycle_time_std_min": 3,
        "base_defect_rate": 0.03,
    },
    {
        "station_id": "STN-06",
        "station_name": "Calibration & Functional Test",
        "description": "Full system accuracy & safety verification",
        "position": 6,
        "num_machines": 2,
        "cycle_time_mean_min": 30,
        "cycle_time_std_min": 2,
        "base_defect_rate": 0.015,
    },
]

# ── sensor definitions per station ───────────────────────────────────────────
# Each sensor: (sensor_name, unit, baseline, noise_std, distribution,
#               alarm_low, alarm_high, degradation_rate_per_day)
SENSOR_MAP: dict[str, list[dict[str, Any]]] = {
    "STN-01": [
        {"name": "spindle_speed",    "unit": "RPM",  "baseline": 8000, "noise_std": 120,  "dist": "gaussian", "alarm_lo": 7200, "alarm_hi": 8800, "degrade": 0.0},
        {"name": "spindle_vibration","unit": "mm/s", "baseline": 2.1,  "noise_std": 0.15, "dist": "lognormal","alarm_lo": None, "alarm_hi": 4.5,  "degrade": 0.08},
        {"name": "coolant_temp",     "unit": "°C",   "baseline": 22.0, "noise_std": 1.2,  "dist": "gaussian", "alarm_lo": 15,   "alarm_hi": 35,   "degrade": 0.0},
        {"name": "tool_wear_index",  "unit": "%",    "baseline": 5.0,  "noise_std": 0.5,  "dist": "gaussian", "alarm_lo": None, "alarm_hi": 85,   "degrade": 2.8},
        {"name": "cutting_force",    "unit": "N",    "baseline": 450,  "noise_std": 30,   "dist": "gaussian", "alarm_lo": 200,  "alarm_hi": 700,  "degrade": 3.0},
    ],
    "STN-02": [
        {"name": "solder_temp",      "unit": "°C",   "baseline": 245,  "noise_std": 3.0,  "dist": "gaussian", "alarm_lo": 230,  "alarm_hi": 260,  "degrade": 0.0},
        {"name": "placement_accuracy","unit": "mm",  "baseline": 0.05, "noise_std": 0.008,"dist": "gaussian", "alarm_lo": None, "alarm_hi": 0.10, "degrade": 0.002},
        {"name": "board_voltage",    "unit": "V",    "baseline": 3.30, "noise_std": 0.02, "dist": "gaussian", "alarm_lo": 3.10, "alarm_hi": 3.50, "degrade": 0.0},
        {"name": "ambient_temp",     "unit": "°C",   "baseline": 23.0, "noise_std": 0.8,  "dist": "gaussian", "alarm_lo": 18,   "alarm_hi": 28,   "degrade": 0.0},
    ],
    "STN-03": [
        {"name": "winding_torque",   "unit": "Nm",   "baseline": 1.8,  "noise_std": 0.12, "dist": "gaussian", "alarm_lo": 1.2,  "alarm_hi": 2.5,  "degrade": 0.015},
        {"name": "insulation_resistance","unit": "MΩ","baseline": 500,  "noise_std": 20,   "dist": "gaussian", "alarm_lo": 200,  "alarm_hi": None, "degrade": -3.0},
        {"name": "motor_current",    "unit": "A",    "baseline": 2.5,  "noise_std": 0.15, "dist": "gaussian", "alarm_lo": None, "alarm_hi": 4.0,  "degrade": 0.02},
        {"name": "coil_temp",        "unit": "°C",   "baseline": 55,   "noise_std": 2.5,  "dist": "gaussian", "alarm_lo": None, "alarm_hi": 85,   "degrade": 0.3},
    ],
    "STN-04": [
        {"name": "joint_torque",     "unit": "Nm",   "baseline": 12.0, "noise_std": 0.8,  "dist": "gaussian", "alarm_lo": 8,    "alarm_hi": 18,   "degrade": 0.05},
        {"name": "positional_accuracy","unit": "mm", "baseline": 0.02, "noise_std": 0.004,"dist": "gaussian", "alarm_lo": None, "alarm_hi": 0.08, "degrade": 0.001},
        {"name": "fastener_torque",  "unit": "Nm",   "baseline": 5.5,  "noise_std": 0.3,  "dist": "gaussian", "alarm_lo": 4.0,  "alarm_hi": 7.0,  "degrade": 0.0},
        {"name": "vibration",        "unit": "mm/s", "baseline": 1.5,  "noise_std": 0.10, "dist": "lognormal","alarm_lo": None, "alarm_hi": 3.5,  "degrade": 0.06},
    ],
    "STN-05": [
        {"name": "particle_count",   "unit": "p/m³", "baseline": 3500, "noise_std": 400,  "dist": "poisson",  "alarm_lo": None, "alarm_hi": 10000,"degrade": 0.0},
        {"name": "humidity",         "unit": "%RH",  "baseline": 42,   "noise_std": 3.0,  "dist": "gaussian", "alarm_lo": 30,   "alarm_hi": 55,   "degrade": 0.0},
        {"name": "cleanroom_temp",   "unit": "°C",   "baseline": 21.0, "noise_std": 0.5,  "dist": "gaussian", "alarm_lo": 19,   "alarm_hi": 23,   "degrade": 0.0},
        {"name": "glove_integrity",  "unit": "score","baseline": 98,   "noise_std": 1.0,  "dist": "gaussian", "alarm_lo": 90,   "alarm_hi": None, "degrade": -0.4},
    ],
    "STN-06": [
        {"name": "repeatability_error","unit": "mm", "baseline": 0.01, "noise_std": 0.003,"dist": "gaussian", "alarm_lo": None, "alarm_hi": 0.05, "degrade": 0.0},
        {"name": "force_feedback",   "unit": "N",    "baseline": 2.0,  "noise_std": 0.1,  "dist": "gaussian", "alarm_lo": 1.0,  "alarm_hi": 3.5,  "degrade": 0.0},
        {"name": "control_latency",  "unit": "ms",   "baseline": 8.0,  "noise_std": 0.5,  "dist": "lognormal","alarm_lo": None, "alarm_hi": 20,   "degrade": 0.0},
        {"name": "system_voltage",   "unit": "V",    "baseline": 48.0, "noise_std": 0.3,  "dist": "gaussian", "alarm_lo": 44,   "alarm_hi": 52,   "degrade": 0.0},
    ],
}

# ── operator pool ────────────────────────────────────────────────────────────
OPERATORS = [
    ("OPR-01", "Maria Santos",     "Day",   8.2, "Expert"),
    ("OPR-02", "James Chen",       "Day",   6.5, "Advanced"),
    ("OPR-03", "Aisha Patel",      "Day",   4.1, "Advanced"),
    ("OPR-04", "Robert Kowalski",  "Day",   2.3, "Intermediate"),
    ("OPR-05", "Yuki Tanaka",      "Swing", 7.8, "Expert"),
    ("OPR-06", "Carlos Rivera",    "Swing", 3.5, "Advanced"),
    ("OPR-07", "Emma Johansson",   "Swing", 1.8, "Intermediate"),
    ("OPR-08", "David Okafor",     "Swing", 5.0, "Advanced"),
    ("OPR-09", "Sarah Mitchell",   "Night", 9.1, "Expert"),
    ("OPR-10", "Ahmed Hassan",     "Night", 2.0, "Intermediate"),
    ("OPR-11", "Lisa Nguyen",      "Night", 0.8, "Junior"),
    ("OPR-12", "Kevin O'Brien",    "Night", 1.2, "Junior"),
]

# ── shift definitions ────────────────────────────────────────────────────────
SHIFTS = {
    "Day":   {"start_hour": 6,  "end_hour": 14, "noise_mult": 1.00, "efficiency": 1.00},
    "Swing": {"start_hour": 14, "end_hour": 22, "noise_mult": 1.08, "efficiency": 0.97},
    "Night": {"start_hour": 22, "end_hour": 6,  "noise_mult": 1.18, "efficiency": 0.93},
}

# ── product variants ─────────────────────────────────────────────────────────
PRODUCTS = [
    {"product_id": "RA-100", "product_name": "RA-100 Standard Arm",     "complexity": 1.0, "unit_material_cost": 1200},
    {"product_id": "RA-200", "product_name": "RA-200 Extended Reach",   "complexity": 1.15,"unit_material_cost": 1450},
    {"product_id": "RA-300", "product_name": "RA-300 High-Precision",   "complexity": 1.30,"unit_material_cost": 1800},
]

# ── downtime categories ──────────────────────────────────────────────────────
DOWNTIME_CATS = {
    "Planned Maintenance":  {"avg_hrs": 2.0, "monthly_freq": 2.0, "scheduled": True},
    "Unplanned Breakdown":  {"avg_hrs": 3.5, "monthly_freq": 0.8, "scheduled": False},
    "Tooling Change":       {"avg_hrs": 0.5, "monthly_freq": 5.0, "scheduled": True},
    "Material Shortage":    {"avg_hrs": 3.0, "monthly_freq": 0.3, "scheduled": False},
    "Quality Hold":         {"avg_hrs": 1.5, "monthly_freq": 0.4, "scheduled": False},
    "Calibration":          {"avg_hrs": 1.0, "monthly_freq": 1.0, "scheduled": True},
}


# ═══════════════════════════════════════════════════════════════════════════════
# Simulation Engine
# ═══════════════════════════════════════════════════════════════════════════════

class ScadaSimulator:
    """Generates a full set of SCADA tables for a robotic-arm assembly line."""

    def __init__(self, seed: int = SEED):
        self.rng = np.random.default_rng(seed)
        random.seed(seed)

        # working-day calendar (Mon-Fri)
        self.workdays = pd.bdate_range(SIM_START, SIM_END).tolist()
        self.total_months = (SIM_END.year - SIM_START.year) * 12 + (SIM_END.month - SIM_START.month) + 1

        # maintenance schedule: station_id → list of maintenance dates
        self.maintenance_dates: dict[str, list[datetime]] = {}

        # order schedule: list of (date, product, priority, order_id)
        self.orders: list[dict] = []

        # result containers
        self.tables: dict[str, pd.DataFrame] = {}

    # ── helpers ───────────────────────────────────────────────────────────

    def _shift_for_hour(self, hour: int) -> str:
        if 6 <= hour < 14:
            return "Day"
        elif 14 <= hour < 22:
            return "Swing"
        return "Night"

    def _seasonal_temp_offset(self, dt: datetime) -> float:
        """Summer peaks ~+4 °C in July/August."""
        day_of_year = dt.timetuple().tm_yday
        return 4.0 * math.sin(2 * math.pi * (day_of_year - 80) / 365)

    def _seasonal_humidity_offset(self, dt: datetime) -> float:
        """Humidity peaks in summer: +10 %RH."""
        day_of_year = dt.timetuple().tm_yday
        return 10.0 * math.sin(2 * math.pi * (day_of_year - 80) / 365)

    def _daily_thermal_cycle(self, hour: int) -> float:
        """Machines warm up through the day."""
        return 1.5 * math.sin(2 * math.pi * (hour - 6) / 24)

    def _demand_multiplier(self, dt: datetime) -> float:
        """Q4 demand surge (Sep-Oct in our 6-month window)."""
        if dt.month in (9, 10):
            return 1.30
        return 1.0

    # ── build dimension tables ────────────────────────────────────────────

    def _build_dim_stations(self) -> pd.DataFrame:
        rows = []
        for s in STATIONS:
            rows.append({
                "station_id": s["station_id"],
                "station_name": s["station_name"],
                "description": s["description"],
                "line_position": s["position"],
                "num_machines": s["num_machines"],
                "target_cycle_time_min": s["cycle_time_mean_min"],
                "is_bottleneck": s["station_id"] == "STN-04",
            })
        return pd.DataFrame(rows)

    def _build_dim_sensors(self) -> pd.DataFrame:
        rows = []
        sid = 1
        for station_id, sensors in SENSOR_MAP.items():
            for s in sensors:
                rows.append({
                    "sensor_id": f"SNS-{sid:03d}",
                    "station_id": station_id,
                    "sensor_name": s["name"],
                    "unit": s["unit"],
                    "baseline_value": s["baseline"],
                    "alarm_low": s["alarm_lo"],
                    "alarm_high": s["alarm_hi"],
                })
                sid += 1
        return pd.DataFrame(rows)

    def _build_dim_operators(self) -> pd.DataFrame:
        rows = []
        for oid, name, shift, exp, skill in OPERATORS:
            eff = round(min(1.0, 0.75 + exp * 0.025 + self.rng.normal(0, 0.02)), 3)
            rows.append({
                "operator_id": oid,
                "operator_name": name,
                "primary_shift": shift,
                "experience_years": exp,
                "skill_level": skill,
                "efficiency_rating": eff,
            })
        return pd.DataFrame(rows)

    def _build_dim_shifts(self) -> pd.DataFrame:
        rows = []
        for name, spec in SHIFTS.items():
            rows.append({
                "shift_name": name,
                "start_hour": spec["start_hour"],
                "end_hour": spec["end_hour"],
                "noise_multiplier": spec["noise_mult"],
                "efficiency_factor": spec["efficiency"],
            })
        return pd.DataFrame(rows)

    def _build_dim_products(self) -> pd.DataFrame:
        return pd.DataFrame(PRODUCTS)

    def _build_dim_date(self) -> pd.DataFrame:
        dates = pd.date_range(SIM_START, SIM_END, freq="D")
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
            })
        return pd.DataFrame(rows)

    # ── generate maintenance schedule ────────────────────────────────────

    def _schedule_maintenance(self):
        """Pre-generate planned maintenance dates per station so degradation
        can reset at those points."""
        for s in STATIONS:
            sid = s["station_id"]
            dates = []
            # roughly every 18-22 working days
            cursor = SIM_START + timedelta(days=int(self.rng.integers(14, 22)))
            while cursor <= SIM_END:
                if cursor.weekday() < 5:
                    dates.append(cursor)
                cursor += timedelta(days=int(self.rng.integers(18, 25)))
            self.maintenance_dates[sid] = dates

    # ── generate order schedule ──────────────────────────────────────────

    def _generate_orders(self):
        """Create daily production orders across the 6-month window."""
        oid = 1
        for day in self.workdays:
            base_count = 8  # ~8 units/day baseline
            count = int(base_count * self._demand_multiplier(day) + self.rng.integers(-1, 3))
            count = max(4, count)
            for _ in range(count):
                product = random.choices(
                    PRODUCTS,
                    weights=[0.50, 0.30, 0.20],  # standard most common
                )[0]
                priority = random.choices(
                    ["Standard", "Rush", "Critical"],
                    weights=[0.65, 0.25, 0.10],
                )[0]
                self.orders.append({
                    "order_id": f"ORD-{oid:05d}",
                    "date": day,
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "complexity": product["complexity"],
                    "priority": priority,
                    "unit_material_cost": product["unit_material_cost"],
                })
                oid += 1

    # ── core: sensor reading generation (7-layer model) ──────────────────

    def _days_since_last_maintenance(self, station_id: str, current_date: datetime) -> int:
        """Days since most recent maintenance before current_date."""
        dates = self.maintenance_dates.get(station_id, [])
        past = [d for d in dates if d <= current_date]
        if not past:
            return (current_date - SIM_START).days
        return (current_date - max(past)).days

    def _generate_sensor_value(
        self,
        sensor: dict[str, Any],
        station_id: str,
        dt: datetime,
        shift: str,
        is_rush: bool,
    ) -> float:
        """
        7-layer signal model:
          Layer 1: baseline + noise (distribution-appropriate)
          Layer 2: shift effect (night = noisier)
          Layer 3: degradation trend (resets on maintenance)
          Layer 4: correlated environmental (seasonal temp/humidity)
          Layer 5: rush order modifier (wider variance)
          Layer 6: bottleneck stress (Station 4 extra variance)
          Layer 7: seasonal + daily thermal cycle
        """
        baseline = sensor["baseline"]
        noise_std = sensor["noise_std"]
        degrade_rate = sensor["degrade"]

        # -- Layer 2: shift noise multiplier --
        shift_mult = SHIFTS[shift]["noise_mult"]
        effective_std = noise_std * shift_mult

        # -- Layer 5: rush order widens variance --
        if is_rush:
            effective_std *= 1.35

        # -- Layer 6: bottleneck station gets extra stress --
        if station_id == "STN-04":
            effective_std *= 1.15

        # -- Layer 1: base noise --
        dist = sensor["dist"]
        if dist == "gaussian":
            noise = self.rng.normal(0, effective_std)
        elif dist == "lognormal":
            sigma_ln = math.sqrt(math.log(1 + (effective_std / baseline) ** 2))
            mu_ln = math.log(baseline) - 0.5 * sigma_ln ** 2
            noise = self.rng.lognormal(mu_ln, sigma_ln) - baseline
        elif dist == "poisson":
            noise = self.rng.poisson(baseline) - baseline
            noise *= shift_mult
        else:
            noise = self.rng.normal(0, effective_std)

        # -- Layer 3: degradation --
        days_since_maint = self._days_since_last_maintenance(station_id, dt)
        degradation = degrade_rate * days_since_maint

        # -- Layer 7: seasonal + daily thermal --
        seasonal = 0.0
        daily = 0.0
        name = sensor["name"]
        if "temp" in name or name == "coil_temp":
            seasonal = self._seasonal_temp_offset(dt)
            daily = self._daily_thermal_cycle(dt.hour)
        elif name == "humidity":
            seasonal = self._seasonal_humidity_offset(dt)
        elif name == "particle_count":
            # particles correlate with humidity
            seasonal = self._seasonal_humidity_offset(dt) * 50  # scale to count

        value = baseline + degradation + seasonal + daily + noise

        # physical clamps
        if sensor["unit"] == "%":
            value = max(0, min(100, value))
        elif sensor["unit"] == "%RH":
            value = max(15, min(85, value))
        elif sensor["unit"] in ("mm/s", "mm", "ms", "N", "A", "Nm", "RPM", "p/m³"):
            value = max(0, value)

        return round(value, 3)

    # ── fact: sensor readings (the big table) ────────────────────────────

    def _build_fact_sensor_readings(self) -> pd.DataFrame:
        """Generate per-minute sensor readings for every station, every workday.
        To keep file size manageable for Power BI free tier, we sample every
        5 minutes during operating hours (6 AM – 10 PM = 16 hrs = 192 readings/sensor/day).
        """
        print("  Generating sensor readings (this is the large table)...")
        sample_interval = 5  # minutes between readings

        # pre-compute: which days have rush orders
        rush_days: dict[str, set[str]] = {}  # date_str → set of station_ids affected
        for order in self.orders:
            if order["priority"] in ("Rush", "Critical"):
                ds = order["date"].strftime("%Y-%m-%d")
                # rush orders stress all stations
                rush_days.setdefault(ds, set()).update(
                    s["station_id"] for s in STATIONS
                )

        # build sensor_id lookup
        sensor_ids = {}
        sid = 1
        for station_id, sensors in SENSOR_MAP.items():
            for s in sensors:
                sensor_ids[(station_id, s["name"])] = f"SNS-{sid:03d}"
                sid += 1

        all_rows = []
        total_days = len(self.workdays)

        for day_idx, day in enumerate(self.workdays):
            if day_idx % 20 == 0:
                print(f"    Day {day_idx + 1}/{total_days}...")

            day_str = day.strftime("%Y-%m-%d")
            is_rush_day = day_str in rush_days

            for station in STATIONS:
                station_id = station["station_id"]
                sensors = SENSOR_MAP[station_id]
                station_rush = is_rush_day and station_id in rush_days.get(day_str, set())

                # operating hours: 06:00 – 22:00 (Day + Swing shifts)
                # night shift: 22:00 – 06:00 next day (reduced volume)
                for hour in range(6, 24):
                    shift = self._shift_for_hour(hour)
                    for minute in range(0, 60, sample_interval):
                        dt = datetime(day.year, day.month, day.day, hour, minute)
                        for s in sensors:
                            value = self._generate_sensor_value(
                                s, station_id, dt, shift, station_rush
                            )
                            all_rows.append({
                                "timestamp": dt.strftime("%Y-%m-%d %H:%M"),
                                "date": day_str,
                                "station_id": station_id,
                                "sensor_id": sensor_ids[(station_id, s["name"])],
                                "sensor_name": s["name"],
                                "value": value,
                                "unit": s["unit"],
                                "shift": shift,
                            })

                # night shift (reduced: only 22:00 – 02:00 sampled at 10-min intervals)
                for hour_offset in range(0, 4):
                    hour = (22 + hour_offset) % 24
                    next_day = day if hour >= 22 else day + timedelta(days=1)
                    shift = "Night"
                    for minute in range(0, 60, 10):
                        dt = datetime(next_day.year, next_day.month, next_day.day, hour, minute)
                        for s in sensors:
                            value = self._generate_sensor_value(
                                s, station_id, dt, shift, station_rush
                            )
                            all_rows.append({
                                "timestamp": dt.strftime("%Y-%m-%d %H:%M"),
                                "date": day_str,
                                "station_id": station_id,
                                "sensor_id": sensor_ids[(station_id, s["name"])],
                                "sensor_name": s["name"],
                                "value": value,
                                "unit": s["unit"],
                                "shift": shift,
                            })

        return pd.DataFrame(all_rows)

    # ── fact: production (unit-level) ────────────────────────────────────

    def _build_fact_production(self) -> pd.DataFrame:
        """One row per unit per station – tracks cycle time, queue time, cost,
        and pass/fail.  Quality outcomes are driven by sensor conditions."""
        print("  Generating production records...")

        operator_by_shift = {}
        for oid, name, shift, exp, skill in OPERATORS:
            operator_by_shift.setdefault(shift, []).append(oid)

        rows = []
        prod_id = 1

        for order in self.orders:
            day = order["date"]
            # assign to a shift (more orders on day shift)
            shift = random.choices(["Day", "Swing", "Night"], weights=[0.50, 0.35, 0.15])[0]
            shift_eff = SHIFTS[shift]["efficiency"]
            is_rush = order["priority"] in ("Rush", "Critical")

            for station in STATIONS:
                stn = station
                cycle_mean = stn["cycle_time_mean_min"] * order["complexity"]
                cycle_std = stn["cycle_time_std_min"]

                # Layer 5: rush orders compress cycle time but add variance
                if is_rush:
                    cycle_mean *= 0.82
                    cycle_std *= 1.40

                # Layer 6: bottleneck has higher queue
                queue_mean = 5.0
                if stn["station_id"] == "STN-04":
                    queue_mean = 25.0  # work backs up here
                elif stn["position"] == 5:
                    queue_mean = 3.0  # starved after bottleneck

                cycle_time = max(cycle_mean * 0.5, self.rng.normal(cycle_mean, cycle_std))
                queue_time = max(0, self.rng.exponential(queue_mean))
                setup_time = max(2, self.rng.normal(8, 2))

                # Operator assignment
                available_ops = operator_by_shift.get(shift, operator_by_shift["Day"])
                operator_id = random.choice(available_ops)

                # -- defect probability (Layer 2 + 4 + 5) --
                defect_rate = stn["base_defect_rate"]
                # shift effect
                defect_rate *= (1 / shift_eff)
                # rush effect
                if is_rush:
                    defect_rate *= 1.8
                # seasonal: summer humidity for cleanroom station
                if stn["station_id"] == "STN-05":
                    humidity = 42 + self._seasonal_humidity_offset(day)
                    if humidity > 55:
                        defect_rate *= 2.5
                    elif humidity > 50:
                        defect_rate *= 1.5
                # degradation: more defects when tools are worn
                days_maint = self._days_since_last_maintenance(stn["station_id"], day)
                defect_rate *= (1 + days_maint * 0.008)

                passed = self.rng.random() > defect_rate
                status = "Pass" if passed else "Fail"

                # costs
                machine_cost = round((cycle_time / 60) * (50 + stn["position"] * 10), 2)
                labor_cost = round((cycle_time / 60) * 38, 2)

                rows.append({
                    "production_id": f"PRD-{prod_id:06d}",
                    "order_id": order["order_id"],
                    "product_id": order["product_id"],
                    "station_id": stn["station_id"],
                    "operator_id": operator_id,
                    "date": day.strftime("%Y-%m-%d"),
                    "shift": shift,
                    "priority": order["priority"],
                    "cycle_time_min": round(cycle_time, 1),
                    "queue_time_min": round(queue_time, 1),
                    "setup_time_min": round(setup_time, 1),
                    "total_time_min": round(cycle_time + queue_time + setup_time, 1),
                    "machine_cost": machine_cost,
                    "labor_cost": labor_cost,
                    "material_cost": round(order["unit_material_cost"] / len(STATIONS), 2),
                    "quality_result": status,
                })
                prod_id += 1

        return pd.DataFrame(rows)

    # ── fact: quality events ─────────────────────────────────────────────

    def _build_fact_quality_events(self, fact_production: pd.DataFrame) -> pd.DataFrame:
        """Create detailed quality event records for every failed unit."""
        print("  Generating quality events...")

        defect_types_by_station = {
            "STN-01": ["Dimensional Out-of-Spec", "Surface Finish Defect", "Tool Mark", "Burr"],
            "STN-02": ["Solder Bridge", "Cold Joint", "Component Misalignment", "Tombstoning"],
            "STN-03": ["Winding Short", "Insulation Failure", "Torque Out-of-Spec", "Bearing Noise"],
            "STN-04": ["Joint Misalignment", "Fastener Under-Torque", "Wiring Error", "Clearance Violation"],
            "STN-05": ["Particulate Contamination", "Seal Failure", "Moisture Ingress", "Label Defect"],
            "STN-06": ["Accuracy Out-of-Spec", "Latency Exceeded", "Force Feedback Error", "Calibration Drift"],
        }

        root_causes_by_station = {
            "STN-01": ["Tool Wear", "Vibration", "Coolant Failure", "Material Variation"],
            "STN-02": ["Solder Temp Drift", "Placement Error", "Component Defect", "Ambient Temp"],
            "STN-03": ["Winding Tension", "Insulation Degradation", "Motor Overload", "Process Drift"],
            "STN-04": ["Operator Error", "Fixture Misalignment", "Component Tolerance Stack", "Fatigue"],
            "STN-05": ["Humidity Excursion", "Filter Degradation", "Glove Breach", "HVAC Failure"],
            "STN-06": ["Sensor Calibration", "Software Bug", "Electrical Noise", "Mechanical Wear"],
        }

        failed = fact_production[fact_production["quality_result"] == "Fail"]
        rows = []
        qid = 1

        for _, row in failed.iterrows():
            station_id = row["station_id"]
            severity = random.choices(
                ["Minor", "Major", "Critical"], weights=[0.50, 0.35, 0.15]
            )[0]
            disposition_map = {
                "Minor":    (["Rework", "Use-As-Is", "Scrap"], [0.75, 0.15, 0.10]),
                "Major":    (["Rework", "Scrap", "Use-As-Is"], [0.45, 0.45, 0.10]),
                "Critical": (["Scrap", "Rework", "Use-As-Is"], [0.70, 0.25, 0.05]),
            }
            opts, wts = disposition_map[severity]
            disposition = random.choices(opts, weights=wts)[0]

            rework_cost = 0.0
            scrap_cost = 0.0
            if disposition == "Rework":
                rework_cost = round(self.rng.uniform(50, 300), 2)
            elif disposition == "Scrap":
                scrap_cost = round(row["material_cost"] * self.rng.uniform(2, 6), 2)

            rows.append({
                "quality_event_id": f"QE-{qid:06d}",
                "production_id": row["production_id"],
                "order_id": row["order_id"],
                "product_id": row["product_id"],
                "station_id": station_id,
                "operator_id": row["operator_id"],
                "date": row["date"],
                "shift": row["shift"],
                "defect_type": random.choice(defect_types_by_station.get(station_id, ["Unknown"])),
                "severity": severity,
                "disposition": disposition,
                "root_cause": random.choice(root_causes_by_station.get(station_id, ["Unknown"])),
                "rework_cost": rework_cost,
                "scrap_cost": scrap_cost,
                "total_quality_cost": round(rework_cost + scrap_cost, 2),
                "corrective_action": random.choice([True, False]),
            })
            qid += 1

        return pd.DataFrame(rows)

    # ── fact: downtime ───────────────────────────────────────────────────

    def _build_fact_downtime(self) -> pd.DataFrame:
        """Generate downtime events. Unplanned breakdowns correlate with
        degradation – they happen more often when days-since-maintenance is high."""
        print("  Generating downtime events...")

        rows = []
        dt_id = 1
        total_days = (SIM_END - SIM_START).days

        for station in STATIONS:
            sid = station["station_id"]
            for cat, spec in DOWNTIME_CATS.items():
                freq = spec["monthly_freq"]

                # more breakdowns for bottleneck station
                if sid == "STN-04" and cat == "Unplanned Breakdown":
                    freq *= 1.6

                n_events = max(0, int(self.rng.poisson(freq * self.total_months)))

                for _ in range(n_events):
                    day_offset = int(self.rng.uniform(0, total_days))
                    event_date = SIM_START + timedelta(days=day_offset)
                    if event_date.weekday() >= 5:
                        continue

                    # unplanned breakdowns cluster when degradation is high
                    if cat == "Unplanned Breakdown":
                        days_maint = self._days_since_last_maintenance(sid, event_date)
                        # probability of keeping this event scales with degradation
                        keep_prob = min(1.0, 0.3 + days_maint * 0.04)
                        if self.rng.random() > keep_prob:
                            continue

                    duration = max(0.25, self.rng.normal(spec["avg_hrs"], spec["avg_hrs"] * 0.3))
                    hour = int(self.rng.integers(6, 22))
                    shift = self._shift_for_hour(hour)

                    hourly_cost = 50 + station["position"] * 10
                    lost_prod_cost = round(duration * hourly_cost, 2)
                    repair_cost = round(self.rng.uniform(100, 800), 2) if not spec["scheduled"] else 0
                    total_cost = round(lost_prod_cost + repair_cost, 2)

                    rows.append({
                        "downtime_id": f"DT-{dt_id:06d}",
                        "station_id": sid,
                        "date": event_date.strftime("%Y-%m-%d"),
                        "start_hour": hour,
                        "shift": shift,
                        "downtime_category": cat,
                        "is_scheduled": spec["scheduled"],
                        "duration_hours": round(duration, 2),
                        "lost_production_cost": lost_prod_cost,
                        "repair_cost": repair_cost,
                        "total_downtime_cost": total_cost,
                    })
                    dt_id += 1

        return pd.DataFrame(rows)

    # ── fact: alarms ─────────────────────────────────────────────────────

    def _build_fact_alarms(self, fact_sensor: pd.DataFrame) -> pd.DataFrame:
        """Derive alarm events from sensor readings that breach thresholds."""
        print("  Generating alarm events from sensor threshold breaches...")

        # build threshold lookup: sensor_name → (alarm_lo, alarm_hi)
        thresholds = {}
        for station_id, sensors in SENSOR_MAP.items():
            for s in sensors:
                thresholds[s["name"]] = (s["alarm_lo"], s["alarm_hi"])

        alarm_rows = []
        aid = 1

        for _, row in fact_sensor.iterrows():
            lo, hi = thresholds.get(row["sensor_name"], (None, None))
            breached = False
            alarm_type = ""
            if lo is not None and row["value"] < lo:
                breached = True
                alarm_type = "Low"
            elif hi is not None and row["value"] > hi:
                breached = True
                alarm_type = "High"

            if breached:
                alarm_rows.append({
                    "alarm_id": f"ALM-{aid:06d}",
                    "timestamp": row["timestamp"],
                    "date": row["date"],
                    "station_id": row["station_id"],
                    "sensor_id": row["sensor_id"],
                    "sensor_name": row["sensor_name"],
                    "alarm_type": alarm_type,
                    "value": row["value"],
                    "threshold": lo if alarm_type == "Low" else hi,
                    "shift": row["shift"],
                })
                aid += 1

        return pd.DataFrame(alarm_rows)

    # ── orchestrator ─────────────────────────────────────────────────────

    def run(self) -> dict[str, pd.DataFrame]:
        """Execute full simulation and return all tables."""
        print("=" * 60)
        print("SCADA Simulation: Robotic Arm Assembly Line")
        print(f"Period: {SIM_START.date()} to {SIM_END.date()}")
        print(f"Working days: {len(self.workdays)}")
        print("=" * 60)

        print("\n[1/9] Building dimension tables...")
        self.tables["dim_stations"]  = self._build_dim_stations()
        self.tables["dim_sensors"]   = self._build_dim_sensors()
        self.tables["dim_operators"] = self._build_dim_operators()
        self.tables["dim_shifts"]    = self._build_dim_shifts()
        self.tables["dim_products"]  = self._build_dim_products()
        self.tables["dim_date"]      = self._build_dim_date()

        print("[2/9] Scheduling maintenance windows...")
        self._schedule_maintenance()

        print("[3/9] Generating order schedule...")
        self._generate_orders()
        print(f"       {len(self.orders):,} orders generated")

        print("[4/9] Generating sensor readings...")
        self.tables["fact_sensor_readings"] = self._build_fact_sensor_readings()

        print("[5/9] Generating production records...")
        self.tables["fact_production"] = self._build_fact_production()

        print("[6/9] Generating quality events...")
        self.tables["fact_quality_events"] = self._build_fact_quality_events(
            self.tables["fact_production"]
        )

        print("[7/9] Generating downtime events...")
        self.tables["fact_downtime"] = self._build_fact_downtime()

        print("[8/9] Deriving alarm events...")
        self.tables["fact_alarms"] = self._build_fact_alarms(
            self.tables["fact_sensor_readings"]
        )

        print("\n[9/9] Done!")
        print("\n--- Table Summary ---")
        for name, df in self.tables.items():
            print(f"  {name:.<35s} {len(df):>10,} rows × {len(df.columns)} cols")

        return self.tables
