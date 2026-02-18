-- ============================================================
-- DAX Measures for SCADA Manufacturing Dashboard
-- ============================================================
-- HOW TO USE: In Power BI Desktop, click on a table in the
-- Fields pane → New Measure → paste each formula below.
-- Create all measures inside fact_production (or a dedicated
-- "Measures" table if you prefer).
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. PRODUCTION & THROUGHPUT
-- ────────────────────────────────────────────────────────────

Total Units = COUNTROWS(fact_production)

Units Passed = CALCULATE(COUNTROWS(fact_production), fact_production[quality_result] = "Pass")

Units Failed = CALCULATE(COUNTROWS(fact_production), fact_production[quality_result] = "Fail")

Defect Rate = DIVIDE([Units Failed], [Total Units], 0)

Avg Cycle Time = AVERAGE(fact_production[cycle_time_min])

Avg Queue Time = AVERAGE(fact_production[queue_time_min])

Avg Setup Time = AVERAGE(fact_production[setup_time_min])

Avg Total Time = AVERAGE(fact_production[total_time_min])

Throughput Per Day =
    DIVIDE(
        [Total Units],
        DISTINCTCOUNT(fact_production[date]),
        0
    )


-- ────────────────────────────────────────────────────────────
-- 2. OEE (Overall Equipment Effectiveness)
-- ────────────────────────────────────────────────────────────
-- OEE = Availability × Performance × Quality
-- Simplified model using the data we have.

-- Availability: uptime / (uptime + downtime)
Total Downtime Hours = SUM(fact_downtime[duration_hours])

Planned Hours =
    DISTINCTCOUNT(fact_production[date]) * 16
    -- 16 operating hrs/day (Day + Swing shifts)

Availability =
    DIVIDE(
        [Planned Hours] - [Total Downtime Hours],
        [Planned Hours],
        0
    )

-- Performance: actual cycle / target cycle
Performance =
    DIVIDE(
        AVERAGE(fact_production[cycle_time_min]),
        RELATED(dim_stations[target_cycle_time_min]),
        0
    )
    -- NOTE: This measure only works when filtered to a single station.
    -- For a card visual, use the simplified version below instead:

Performance Ratio =
    DIVIDE(
        SUMX(fact_production, fact_production[cycle_time_min]),
        SUMX(
            fact_production,
            RELATED(dim_stations[target_cycle_time_min])
        ),
        0
    )

-- Quality: good units / total units
Quality Rate = DIVIDE([Units Passed], [Total Units], 0)

-- Combined OEE
OEE = [Availability] * DIVIDE(1, [Performance Ratio], 0) * [Quality Rate]
    -- NOTE: Performance inverted because faster = better


-- ────────────────────────────────────────────────────────────
-- 3. COST METRICS
-- ────────────────────────────────────────────────────────────

Total Machine Cost = SUM(fact_production[machine_cost])

Total Labor Cost = SUM(fact_production[labor_cost])

Total Material Cost = SUM(fact_production[material_cost])

Total Production Cost = [Total Machine Cost] + [Total Labor Cost] + [Total Material Cost]

Total Rework Cost = SUM(fact_quality_events[rework_cost])

Total Scrap Cost = SUM(fact_quality_events[scrap_cost])

Total Quality Cost = SUM(fact_quality_events[total_quality_cost])

Total Downtime Cost = SUM(fact_downtime[total_downtime_cost])

Cost Per Unit = DIVIDE([Total Production Cost], [Total Units], 0)

Cost of Poor Quality = [Total Quality Cost] + [Total Downtime Cost]


-- ────────────────────────────────────────────────────────────
-- 4. ALARM & SENSOR METRICS
-- ────────────────────────────────────────────────────────────

Total Alarms = COUNTROWS(fact_alarms)

High Alarms = CALCULATE(COUNTROWS(fact_alarms), fact_alarms[alarm_type] = "High")

Low Alarms = CALCULATE(COUNTROWS(fact_alarms), fact_alarms[alarm_type] = "Low")

Alarms Per Day =
    DIVIDE(
        [Total Alarms],
        DISTINCTCOUNT(fact_alarms[date]),
        0
    )

Avg Sensor Value = AVERAGE(fact_sensor_readings[value])


-- ────────────────────────────────────────────────────────────
-- 5. DOWNTIME METRICS
-- ────────────────────────────────────────────────────────────

Total Downtime Events = COUNTROWS(fact_downtime)

Unplanned Downtime Hours =
    CALCULATE(
        SUM(fact_downtime[duration_hours]),
        fact_downtime[is_scheduled] = FALSE
    )

Planned Downtime Hours =
    CALCULATE(
        SUM(fact_downtime[duration_hours]),
        fact_downtime[is_scheduled] = TRUE
    )

Avg Downtime Duration = AVERAGE(fact_downtime[duration_hours])

MTBF (Mean Time Between Failures) =
    DIVIDE(
        [Planned Hours],
        CALCULATE(
            COUNTROWS(fact_downtime),
            fact_downtime[downtime_category] = "Unplanned Breakdown"
        ),
        0
    )
    -- Hours of operation per unplanned breakdown


-- ────────────────────────────────────────────────────────────
-- 6. SHIFT COMPARISON
-- ────────────────────────────────────────────────────────────

Day Shift Defect Rate =
    CALCULATE([Defect Rate], fact_production[shift] = "Day")

Swing Shift Defect Rate =
    CALCULATE([Defect Rate], fact_production[shift] = "Swing")

Night Shift Defect Rate =
    CALCULATE([Defect Rate], fact_production[shift] = "Night")


-- ────────────────────────────────────────────────────────────
-- 7. RUSH ORDER IMPACT
-- ────────────────────────────────────────────────────────────

Rush Defect Rate =
    CALCULATE(
        [Defect Rate],
        fact_production[priority] IN {"Rush", "Critical"}
    )

Standard Defect Rate =
    CALCULATE(
        [Defect Rate],
        fact_production[priority] = "Standard"
    )

Rush Cost Premium =
    DIVIDE([Rush Defect Rate], [Standard Defect Rate], 0) - 1
    -- e.g., 0.65 means rush orders have 65% higher defect rate
