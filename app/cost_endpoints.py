# Cost Estimation Endpoints for DW Migration Assistant

# This code should be inserted into main.py before the static file serving routes

# ============================================
# COST ESTIMATION ENDPOINTS
# ============================================

# DBU rates per hour for SQL Serverless (approximate)
DBU_RATE_SERVERLESS = 0.22  # USD per DBU
STORAGE_RATE_GB_MONTH = 0.023  # USD per GB per month for Delta Lake
NETWORK_TRANSFER_RATE_GB = 0.09  # USD per GB for data transfer

# Warehouse size to DBU mapping (SQL Serverless)
WAREHOUSE_DBU_RATES = {
    "X-Small": 1,
    "Small": 2,
    "Medium": 4,
    "Large": 8,
    "X-Large": 16,
    "2X-Large": 32,
    "3X-Large": 64,
    "4X-Large": 128
}

def estimate_sql_tokens(num_objects: int, complexity: str = "medium") -> Dict[str, int]:
    """Estimate token usage for SQL translation"""
    # Base tokens per object (prompt + completion)
    base_prompt = 500  # System prompt + context
    base_completion = 300

    complexity_multipliers = {
        "low": 0.5,
        "medium": 1.0,
        "high": 2.0
    }

    multiplier = complexity_multipliers.get(complexity, 1.0)

    # Average SQL object token counts
    avg_prompt_tokens = int(base_prompt * multiplier)
    avg_completion_tokens = int(base_completion * multiplier)

    total_prompt = avg_prompt_tokens * num_objects
    total_completion = avg_completion_tokens * num_objects

    return {
        "prompt_tokens": total_prompt,
        "completion_tokens": total_completion,
        "total_tokens": total_prompt + total_completion
    }

@app.post("/api/estimate/migration", response_model=MigrationCostEstimateResponse)
async def estimate_migration_cost(request: MigrationCostEstimateRequest):
    """Estimate total migration costs including LLM, compute, storage, and network"""
    try:
        total_objects = request.num_tables + request.num_views + request.num_procedures

        # 1. LLM Translation Costs
        token_estimate = estimate_sql_tokens(total_objects, request.avg_sql_complexity)
        llm_cost = calculate_llm_cost(
            request.model_id,
            token_estimate["prompt_tokens"],
            token_estimate["completion_tokens"]
        )

        # 2. Compute Costs for Migration
        # Estimate time based on number of objects and complexity
        complexity_time_multipliers = {"low": 0.5, "medium": 1.0, "high": 2.0}
        time_multiplier = complexity_time_multipliers.get(request.avg_sql_complexity, 1.0)

        # Base: 30 seconds per object for translation + validation
        estimated_seconds = total_objects * 30 * time_multiplier
        estimated_hours = estimated_seconds / 3600

        # Use Medium warehouse for migrations (4 DBUs)
        migration_dbus = WAREHOUSE_DBU_RATES["Medium"]
        compute_cost = estimated_hours * migration_dbus * DBU_RATE_SERVERLESS

        # 3. Storage Costs (annual)
        storage_cost_annual = request.data_size_gb * STORAGE_RATE_GB_MONTH * 12

        # 4. Network Transfer Costs (one-time data transfer)
        # Estimate 10% of data size for network transfer overhead
        network_cost = request.data_size_gb * NETWORK_TRANSFER_RATE_GB * 0.1

        # Total
        total_cost = llm_cost + compute_cost + storage_cost_annual + network_cost

        breakdown = CostBreakdown(
            llm_translation=round(llm_cost, 2),
            compute_migration=round(compute_cost, 2),
            storage_annual=round(storage_cost_annual, 2),
            network_transfer=round(network_cost, 2),
            total=round(total_cost, 2)
        )

        details = {
            "num_objects": total_objects,
            "token_estimate": token_estimate,
            "llm_model": request.model_id,
            "warehouse_size": "Medium",
            "warehouse_dbus": migration_dbus,
            "estimated_seconds": int(estimated_seconds),
            "data_size_gb": request.data_size_gb,
            "total_rows": request.total_rows,
            "source_type": request.source_type,
            "complexity": request.avg_sql_complexity
        }

        return MigrationCostEstimateResponse(
            success=True,
            breakdown=breakdown,
            estimated_duration_hours=round(estimated_hours, 2),
            details=details
        )

    except Exception as e:
        return MigrationCostEstimateResponse(
            success=False,
            error=str(e)
        )

@app.get("/api/estimate/storage")
async def estimate_storage_cost(data_size_gb: float = 100.0, months: int = 12):
    """Estimate storage costs for Delta Lake"""
    try:
        monthly_cost = data_size_gb * STORAGE_RATE_GB_MONTH
        total_cost = monthly_cost * months

        return {
            "success": True,
            "data_size_gb": data_size_gb,
            "months": months,
            "monthly_cost": round(monthly_cost, 2),
            "total_cost": round(total_cost, 2),
            "rate_per_gb_month": STORAGE_RATE_GB_MONTH
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/estimate/compute")
async def estimate_compute_cost(warehouse_size: str = "Medium", hours: float = 100.0):
    """Estimate SQL Warehouse compute costs"""
    try:
        if warehouse_size not in WAREHOUSE_DBU_RATES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid warehouse size. Must be one of: {list(WAREHOUSE_DBU_RATES.keys())}"
            )

        dbus = WAREHOUSE_DBU_RATES[warehouse_size]
        cost_per_hour = dbus * DBU_RATE_SERVERLESS
        total_cost = cost_per_hour * hours

        return {
            "success": True,
            "warehouse_size": warehouse_size,
            "dbus": dbus,
            "hours": hours,
            "cost_per_hour": round(cost_per_hour, 2),
            "total_cost": round(total_cost, 2),
            "dbu_rate": DBU_RATE_SERVERLESS
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/estimate/compare")
async def compare_costs(request: CostComparisonRequest):
    """Compare costs between different migration approaches"""
    try:
        # Get base migration estimate
        migration_estimate = await estimate_migration_cost(request.migration_request)

        if not migration_estimate.success:
            return {"success": False, "error": "Failed to estimate migration cost"}

        # Calculate ongoing costs
        monthly_storage = request.migration_request.data_size_gb * STORAGE_RATE_GB_MONTH
        monthly_compute = request.compute_hours_monthly * WAREHOUSE_DBU_RATES["Medium"] * DBU_RATE_SERVERLESS

        # Year 1: Migration + ongoing
        year1_total = (
            migration_estimate.breakdown.llm_translation +
            migration_estimate.breakdown.compute_migration +
            migration_estimate.breakdown.network_transfer +
            (monthly_storage * 12) +
            (monthly_compute * 12)
        )

        # Year 2+: Only ongoing costs
        yearly_ongoing = (monthly_storage * 12) + (monthly_compute * 12)

        # Compare with different models
        model_comparisons = []
        for model_key, model_info in AVAILABLE_MODELS.items():
            total_objects = (
                request.migration_request.num_tables +
                request.migration_request.num_views +
                request.migration_request.num_procedures
            )
            token_estimate = estimate_sql_tokens(
                total_objects,
                request.migration_request.avg_sql_complexity
            )
            model_llm_cost = calculate_llm_cost(
                model_info["id"],
                token_estimate["prompt_tokens"],
                token_estimate["completion_tokens"]
            )
            model_comparisons.append({
                "model_name": model_info["name"],
                "model_id": model_info["id"],
                "llm_cost": round(model_llm_cost, 2),
                "total_migration_cost": round(
                    model_llm_cost +
                    migration_estimate.breakdown.compute_migration +
                    migration_estimate.breakdown.network_transfer,
                    2
                )
            })

        # Sort by cost
        model_comparisons.sort(key=lambda x: x["llm_cost"])

        return {
            "success": True,
            "base_estimate": migration_estimate.dict(),
            "monthly_costs": {
                "storage": round(monthly_storage, 2),
                "compute": round(monthly_compute, 2),
                "total": round(monthly_storage + monthly_compute, 2)
            },
            "yearly_costs": {
                "year_1": round(year1_total, 2),
                "year_2_onwards": round(yearly_ongoing, 2)
            },
            "model_comparisons": model_comparisons,
            "cost_breakdown_3_years": {
                "migration_one_time": round(
                    migration_estimate.breakdown.llm_translation +
                    migration_estimate.breakdown.compute_migration +
                    migration_estimate.breakdown.network_transfer,
                    2
                ),
                "storage_3_years": round(monthly_storage * 36, 2),
                "compute_3_years": round(monthly_compute * 36, 2),
                "total_3_years": round(
                    migration_estimate.breakdown.llm_translation +
                    migration_estimate.breakdown.compute_migration +
                    migration_estimate.breakdown.network_transfer +
                    (monthly_storage * 36) +
                    (monthly_compute * 36),
                    2
                )
            }
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
