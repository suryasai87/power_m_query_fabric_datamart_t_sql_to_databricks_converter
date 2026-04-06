"""SQL translation, execution, DDL conversion, and AI suggestion endpoints."""
import time
import re
import requests
from fastapi import APIRouter, HTTPException
from databricks import sql

from app.config import (
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH, LLM_AGENT_ENDPOINT
)
from app.models import (
    TranslateSqlRequest, TranslateSqlResponse,
    ExecuteSqlRequest, ExecuteSqlResponse,
    ConvertDdlRequest, ConvertDdlResponse,
    BusinessLogicSuggestionRequest, JoinConditionSuggestionRequest, GenerateSqlRequest,
)
from app.utils import calculate_llm_cost, quote_identifier
from app.dependencies import OPENAI_AVAILABLE, openai, get_llm_client, get_databricks_connection

router = APIRouter()


@router.post("/translate-sql", response_model=TranslateSqlResponse)
async def translate_sql(request: TranslateSqlRequest):
    """Translate SQL from source system to Databricks SQL using Foundation Model"""
    start_time = time.time()

    try:
        # Check if OpenAI is available
        if not OPENAI_AVAILABLE or openai is None:
            return TranslateSqlResponse(
                success=False,
                translatedSql="",
                error="OpenAI library not available. Please ensure openai is installed."
            )

        # Check if credentials are configured
        if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
            return TranslateSqlResponse(
                success=False,
                translatedSql="",
                error="Databricks credentials not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN."
            )

        # Initialize OpenAI client with Databricks endpoint
        client = get_llm_client()

        # Create prompt for SQL translation
        system_prompt = f"""You are an expert SQL translator specializing in migrating SQL from {request.sourceSystem} to Databricks SQL.

Your task is to:
1. Analyze the input SQL from {request.sourceSystem}
2. Convert it to valid Databricks SQL syntax
3. Handle dialect-specific functions, data types, and syntax differences
4. Preserve the original logic and intent
5. Add comments for significant changes

Important considerations:
- Use Databricks SQL functions and syntax
- Handle data type conversions correctly
- Preserve column names and aliases
- Maintain query structure and joins
- Use appropriate Databricks-specific optimizations where applicable"""

        user_prompt = f"""Translate the following {request.sourceSystem} SQL to Databricks SQL:

```sql
{request.sourceSql}
```

Provide ONLY the translated Databricks SQL without explanations. If there are important conversion notes, add them as SQL comments."""

        # Call Databricks Foundation Model
        response = client.chat.completions.create(
            model=request.modelId,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=2000,
            temperature=0.1  # Low temperature for more deterministic translations
        )

        translated_sql = response.choices[0].message.content.strip()

        # Remove markdown code blocks if present
        if translated_sql.startswith("```sql"):
            translated_sql = translated_sql[6:]
        if translated_sql.startswith("```"):
            translated_sql = translated_sql[3:]
        if translated_sql.endswith("```"):
            translated_sql = translated_sql[:-3]
        translated_sql = translated_sql.strip()

        # Extract token usage
        prompt_tokens = response.usage.prompt_tokens if response.usage else 0
        completion_tokens = response.usage.completion_tokens if response.usage else 0
        total_tokens = response.usage.total_tokens if response.usage else 0

        # Calculate cost
        estimated_cost = calculate_llm_cost(request.modelId, prompt_tokens, completion_tokens)

        # Calculate execution time
        execution_time_ms = int((time.time() - start_time) * 1000)

        return TranslateSqlResponse(
            success=True,
            translatedSql=translated_sql,
            modelUsed=request.modelId,
            promptTokens=prompt_tokens,
            completionTokens=completion_tokens,
            totalTokens=total_tokens,
            estimatedCost=estimated_cost,
            executionTimeMs=execution_time_ms
        )

    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        return TranslateSqlResponse(
            success=False,
            translatedSql="",
            error=f"Translation failed: {str(e)}",
            executionTimeMs=execution_time_ms
        )


@router.post("/execute-sql", response_model=ExecuteSqlResponse)
async def execute_sql(request: ExecuteSqlRequest):
    """Execute SQL in Databricks SQL"""
    try:
        if not DATABRICKS_TOKEN or not DATABRICKS_HTTP_PATH:
            return ExecuteSqlResponse(
                success=False,
                error="Databricks credentials not configured"
            )

        # Validate catalog and schema names to prevent SQL injection
        try:
            safe_catalog = quote_identifier(request.catalog)
            safe_schema = quote_identifier(request.schema)
        except HTTPException as e:
            return ExecuteSqlResponse(success=False, error=e.detail)

        sql_text = request.sql.strip()
        if sql_text.upper().startswith('SELECT') and 'LIMIT' not in sql_text.upper():
            sql_text = f"{sql_text} LIMIT 1"

        full_sql = f"USE CATALOG {safe_catalog}; USE SCHEMA {safe_schema}; {sql_text}"

        with get_databricks_connection() as connection:
            with connection.cursor() as cursor:
                start_time = time.time()
                cursor.execute(full_sql)
                execution_time = time.time() - start_time

                result = None
                row_count = 0
                if sql_text.upper().startswith('SELECT'):
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    result = [dict(zip(columns, row)) for row in rows]
                    row_count = len(result)

                return ExecuteSqlResponse(
                    success=True,
                    result=result,
                    rowCount=row_count,
                    executionTime=round(execution_time, 3)
                )

    except Exception as e:
        return ExecuteSqlResponse(
            success=False,
            error=str(e)
        )


@router.post("/convert-ddl", response_model=ConvertDdlResponse)
async def convert_ddl(request: ConvertDdlRequest):
    """Convert DDL from source system to Databricks SQL DDL"""
    try:
        if not LLM_AGENT_ENDPOINT:
            return ConvertDdlResponse(
                success=False,
                convertedDdl="",
                error="LLM Agent endpoint not configured"
            )

        payload = {
            "source_system": request.sourceSystem,
            "source_ddl": request.sourceDdl,
            "target_catalog": request.targetCatalog,
            "target_schema": request.targetSchema
        }

        response = requests.post(
            f"{LLM_AGENT_ENDPOINT}/convert-ddl",
            json=payload,
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            timeout=60
        )

        if response.status_code != 200:
            return ConvertDdlResponse(
                success=False,
                convertedDdl="",
                error=f"LLM API error: {response.text}"
            )

        result = response.json()
        converted_ddl = result.get("converted_ddl", "")
        warnings = result.get("warnings", [])

        executed = False
        if request.executeImmediately and converted_ddl:
            try:
                exec_result = await execute_sql(ExecuteSqlRequest(
                    sql=converted_ddl,
                    catalog=request.targetCatalog,
                    schema=request.targetSchema
                ))
                executed = exec_result.success
                if not executed:
                    warnings.append(f"Execution failed: {exec_result.error}")
            except Exception as e:
                warnings.append(f"Execution error: {str(e)}")

        return ConvertDdlResponse(
            success=True,
            convertedDdl=converted_ddl,
            executed=executed,
            warnings=warnings
        )

    except Exception as e:
        return ConvertDdlResponse(
            success=False,
            convertedDdl="",
            error=str(e)
        )


@router.post("/suggest-business-logic")
async def suggest_business_logic(request: BusinessLogicSuggestionRequest):
    """Generate business logic suggestions using Foundation Model"""
    start_time = time.time()

    try:
        if not OPENAI_AVAILABLE or openai is None:
            return {"suggestions": [], "error": "OpenAI library not available"}

        client = get_llm_client()

        full_table_name = f"{request.catalog}.{request.schema_name}.{request.table}"
        context = f"Table: {full_table_name}\nColumns: {', '.join(request.columns)}"

        system_prompt = """You are a helpful data analyst assistant. Generate 5 diverse business logic examples for analyzing data. Each suggestion should be a clear, natural language business question."""

        user_prompt = f"""Based on this table:\n{context}\n\nGenerate 5 business logic examples. Format as numbered list (1-5)."""

        response = client.chat.completions.create(
            model=request.model_id,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=300,
            temperature=0.7
        )

        suggestions_text = response.choices[0].message.content.strip()

        suggestions = []
        for line in suggestions_text.split('\n'):
            match = re.match(r'^\d+\.\s*(.+)$', line.strip())
            if match:
                suggestion = match.group(1).strip().strip('"').strip("'")
                if len(suggestion) > 15:
                    suggestions.append(suggestion)

        return {
            "suggestions": suggestions[:5],
            "model_used": request.model_id,
            "execution_time_ms": int((time.time() - start_time) * 1000)
        }
    except Exception as e:
        return {"suggestions": [], "error": str(e)}


@router.post("/suggest-join-conditions")
async def suggest_join_conditions(request: JoinConditionSuggestionRequest):
    """Suggest JOIN conditions using Foundation Model"""
    start_time = time.time()

    try:
        if not OPENAI_AVAILABLE or openai is None:
            return {"suggestions": [], "error": "OpenAI library not available"}

        if len(request.tables) < 2:
            raise HTTPException(status_code=400, detail="At least 2 tables required")

        client = get_llm_client()

        tables_context = ""
        for idx, table in enumerate(request.tables, 1):
            full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
            tables_context += f"\nTable {idx}: {full_name}\nColumns: {', '.join(table.columns)}\n"

        system_prompt = """You are a SQL expert. Suggest JOIN conditions based on column names."""
        user_prompt = f"""Based on:\n{tables_context}\n\nSuggest 3 JOIN conditions. Format:\n1. table1.col = table2.col\n2. table1.col = table2.col\n3. table1.col = table2.col"""

        response = client.chat.completions.create(
            model=request.model_id,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=200,
            temperature=0.3
        )

        suggestions = []
        for line in response.choices[0].message.content.strip().split('\n'):
            match = re.match(r'^\d+\.\s*(.+)$', line.strip())
            if match:
                suggestions.append(match.group(1).strip())

        return {
            "suggestions": suggestions[:3],
            "model_used": request.model_id,
            "execution_time_ms": int((time.time() - start_time) * 1000)
        }
    except Exception as e:
        return {"suggestions": [], "error": str(e)}


@router.post("/generate-sql")
async def generate_sql(request: GenerateSqlRequest):
    """Generate SQL query from business logic using Foundation Model"""
    start_time = time.time()

    try:
        if not OPENAI_AVAILABLE or openai is None:
            return {"success": False, "generated_sql": "", "error": "OpenAI library not available"}

        client = get_llm_client()

        tables_context = ""
        for table in request.tables:
            full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
            tables_context += f"\nTable: {full_name}\nColumns: {', '.join(table.columns)}\n"

        join_info = f"\nJOIN conditions: {request.join_conditions}" if request.join_conditions else ""
        user_prompt = f"""Tables:\n{tables_context}{join_info}\n\nBusiness Logic: {request.business_logic}\n\nGenerate Databricks SQL (no explanations)."""

        response = client.chat.completions.create(
            model=request.model_id,
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate optimized Databricks SQL."},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=1000,
            temperature=0.1
        )

        generated_sql = response.choices[0].message.content.strip()
        if generated_sql.startswith("```sql"):
            generated_sql = generated_sql[6:]
        if generated_sql.startswith("```"):
            generated_sql = generated_sql[3:]
        if generated_sql.endswith("```"):
            generated_sql = generated_sql[:-3]
        generated_sql = generated_sql.strip()

        prompt_tokens = response.usage.prompt_tokens if response.usage else 0
        completion_tokens = response.usage.completion_tokens if response.usage else 0

        return {
            "success": True,
            "generated_sql": generated_sql,
            "model_used": request.model_id,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
            "estimated_cost": calculate_llm_cost(request.model_id, prompt_tokens, completion_tokens),
            "execution_time_ms": int((time.time() - start_time) * 1000)
        }
    except Exception as e:
        return {"success": False, "generated_sql": "", "error": str(e)}
