"""Utility functions for SQL validation, cost calculation, and identifier handling."""
import re
from typing import Optional, Dict
from fastapi import HTTPException
from app.config import AVAILABLE_MODELS


def calculate_llm_cost(model_id: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Calculate estimated cost in USD for LLM usage"""
    # Find pricing for the model
    pricing = None
    for model_key, model_info in AVAILABLE_MODELS.items():
        if model_info["id"] == model_id:
            pricing = model_info["pricing"]
            break

    if not pricing:
        # Default pricing if model not found
        pricing = {"input": 0.15, "output": 0.60}

    input_cost = (prompt_tokens / 1_000_000) * pricing["input"]
    output_cost = (completion_tokens / 1_000_000) * pricing["output"]
    return input_cost + output_cost


def validate_identifier(name: str, identifier_type: str = "identifier") -> str:
    """
    Validate SQL identifiers (catalog, schema, table names) to prevent SQL injection.
    Only allows alphanumeric characters, underscores, and hyphens.
    """
    if not name:
        raise HTTPException(status_code=400, detail=f"Invalid {identifier_type}: cannot be empty")

    # Check for valid identifier pattern
    # Allow: alphanumeric, underscore, hyphen, and backticks for quoting
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_\-]*$'

    # Remove backticks if present (they're used for quoting in SQL)
    clean_name = name.strip('`')

    if not re.match(pattern, clean_name):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {identifier_type}: '{name}'. Only alphanumeric characters, underscores, and hyphens are allowed."
        )

    # Check max length (Databricks limit is 255)
    if len(clean_name) > 255:
        raise HTTPException(status_code=400, detail=f"Invalid {identifier_type}: exceeds maximum length of 255 characters")

    return clean_name


def quote_identifier(name: str) -> str:
    """Safely quote a SQL identifier using backticks."""
    clean_name = validate_identifier(name)
    return f"`{clean_name}`"


def estimate_sql_tokens(num_objects: int, complexity: str = "medium") -> Dict[str, int]:
    """Estimate token usage for SQL translation"""
    base_prompt = 500
    base_completion = 300
    complexity_multipliers = {"low": 0.5, "medium": 1.0, "high": 2.0}
    multiplier = complexity_multipliers.get(complexity, 1.0)
    avg_prompt_tokens = int(base_prompt * multiplier)
    avg_completion_tokens = int(base_completion * multiplier)
    total_prompt = avg_prompt_tokens * num_objects
    total_completion = avg_completion_tokens * num_objects
    return {"prompt_tokens": total_prompt, "completion_tokens": total_completion, "total_tokens": total_prompt + total_completion}


def validate_sql_syntax(query: str) -> tuple[bool, Optional[str]]:
    """Basic SQL syntax validation"""
    query = query.strip()
    if not query:
        return False, "Empty query"

    # Remove comments and whitespace
    query_upper = re.sub(r'--.*$', '', query, flags=re.MULTILINE).strip().upper()

    # Check for dangerous operations
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE OR REPLACE']
    for keyword in dangerous_keywords:
        if keyword in query_upper and 'TABLE' in query_upper:
            return False, f"Query contains potentially dangerous operation: {keyword}"

    # Basic syntax check - must start with SELECT, WITH, or CREATE
    valid_starts = ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN']
    if not any(query_upper.startswith(start) for start in valid_starts):
        return False, "Query must start with SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN"

    return True, None
