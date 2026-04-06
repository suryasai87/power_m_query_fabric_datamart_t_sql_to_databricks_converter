"""Health check and debug endpoints."""
from pathlib import Path
from fastapi import APIRouter, HTTPException
from databricks import sql

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH, AVAILABLE_MODELS

router = APIRouter()


@router.get("/health")
async def health_check():
    return {"status": "healthy", "version": "2.0.0"}


@router.get("/debug")
async def debug_info():
    """Debug endpoint to check runtime environment"""
    current_file = Path(__file__).resolve()
    current_dir = current_file.parent
    cwd = Path.cwd()

    # Test all possible paths
    path_checks = {}
    static_dir = Path(__file__).resolve().parent.parent.parent / "static"
    test_paths = [
        ("Path(__file__).parent / static", current_dir / "static"),
        ("Path.cwd() / static", cwd / "static"),
        ("Path('static').resolve()", Path("static").resolve()),
    ]

    for name, path in test_paths:
        path_checks[name] = {
            "path": str(path),
            "exists": path.exists(),
            "is_dir": path.is_dir() if path.exists() else False,
            "has_index": (path / "index.html").exists() if path.exists() else False,
            "has_static_subdir": (path / "static").exists() if path.exists() else False
        }

    return {
        "cwd": str(cwd),
        "current_file": str(current_file),
        "current_dir": str(current_dir),
        "static_dir": str(static_dir),
        "static_dir_exists": static_dir.exists(),
        "static_assets_exists": (static_dir / "static").exists() if static_dir.exists() else False,
        "path_checks": path_checks,
        "files_in_cwd": sorted([f.name for f in cwd.iterdir()])[:30] if cwd.exists() else [],
        "files_in_current_dir": sorted([f.name for f in current_dir.iterdir()])[:30] if current_dir.exists() else [],
        "static_files": sorted([f.name for f in static_dir.iterdir()])[:30] if static_dir.exists() else []
    }


@router.get("/models")
async def list_models():
    """List available foundation models"""
    return {"models": list(AVAILABLE_MODELS.values())}


@router.get("/warehouse-status")
async def get_warehouse_status():
    """Get SQL warehouse status"""
    try:
        # Extract warehouse ID from HTTP path
        warehouse_id = DATABRICKS_HTTP_PATH.split("/")[-1] if DATABRICKS_HTTP_PATH else None

        if not warehouse_id:
            return {
                "warehouse_id": None,
                "warehouse_name": "Not configured",
                "status": "UNKNOWN",
                "http_path": DATABRICKS_HTTP_PATH
            }

        # Try to connect to get warehouse info
        try:
            with sql.connect(
                server_hostname=DATABRICKS_HOST.replace("https://", ""),
                http_path=DATABRICKS_HTTP_PATH,
                access_token=DATABRICKS_TOKEN
            ) as connection:
                # Connection successful means warehouse is running
                return {
                    "warehouse_id": warehouse_id,
                    "warehouse_name": f"Warehouse {warehouse_id}",
                    "status": "RUNNING",
                    "http_path": DATABRICKS_HTTP_PATH
                }
        except Exception as conn_error:
            return {
                "warehouse_id": warehouse_id,
                "warehouse_name": f"Warehouse {warehouse_id}",
                "status": "STOPPED",
                "http_path": DATABRICKS_HTTP_PATH,
                "error": str(conn_error)
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get warehouse status: {str(e)}")
