"""DW Migration Assistant — FastAPI backend."""
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="DW Migration Assistant API", description="API for data warehouse migration to Databricks SQL", version="2.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

from app.routers import health, translation, catalog, connection, migration, schedule, compare, cost, testing, rollback

app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(translation.router, prefix="/api", tags=["translation"])
app.include_router(catalog.router, prefix="/api", tags=["catalog"])
app.include_router(connection.router, prefix="/api", tags=["connection"])
app.include_router(migration.router, prefix="/api", tags=["migration"])
app.include_router(schedule.router, prefix="/api", tags=["schedule"])
app.include_router(compare.router, prefix="/api", tags=["compare"])
app.include_router(cost.router, prefix="/api", tags=["cost"])
app.include_router(testing.router, prefix="/api", tags=["testing"])
app.include_router(rollback.router, prefix="/api", tags=["rollback"])

# Also mount health without /api prefix for Databricks Apps proxy
app.include_router(health.router, tags=["health-root"])

# Static files
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    static_assets = static_dir / "static"
    if static_assets.exists():
        app.mount("/static", StaticFiles(directory=str(static_assets)), name="static-assets")

    @app.get("/favicon.ico")
    async def favicon():
        path = static_dir / "favicon.ico"
        return FileResponse(str(path)) if path.exists() else HTTPException(404)

    @app.get("/manifest.json")
    async def manifest():
        path = static_dir / "manifest.json"
        return FileResponse(str(path)) if path.exists() else HTTPException(404)

    @app.get("/{full_path:path}")
    async def serve_react_app(full_path: str):
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path == "openapi.json":
            raise HTTPException(status_code=404, detail="Not found")
        file_path = static_dir / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        index_path = static_dir / "index.html"
        if index_path.is_file():
            return FileResponse(str(index_path))
        return {"message": "DW Migration Assistant API", "version": "2.0.0"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
