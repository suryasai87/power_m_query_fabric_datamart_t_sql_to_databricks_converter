#!/usr/bin/env python3
"""Build script: compile frontend and prepare for deployment."""
import subprocess
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parent
FRONTEND_DIR = ROOT / "frontend"
STATIC_DIR = ROOT / "static"


def build_frontend():
    """Build React frontend and copy to static/."""
    if not FRONTEND_DIR.exists():
        print("No frontend directory found, skipping frontend build.")
        return

    print("Installing frontend dependencies...")
    subprocess.run(["npm", "install"], cwd=FRONTEND_DIR, check=True)

    print("Building frontend...")
    subprocess.run(["npm", "run", "build"], cwd=FRONTEND_DIR, check=True)

    # Vite outputs to ../static/ via vite.config.ts
    if STATIC_DIR.exists():
        print(f"Frontend built successfully -> {STATIC_DIR}")
    else:
        print("WARNING: Build completed but static/ directory not found!")
        sys.exit(1)


def verify_backend():
    """Quick sanity check that backend imports work."""
    print("Verifying backend imports...")
    try:
        subprocess.run(
            [sys.executable, "-c", "from app.main import app; print(f'FastAPI app: {app.title} v{app.version}')"],
            cwd=ROOT,
            check=True,
        )
        print("Backend verified OK.")
    except subprocess.CalledProcessError:
        print("WARNING: Backend import check failed — check dependencies.")


if __name__ == "__main__":
    build_frontend()
    verify_backend()
    print("\nBuild complete. Deploy with: databricks apps deploy dw-migration-assistant --profile DEFAULT")
