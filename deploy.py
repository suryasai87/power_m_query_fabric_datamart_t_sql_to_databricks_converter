#!/usr/bin/env python3
"""Deploy DW Migration Assistant to Databricks Apps.

Usage:
    python deploy.py [--profile DEFAULT] [--app-name dw-migration-assistant]
"""
import subprocess
import shutil
import sys
import argparse
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent
STATIC_DIR = ROOT / "static"
FRONTEND_DIR = ROOT / "frontend"

# Files/dirs to include in deployment
INCLUDE = [
    "app/",
    "converters/",
    "utils/",
    "static/",
    "app.yaml",
    "requirements.txt",
]

# Files/dirs to exclude
EXCLUDE = [
    "__pycache__",
    ".git",
    ".gitignore",
    "frontend/",
    "tests/",
    "*.pyc",
    ".env",
    "node_modules/",
    "build.py",
    "deploy.py",
    "convert_to_databricks.py",
    "config.py",
    "output/",
]


def build_frontend():
    """Build frontend if source exists."""
    if not FRONTEND_DIR.exists():
        print("No frontend/ directory, skipping build.")
        return
    if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
        print("static/ already has index.html, skipping build (use --rebuild to force).")
        return
    print("Building frontend...")
    subprocess.run(["npm", "install"], cwd=FRONTEND_DIR, check=True)
    subprocess.run(["npm", "run", "build"], cwd=FRONTEND_DIR, check=True)


def create_staging_dir() -> Path:
    """Create a clean staging directory with only deployment files."""
    staging = Path(tempfile.mkdtemp(prefix="dw-migration-deploy-"))
    print(f"Staging directory: {staging}")

    for item in INCLUDE:
        src = ROOT / item
        dst = staging / item
        if src.is_dir():
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns(*EXCLUDE))
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        else:
            print(f"WARNING: {item} not found, skipping")

    return staging


def deploy(profile: str, app_name: str, workspace_path: str):
    """Deploy to Databricks workspace and start the app."""
    staging = create_staging_dir()

    try:
        # Clean workspace target
        target = workspace_path or f"/Workspace/Users/suryasai.turaga@databricks.com/{app_name}"
        print(f"Uploading to {target}...")

        subprocess.run(
            ["databricks", "workspace", "import-dir", str(staging), target,
             "--overwrite", "--profile", profile],
            check=True,
        )
        print(f"Files uploaded to {target}")

        # Deploy the app
        print(f"Deploying app '{app_name}'...")
        result = subprocess.run(
            ["databricks", "apps", "deploy", app_name,
             "--source-code-path", target, "--profile", profile],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"Deployment started successfully!")
            print(result.stdout)
        else:
            print(f"Deployment command output: {result.stdout}")
            print(f"Errors: {result.stderr}")

    finally:
        shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy DW Migration Assistant")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    parser.add_argument("--app-name", default="dw-migration-assistant", help="App name")
    parser.add_argument("--workspace-path", default="", help="Override workspace path")
    parser.add_argument("--rebuild", action="store_true", help="Force frontend rebuild")
    args = parser.parse_args()

    if args.rebuild and STATIC_DIR.exists():
        shutil.rmtree(STATIC_DIR)

    build_frontend()
    deploy(args.profile, args.app_name, args.workspace_path)
