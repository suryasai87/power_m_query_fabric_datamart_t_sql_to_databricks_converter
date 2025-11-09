#!/usr/bin/env python3
"""
Power M Query / Fabric Datamart / T-SQL to Databricks SQL Converter.

This script converts SQL queries from various sources to Databricks SQL format
and optionally tests them against Databricks SQL Serverless.

Supports both local filesystem paths and Unity Catalog volumes.

Usage:
    # Local paths:
    python convert_to_databricks.py --input-dir ./tests/sample_queries --output-dir ./output --test

    # Unity Catalog volumes:
    python convert_to_databricks.py --input-dir /Volumes/catalog/schema/volume/input --output-dir /Volumes/catalog/schema/volume/output --test

For help:
    python convert_to_databricks.py --help
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table
from rich.progress import track
from databricks.sdk import WorkspaceClient

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from converters.tsql.converter import TSQLConverter
from converters.power_m.converter import PowerMConverter
from converters.fabric.converter import FabricConverter
from utils.databricks_client import DatabricksClient
from config import DATABRICKS_PROFILE, WAREHOUSE_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

console = Console()


class MigrationTool:
    """Main migration tool orchestrator with support for local and Unity Catalog volume paths."""

    def __init__(self, input_dir: str, output_dir: str, catalog: str = None, schema: str = None):
        """
        Initialize migration tool.

        Args:
            input_dir: Directory containing source query files (local path or /Volumes/...)
            output_dir: Directory for output files (local path or /Volumes/...)
            catalog: Target Databricks catalog
            schema: Target schema
        """
        self.input_dir_str = input_dir
        self.output_dir_str = output_dir
        self.catalog = catalog or DEFAULT_CATALOG
        self.schema = schema or DEFAULT_SCHEMA

        # Determine if paths are volumes or local
        self.input_is_volume = self._is_volume_path(input_dir)
        self.output_is_volume = self._is_volume_path(output_dir)

        # Initialize Databricks client if volumes are used
        self.dbx_client = None
        if self.input_is_volume or self.output_is_volume:
            try:
                self.dbx_client = WorkspaceClient(profile=DATABRICKS_PROFILE)
                logger.info(f"Connected to Databricks for volume operations")
            except Exception as e:
                logger.error(f"Failed to connect to Databricks: {e}")
                raise

        # For local paths, use Path objects
        if not self.input_is_volume:
            self.input_dir = Path(input_dir)
        if not self.output_is_volume:
            self.output_dir = Path(output_dir)
            # Create output directory for local paths
            self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize converters
        self.tsql_converter = TSQLConverter(self.catalog, self.schema)
        self.power_m_converter = PowerMConverter(self.catalog, self.schema)
        self.fabric_converter = FabricConverter(self.catalog, self.schema)

        # Conversion results
        self.conversion_results = []
        self.test_results = []

    @staticmethod
    def _is_volume_path(path: str) -> bool:
        """Check if path is a Unity Catalog volume path."""
        return path.startswith('/Volumes/')

    def _list_files_in_volume(self, volume_path: str) -> List[str]:
        """
        List SQL and M files in a Unity Catalog volume.

        Args:
            volume_path: Volume path starting with /Volumes/

        Returns:
            List of file paths
        """
        files = []
        try:
            for file_info in self.dbx_client.files.list_directory_contents(volume_path):
                if file_info.is_directory:
                    # Recursively list subdirectories
                    files.extend(self._list_files_in_volume(file_info.path))
                elif file_info.path.endswith('.sql') or file_info.path.endswith('.m'):
                    files.append(file_info.path)
        except Exception as e:
            logger.error(f"Error listing volume files: {e}")
        return files

    def _read_file(self, file_path: Union[str, Path]) -> str:
        """
        Read file content from local or volume path.

        Args:
            file_path: Local path or volume path

        Returns:
            File content as string
        """
        if isinstance(file_path, str) and self._is_volume_path(file_path):
            # Read from volume
            with self.dbx_client.files.download(file_path) as f:
                return f.read().decode('utf-8')
        else:
            # Read from local filesystem
            return Path(file_path).read_text()

    def _write_file(self, file_path: str, content: str):
        """
        Write file content to local or volume path.

        Args:
            file_path: Local path or volume path
            content: Content to write
        """
        if self._is_volume_path(file_path):
            # Write to volume
            self.dbx_client.files.upload(file_path, content.encode('utf-8'), overwrite=True)
        else:
            # Write to local filesystem
            with open(file_path, 'w') as f:
                f.write(content)

    def convert_all_files(self) -> List[Dict]:
        """
        Convert all files in input directory (local or volume).

        Returns:
            List of conversion results
        """
        console.print("\n[bold blue]Starting conversion...[/bold blue]\n")

        # Find all supported files
        if self.input_is_volume:
            files = self._list_files_in_volume(self.input_dir_str)
            console.print(f"[cyan]Reading from Unity Catalog volume: {self.input_dir_str}[/cyan]")
        else:
            files = list(self.input_dir.glob('**/*.sql')) + list(self.input_dir.glob('**/*.m'))
            console.print(f"[cyan]Reading from local directory: {self.input_dir}[/cyan]")

        if self.output_is_volume:
            console.print(f"[cyan]Writing to Unity Catalog volume: {self.output_dir_str}[/cyan]\n")
        else:
            console.print(f"[cyan]Writing to local directory: {self.output_dir}[/cyan]\n")

        for file_path in track(files, description="Converting files..."):
            try:
                self._convert_file(file_path)
            except Exception as e:
                logger.error(f"Error converting {file_path}: {e}")
                self.conversion_results.append({
                    'file': str(file_path),
                    'status': 'error',
                    'error': str(e)
                })

        console.print(f"\n[bold green]Conversion complete![/bold green] Processed {len(files)} files\n")

        return self.conversion_results

    def _convert_file(self, file_path: Union[str, Path]):
        """Convert a single file (from local or volume path)."""
        logger.info(f"Converting file: {file_path}")

        # Get file name and extension
        if isinstance(file_path, str):
            file_name = os.path.basename(file_path)
            file_ext = os.path.splitext(file_path)[1]
            file_stem = os.path.splitext(file_name)[0]
        else:
            file_name = file_path.name
            file_ext = file_path.suffix
            file_stem = file_path.stem

        # Read file content using helper method
        content = self._read_file(file_path)

        # Determine file type and convert
        if file_ext == '.m':
            # Power M Query file
            converted_sql, conversion_log = self.power_m_converter.convert(content)
            converter_type = 'Power M Query'
        elif file_ext == '.sql':
            # Check if it's a DDL or query
            if 'CREATE TABLE' in content.upper():
                converted_sql, conversion_log = self.tsql_converter.convert_ddl(content)
            else:
                converted_sql, conversion_log = self.tsql_converter.convert_query(content)
            converter_type = 'T-SQL / Fabric'
        else:
            logger.warning(f"Unsupported file type: {file_ext}")
            return

        # Generate output file path
        output_file_name = f"{file_stem}_databricks.sql"
        if self.output_is_volume:
            output_file = f"{self.output_dir_str.rstrip('/')}/{output_file_name}"
        else:
            output_file = str(self.output_dir / output_file_name)

        # Build output content
        output_content = f"""-- Converted from: {file_name}
-- Converter: {converter_type}
-- Conversion Date: {datetime.now().isoformat()}
-- Original SQL:
/*
{content}
*/

-- Databricks SQL:
{converted_sql}
"""

        # Write converted SQL using helper method
        self._write_file(output_file, output_content)

        # Record results
        self.conversion_results.append({
            'file': file_name,
            'output': output_file,
            'converter': converter_type,
            'status': 'success',
            'conversion_notes': [note['message'] for note in conversion_log]
        })

    def generate_report(self):
        """Generate migration report in markdown format."""
        # Build report content
        report_content = "# SQL Migration Report\n\n"
        report_content += f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report_content += f"**Source Directory:** {self.input_dir_str}\n\n"
        report_content += f"**Target Catalog:** {self.catalog}\n\n"
        report_content += f"**Target Schema:** {self.schema}\n\n"

        # Summary
        report_content += "## Summary\n\n"
        total = len(self.conversion_results)
        successful = sum(1 for r in self.conversion_results if r.get('status') == 'success')
        report_content += f"- **Total Files:** {total}\n"
        report_content += f"- **Successful:** {successful}\n"
        report_content += f"- **Failed:** {total - successful}\n\n"

        # Conversion Details
        report_content += "## Conversion Details\n\n"
        for result in self.conversion_results:
            report_content += f"### {result['file']}\n\n"
            report_content += f"- **Status:** {result.get('status', 'unknown')}\n"
            report_content += f"- **Converter:** {result.get('converter', 'N/A')}\n"

            if result.get('conversion_notes'):
                report_content += f"\n**Conversion Notes:**\n\n"
                for note in result['conversion_notes']:
                    report_content += f"- {note}\n"

            if result.get('error'):
                report_content += f"\n**Error:** {result['error']}\n"

            report_content += "\n---\n\n"

        # Test Results (if available)
        if self.test_results:
            report_content += "## Test Results\n\n"
            for test in self.test_results:
                report_content += f"### {test['file']}\n\n"
                report_content += f"- **Status:** {test['status']}\n"
                if test.get('error'):
                    report_content += f"- **Error:** {test['error']}\n"
                report_content += "\n"

        # Write report file
        if self.output_is_volume:
            report_file = f"{self.output_dir_str.rstrip('/')}/migration_report.md"
        else:
            report_file = str(self.output_dir / 'migration_report.md')

        self._write_file(report_file, report_content)
        console.print(f"[bold green]Report generated:[/bold green] {report_file}")

    def test_conversions(self, dry_run: bool = False):
        """
        Test converted SQL against Databricks.

        Args:
            dry_run: If True, only validate syntax without executing
        """
        console.print("\n[bold blue]Testing conversions...[/bold blue]\n")

        try:
            client = DatabricksClient(profile=DATABRICKS_PROFILE, warehouse_id=WAREHOUSE_ID)

            # Test connection first
            if not client.test_connection():
                console.print("[bold red]Connection test failed! Skipping tests.[/bold red]")
                return

            # Test each converted file
            for result in track(self.conversion_results, description="Testing queries..."):
                if result.get('status') != 'success':
                    continue

                try:
                    output_file = result['output']
                    content = self._read_file(output_file)

                    # Extract Databricks SQL (skip comments)
                    lines = content.split('\n')
                    sql_lines = [l for l in lines if not l.strip().startswith('--') and l.strip()]
                    sql = '\n'.join(sql_lines)

                    if dry_run:
                        # Only validate syntax
                        validation = client.validate_sql(sql)
                        self.test_results.append({
                            'file': result['file'],
                            'status': 'validated' if validation['valid'] else 'invalid',
                            'message': validation.get('message', '')
                        })
                    else:
                        # Execute query (for SELECT statements only)
                        if sql.strip().upper().startswith('SELECT'):
                            response = client.execute_sql(sql)
                            self.test_results.append({
                                'file': result['file'],
                                'status': 'passed' if response.status.state == 'SUCCEEDED' else 'failed'
                            })

                except Exception as e:
                    logger.error(f"Test failed for {result['file']}: {e}")
                    self.test_results.append({
                        'file': result['file'],
                        'status': 'error',
                        'error': str(e)
                    })

            console.print(f"\n[bold green]Testing complete![/bold green]\n")

        except Exception as e:
            console.print(f"[bold red]Error initializing Databricks client: {e}[/bold red]")
            logger.error(f"Databricks client error: {e}")

    def print_summary(self):
        """Print summary table to console."""
        table = Table(title="Conversion Summary")

        table.add_column("File", style="cyan")
        table.add_column("Converter", style="magenta")
        table.add_column("Status", style="green")

        for result in self.conversion_results:
            status_style = "green" if result.get('status') == 'success' else "red"
            table.add_row(
                result['file'],
                result.get('converter', 'N/A'),
                f"[{status_style}]{result.get('status', 'unknown')}[/{status_style}]"
            )

        console.print("\n", table, "\n")


@click.command()
@click.option('--input-dir', required=True, help='Directory containing source query files')
@click.option('--output-dir', required=True, help='Directory for output files')
@click.option('--test', is_flag=True, help='Run test suite after conversion')
@click.option('--dry-run', is_flag=True, help='Validate syntax without executing')
@click.option('--catalog', default=None, help='Target Databricks catalog')
@click.option('--schema', default=None, help='Target schema')
@click.option('--connection-profile', default=None, help='Databricks connection profile')
def main(input_dir, output_dir, test, dry_run, catalog, schema, connection_profile):
    """
    Convert Power M Query, Fabric Datamart, and T-SQL to Databricks SQL.

    This tool converts SQL queries from various sources to Databricks SQL format
    and optionally tests them against Databricks SQL Serverless.
    """
    console.print("\n[bold blue]═══════════════════════════════════════════════════════[/bold blue]")
    console.print("[bold blue]  SQL to Databricks Converter[/bold blue]")
    console.print("[bold blue]═══════════════════════════════════════════════════════[/bold blue]\n")

    # Initialize tool
    tool = MigrationTool(input_dir, output_dir, catalog, schema)

    # Convert files
    tool.convert_all_files()

    # Print summary
    tool.print_summary()

    # Test if requested
    if test:
        tool.test_conversions(dry_run=dry_run)

    # Generate report
    tool.generate_report()

    console.print("\n[bold green]✓ Migration complete![/bold green]\n")


if __name__ == '__main__':
    main()
