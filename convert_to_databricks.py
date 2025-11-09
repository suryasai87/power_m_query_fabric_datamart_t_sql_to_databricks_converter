#!/usr/bin/env python3
"""
Power M Query / Fabric Datamart / T-SQL to Databricks SQL Converter.

This script converts SQL queries from various sources to Databricks SQL format
and optionally tests them against Databricks SQL Serverless.

Usage:
    python convert_to_databricks.py --input-dir ./tests/sample_queries --output-dir ./output --test

For help:
    python convert_to_databricks.py --help
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table
from rich.progress import track

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
    """Main migration tool orchestrator."""

    def __init__(self, input_dir: str, output_dir: str, catalog: str = None, schema: str = None):
        """
        Initialize migration tool.

        Args:
            input_dir: Directory containing source query files
            output_dir: Directory for output files
            catalog: Target Databricks catalog
            schema: Target schema
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.catalog = catalog or DEFAULT_CATALOG
        self.schema = schema or DEFAULT_SCHEMA

        # Initialize converters
        self.tsql_converter = TSQLConverter(self.catalog, self.schema)
        self.power_m_converter = PowerMConverter(self.catalog, self.schema)
        self.fabric_converter = FabricConverter(self.catalog, self.schema)

        # Conversion results
        self.conversion_results = []
        self.test_results = []

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def convert_all_files(self) -> List[Dict]:
        """
        Convert all files in input directory.

        Returns:
            List of conversion results
        """
        console.print("\n[bold blue]Starting conversion...[/bold blue]\n")

        # Find all supported files
        files = list(self.input_dir.glob('**/*.sql')) + list(self.input_dir.glob('**/*.m'))

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

    def _convert_file(self, file_path: Path):
        """Convert a single file."""
        logger.info(f"Converting file: {file_path}")

        # Read file content
        content = file_path.read_text()

        # Determine file type and convert
        if file_path.suffix == '.m':
            # Power M Query file
            converted_sql, conversion_log = self.power_m_converter.convert(content)
            converter_type = 'Power M Query'
        elif file_path.suffix == '.sql':
            # Check if it's a DDL or query
            if 'CREATE TABLE' in content.upper():
                converted_sql, conversion_log = self.tsql_converter.convert_ddl(content)
            else:
                converted_sql, conversion_log = self.tsql_converter.convert_query(content)
            converter_type = 'T-SQL / Fabric'
        else:
            logger.warning(f"Unsupported file type: {file_path.suffix}")
            return

        # Generate output file name
        output_file = self.output_dir / f"{file_path.stem}_databricks.sql"

        # Write converted SQL
        with open(output_file, 'w') as f:
            f.write(f"-- Converted from: {file_path.name}\n")
            f.write(f"-- Converter: {converter_type}\n")
            f.write(f"-- Conversion Date: {datetime.now().isoformat()}\n")
            f.write(f"-- Original SQL:\n/*\n{content}\n*/\n\n")
            f.write(f"-- Databricks SQL:\n{converted_sql}\n")

        # Record results
        self.conversion_results.append({
            'file': file_path.name,
            'output': str(output_file),
            'converter': converter_type,
            'status': 'success',
            'conversion_notes': [note['message'] for note in conversion_log]
        })

    def generate_report(self):
        """Generate migration report in markdown format."""
        report_file = self.output_dir / 'migration_report.md'

        with open(report_file, 'w') as f:
            f.write("# SQL Migration Report\n\n")
            f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Source Directory:** {self.input_dir}\n\n")
            f.write(f"**Target Catalog:** {self.catalog}\n\n")
            f.write(f"**Target Schema:** {self.schema}\n\n")

            # Summary
            f.write("## Summary\n\n")
            total = len(self.conversion_results)
            successful = sum(1 for r in self.conversion_results if r.get('status') == 'success')
            f.write(f"- **Total Files:** {total}\n")
            f.write(f"- **Successful:** {successful}\n")
            f.write(f"- **Failed:** {total - successful}\n\n")

            # Conversion Details
            f.write("## Conversion Details\n\n")
            for result in self.conversion_results:
                f.write(f"### {result['file']}\n\n")
                f.write(f"- **Status:** {result.get('status', 'unknown')}\n")
                f.write(f"- **Converter:** {result.get('converter', 'N/A')}\n")

                if result.get('conversion_notes'):
                    f.write(f"\n**Conversion Notes:**\n\n")
                    for note in result['conversion_notes']:
                        f.write(f"- {note}\n")

                if result.get('error'):
                    f.write(f"\n**Error:** {result['error']}\n")

                f.write("\n---\n\n")

            # Test Results (if available)
            if self.test_results:
                f.write("## Test Results\n\n")
                for test in self.test_results:
                    f.write(f"### {test['file']}\n\n")
                    f.write(f"- **Status:** {test['status']}\n")
                    if test.get('error'):
                        f.write(f"- **Error:** {test['error']}\n")
                    f.write("\n")

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
                    output_file = Path(result['output'])
                    content = output_file.read_text()

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
