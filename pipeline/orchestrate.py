from dagster import job, op, Definitions
import subprocess
import os

# Get project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@op
def ingest():
    """Ingestion step: Load CSV data into DuckDB"""
    result = subprocess.run(["python", "pipeline/ingest.py"], cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")
    return result.stdout

@op
def validate(ingest_output):
    """Validation step: Check data quality and schema"""
    result = subprocess.run(["python", "pipeline/validate.py"], cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Validation failed: {result.stderr}")
    return result.stdout

@op
def transform(validate_output):
    """Transformation step: Run dbt transformations"""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=os.path.join(PROJECT_ROOT, "dbt_pipeline"),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Transformation failed: {result.stderr}")
    return result.stdout

@op
def test_data(transform_output):
    """Testing step: Run dbt tests for data quality"""
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=os.path.join(PROJECT_ROOT, "dbt_pipeline"),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Tests failed: {result.stderr}")
    return result.stdout

@job
def ventes_pipeline():
    """Complete ETL pipeline: Ingest → Validate → Transform → Test"""
    test_data(transform(validate(ingest())))

# Export definitions for Dagster to discover
defs = Definitions(jobs=[ventes_pipeline])
