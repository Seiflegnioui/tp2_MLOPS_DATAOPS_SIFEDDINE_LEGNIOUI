from dagster import job, op
import os

@op
def ingest():
    os.system("python pipeline/ingest.py")

@op
def validate():
    os.system("python pipeline/validate.py")

@op
def transform():
    os.system("cd dbt_pipeline && dbt run --profiles-dir .")

@op
def test_data():
    os.system("cd dbt_pipeline && dbt test --profiles-dir .")

@job
def ventes_pipeline():
    test_data(transform(validate(ingest())))
