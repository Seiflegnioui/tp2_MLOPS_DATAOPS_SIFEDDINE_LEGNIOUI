from dagster import job, op
import os

@op
def ingest():
    os.system("python pipeline/ingest.py")
    return True

@op
def validate(_input):
    os.system("python pipeline/validate.py")
    return True

@op
def transform(_input):
    os.system("cd dbt_pipeline && dbt run --profiles-dir .")
    return True

@op
def test_data(_input):
    os.system("cd dbt_pipeline && dbt test --profiles-dir .")

@job
def ventes_pipeline():
    test_data(transform(validate(ingest())))
