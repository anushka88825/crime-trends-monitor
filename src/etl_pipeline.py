from prefect import flow
from src.extract import extract_data
from src.validate import validate_data
from src.transform import transform_data
from src.load import load_into_postgres


@flow(name="cpd_incident_etl", log_prints=True)
def etl():
    '''Extract -> validate -> transform -> load CPD incident data into Postgres.'''
    print("Extracting data...")
    extracted_df = extract_data()

    print("Performing data quality checks...")
    validated_df = validate_data(extracted_df)

    print("Performing data transformations...")
    transformed_df = transform_data(validated_df)

    print("Importing data into Postgres...")
    load_into_postgres(transformed_df)

    print("ETL complete!")


if __name__ == "__main__":
    # The dataset updates daily, so run at midnight.
    # Prefect cron uses UTC unless you set a timezone; this one matches Cambridge, MA.
    etl.serve(
        name="cpd-pipeline-deployment",
        cron="0 0 * * *",
        #timezone="America/New_York",
    )