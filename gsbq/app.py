"""ETL script to transfer data from Google Sheets to BigQuery with auto-creation and append functionality."""

from pathlib import Path
from typing import Any, List, Tuple, Union

from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
import polars as pl


def authenticate(credentials_path: Union[Path, str]) -> Tuple[Any, bigquery.Client]:
    """Authenticates with Google Sheets API and BigQuery."""
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )
    sheets_service = build("sheets", "v4", credentials=credentials)
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return sheets_service, bq_client


def get_sheet_data(
    service: Any, spreadsheet_id: str, sheet_name: str
) -> List[List[Any]]:
    """Retrieves all data from a Google Sheet."""
    sheet = service.spreadsheets()
    data = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    return data.get("values", [])


def create_dataset_if_not_exists(client: bigquery.Client, dataset_id: str) -> None:
    """Creates a BigQuery dataset if it doesn't exist."""
    dataset = bigquery.Dataset(dataset_id)

    try:
        client.get_dataset(dataset_id)
    except exceptions.NotFound:
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")


def create_table_if_not_exists(
    client: bigquery.Client, table_id: str, df: pl.DataFrame
) -> None:
    """Creates a BigQuery table if it doesn't exist, using the Polars DataFrame schema."""
    schema = []

    for col_name, dtype in zip(df.columns, df.dtypes, strict=True):
        if dtype == pl.Int64:
            field_type = "INTEGER"
        elif dtype == pl.Float64:
            field_type = "FLOAT"
        elif dtype == pl.Boolean:
            field_type = "BOOLEAN"
        elif dtype == pl.Datetime:
            field_type = "DATETIME"
        elif dtype == pl.Date:
            field_type = "DATE"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(col_name, field_type))

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
    except exceptions.NotFound:
        client.create_table(table)
        print(f"Table {table_id} created.")


def convert_data_to_dataframe(data: List[List[Any]]) -> pl.DataFrame:
    """Processes the data using Polars."""
    df = pl.DataFrame(data[1:], schema=data[0], orient="row")
    return df


def upload_to_bigquery(
    client: bigquery.Client, dataframe: pl.DataFrame, table_id: str
) -> None:
    """Uploads data to BigQuery, appending to the existing table."""
    job_config = bigquery.LoadJobConfig(
        autodetect=True, write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(
        dataframe.to_pandas(), table_id, job_config=job_config
    )
    job.result()


if __name__ == "__main__":
    sheet_name = "Sheet1"
    service_account_file = Path("google_service_account_key.json")
    spreadsheet_id = "1Ba5nr8nzBh2MSVzIOpvtmHtzWqjDT4aP5G2YBkZN1mk"
    table_id = "gsbq-demo.gsbq_dataset.sample_table"

    sheets_service, bq_client = authenticate(service_account_file)

    raw_data = get_sheet_data(sheets_service, spreadsheet_id, sheet_name)
    processed_data = convert_data_to_dataframe(raw_data)

    dataset_id = ".".join(table_id.split(".")[:2])

    create_dataset_if_not_exists(bq_client, dataset_id)
    create_table_if_not_exists(bq_client, table_id, processed_data)

    upload_to_bigquery(bq_client, processed_data, table_id)

    print("Data successfully transferred from Google Sheets to BigQuery.")
