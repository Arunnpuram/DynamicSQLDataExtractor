import argparse
import json
import boto3
import base64
import time
import mysql.connector
import pandas as pd
import dask.dataframe as dd
import os
from mysql.connector import Error
from typing import Any, Dict, List


# retrieve MySQL credentials from AWS Secrets Manager
def get_secret(args: argparse.Namespace) -> Dict[str, Any]:
    session = boto3.session.Session(profile_name=args.aws_profile)
    client = session.client(service_name="secretsmanager")
    get_secret_value_response = client.get_secret_value(SecretId=args.secret_name)
    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
    else:
        secret = base64.b64decode(get_secret_value_response["SecretBinary"])
    return json.loads(secret)

# Command-line arguments 
parser = argparse.ArgumentParser(description="Execute SQL queries with input dates.")
parser.add_argument(
    "--aws-profile", type=str, required=True, help="Name of the AWS CLI profile"
)
parser.add_argument(
    "--secret-name",
    type=str,
    required=True,
    help="Name of the secret in AWS Secrets Manager.",
)
args = parser.parse_args()

# Retrieve MySQL credentials
credentials: Dict[str, Any] = get_secret(args)
username: str = credentials["username"]
password: str = credentials["password"]
host: str = credentials["host"]
dbname: str = credentials["dbname"]
port: int = credentials["port"]

# Connect to MySQL database
connection = mysql.connector.connect(
    user=username, password=password, host=host, database=dbname, port=port
)
cursor = connection.cursor()

# Read SQL queries from JSON file
with open("queries.json", "r") as file:
    sql_queries: List[Dict[str, Any]] = json.load(file)


# Function to ensure the reports directory exists
def ensure_reports_dir_exists():
    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)


while True:
    # List available queries and prompt user for selection
    print("Available queries:")
    for i, query_info in enumerate(sql_queries, start=1):
        print(f"{i}. {query_info['description']}")
    selected_query_number: int = (
        int(input("Select a query by entering its number: ")) - 1
    )

    if 0 <= selected_query_number < len(sql_queries):
        selected_query_info: Dict[str, Any] = sql_queries[selected_query_number]
    else:
        print("Invalid selection, Try again!")
        continue

    # Extract the description from the selected query info
    description: str = selected_query_info["description"]

    # Prompt user for start and end dates
    start_date: str = input("Enter the start date (YYYY-MM-DD): ")
    end_date: str = input("Enter the end date (YYYY-MM-DD): ")

    # Generate the CSV filename based on the description and the user-provided dates
    csv_filename: str = os.path.join(
        "reports", f"{description}_{start_date}_{end_date}.csv"
    )

    try:
        # Time elapsed for the Query
        start_time: float = time.time()

        # Execute the query with start_date and end_date as parameters
        cursor.execute(selected_query_info["query"], (start_date, end_date))

        # Fetch data in chunks and load into a Dask DataFrame
        chunks: List[pd.DataFrame] = []
        while True:
            chunk: pd.DataFrame = cursor.fetchmany(1000)
            if not chunk:
                break
            chunks.append(
                pd.DataFrame(chunk, columns=[desc[0] for desc in cursor.description])
            )

        # Check if chunks is empty before proceeding
        if not chunks:
            print("No data returned from the query.")
            continue  # Skip the rest of the loop iteration if no data was fetched

        df: dd.DataFrame = dd.from_pandas(
            pd.concat(chunks), npartitions=10
        )  # Adjust npartitions as needed

        # Compute the number of rows in the Dask DataFrame
        num_rows: int = df.compute().shape[0]
        print("\033[1;32m" + f"Fetched {num_rows} rows from the database." + "\033[0m")

        # Ensure the reports directory exists
        ensure_reports_dir_exists()

        # Save the Dask DataFrame to CSV
        print(
            "\033[1;32m"
            + f"Saving {num_rows} rows to CSV file: {csv_filename}"
            + "\033[0m"
        )
        df.to_csv(csv_filename, single_file=True, index=False)

        # Calculate the elapsed time
        elapsed_time: float = time.time() - start_time
        print(
            "\033[1;32m"
            + f"Finished saving Data to CSV file. Elapsed time: {elapsed_time:.2f} seconds."
            + "\033[0m"
        )

    except Error as e:
        print(f"Error executing query: {e}")

    finally:
        if connection.is_connected():
            user_choice: str = input(
                "Do you want to execute another report? (y/n): "
            ).lower()
            if user_choice == "n":
                cursor.close()
                connection.close()
                break