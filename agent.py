import os
import logging
import openai
from phidata.bigquery.client import BigQueryClient
from google.cloud.exceptions import NotFound, BadRequest
import pandas as pd

# Configure logging
logging.basicConfig(
    filename="ai_bigquery_agent.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Set up OpenAI API
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize the BigQuery client
bq_client = BigQueryClient()

def validate_dataset_and_table(dataset, table):
    """
    Validates whether the given dataset and table exist in BigQuery.
    """
    try:
        dataset_ref = bq_client.client.get_dataset(dataset)
        table_ref = dataset_ref.table(table)
        bq_client.client.get_table(table_ref)
        return True
    except NotFound as e:
        logging.error(f"Dataset or table not found: {e}")
        return False

def query_bigquery(sql_query):
    """
    Executes a SQL query in BigQuery and returns the results.
    Adds validation for dataset and table existence.
    """
    try:
        # Extract dataset and table names (basic parsing, assumes `FROM dataset.table`)
        if "FROM" in sql_query.upper():
            parts = sql_query.split()
            from_index = parts.index("FROM") + 1
            dataset_table = parts[from_index]
            dataset, table = dataset_table.split(".")

            if not validate_dataset_and_table(dataset, table):
                return f"Error: Dataset or table '{dataset}.{table}' does not exist."

        # Execute query
        result = bq_client.query(sql_query)
        df = result.to_dataframe()
        
        if df.empty:
            return "Query executed successfully, but no results were found."
        
        return df
    except BadRequest as e:
        logging.error(f"BadRequest while querying BigQuery: {e}")
        return f"Error querying BigQuery: {e}"
    except Exception as e:
        logging.error(f"Unexpected error querying BigQuery: {e}")
        return f"Unexpected error: {e}"

def chat_with_agent(prompt):
    """
    Uses ChatGPT to process user prompts and generate SQL queries.
    """
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a data analyst skilled in writing BigQuery SQL queries."},
                {"role": "user", "content": prompt},
            ],
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.error(f"Error in ChatGPT interaction: {e}")
        return f"Error generating SQL query: {e}"

def log_query_and_results(user_input, sql_query, results):
    """
    Logs user input, generated SQL query, and results.
    """
    logging.info(f"User Input: {user_input}")
    logging.info(f"Generated SQL Query: {sql_query}")
    if isinstance(results, pd.DataFrame):
        logging.info(f"Query Results:\n{results.head(5)}")  # Log first 5 rows
    else:
        logging.info(f"Query Results: {results}")

def main():
    print("Welcome to the Enhanced AI BigQuery Agent!")
    while True:
        user_input = input("\nAsk your question or type 'exit' to quit: ")
        if user_input.lower() == "exit":
            print("Goodbye!")
            break

        # Generate SQL query using ChatGPT
        print("\nProcessing your request...")
        sql_query = chat_with_agent(f"Generate a BigQuery SQL query for: {user_input}")
        print(f"\nGenerated SQL Query:\n{sql_query}")

        # Query BigQuery and display results
        print("\nQuerying BigQuery...")
        query_results = query_bigquery(sql_query)

        if isinstance(query_results, str):  # If there's an error
            print(query_results)
        else:
            print("\nQuery Results:")
            print(query_results)

        # Log the interaction
        log_query_and_results(user_input, sql_query, query_results)

if __name__ == "__main__":
    main()
      
