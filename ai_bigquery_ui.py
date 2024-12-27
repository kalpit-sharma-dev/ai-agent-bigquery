import os
import openai
import streamlit as st
from phidata.bigquery.client import BigQueryClient
from google.cloud.exceptions import NotFound, BadRequest
import pandas as pd

# Initialize BigQuery client
bq_client = BigQueryClient()

# Set OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Function to validate dataset and table
def validate_dataset_and_table(dataset, table):
    try:
        dataset_ref = bq_client.client.get_dataset(dataset)
        table_ref = dataset_ref.table(table)
        bq_client.client.get_table(table_ref)
        return True
    except NotFound:
        return False

# Function to query BigQuery
def query_bigquery(sql_query):
    try:
        if "FROM" in sql_query.upper():
            parts = sql_query.split()
            from_index = parts.index("FROM") + 1
            dataset_table = parts[from_index]
            dataset, table = dataset_table.split(".")

            if not validate_dataset_and_table(dataset, table):
                return f"Error: Dataset or table '{dataset}.{table}' does not exist."

        result = bq_client.query(sql_query)
        df = result.to_dataframe()
        
        if df.empty:
            return "Query executed successfully, but no results were found."
        
        return df
    except BadRequest as e:
        return f"Error querying BigQuery: {e}"
    except Exception as e:
        return f"Unexpected error: {e}"

# Function to generate SQL query using ChatGPT
def chat_with_agent(prompt):
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
        return f"Error generating SQL query: {e}"

# Streamlit app layout
st.title("AI BigQuery Agent")
st.markdown(
    """
    This tool allows you to interact with BigQuery using natural language.
    Ask questions about your data, and the AI will generate SQL queries and return results.
    """
)

# Input box for user query
user_input = st.text_area("Ask your question:", placeholder="e.g., Show the top 10 customers by revenue in 2023")

# Button to process the input
if st.button("Submit"):
    with st.spinner("Processing your request..."):
        # Generate SQL query
        sql_query = chat_with_agent(f"Generate a BigQuery SQL query for: {user_input}")
        st.subheader("Generated SQL Query")
        st.code(sql_query)

        # Query BigQuery
        query_results = query_bigquery(sql_query)

        if isinstance(query_results, str):  # If error or no results
            st.error(query_results)
        else:
            st.subheader("Query Results")
            st.dataframe(query_results)

# Footer
st.markdown("---")
st.markdown("Powered by [OpenAI](https://openai.com) and [Google BigQuery](https://cloud.google.com/bigquery)")

