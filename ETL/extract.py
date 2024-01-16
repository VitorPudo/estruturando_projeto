import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import configparser
import psycopg2
def get_data():
    url = r"https://official-joke-api.appspot.com/random_ten"
    response = requests.get(url)
    data = json.loads(response.text)

    dataframe = pd.json_normalize(data=data)
    return dataframe


def commit_to_postgres():

    # creating a Configparser object
    config = configparser.ConfigParser()
    # reading the configuration file
    config.read('postgres_db_credentials.txt')

    # reading credentials from file
    username = config.get('Credentials', 'postgres')
    host = config.get('Credentials', 'host')
    password = config.get('Credentials', 'admin')
    port = config.get('Credentials', 'port')
    db_name = config.get('Credentials', 'db_name')

    engine = create_engine(
        'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
            username,
            password,
            host,
            port,
            db_name
        ))

    # sql syntax to create the table that would hold our data
    create_table_query = """ 
    CREATE TABLE jokes_data(
                type text,
                setup text,
                punchline text,
                id integer primary key
                ) 
            """

    # a raw database connection that allows direct interaction with the database
    connection = engine.raw_connection()

    # the cursor allows us to execute queries and retrieve results from the database
    cursor = connection.cursor()

    # creating the table using the cursor
    cursor.execute(create_table_query)

    # storing the result of the function into a variable
    dataframe = get_data()

    # pushing the data into the database
    for _, row in dataframe.iterrows():
        cursor.execute(
            "INSERT INTO jokes_data (id, type, setup, punchline) VALUES (%s, %s, %s, %s)",
            (
            row["id"],
            row["type"],
            row["setup"],
            row["punchline"]),
        )

    # committing the current transaction to the database
    connection.commit()

    # closing the cursor
    cursor.close()
    # closing the connection
    connection.close()


# calling our functions
get_data()
commit_to_postgres()

