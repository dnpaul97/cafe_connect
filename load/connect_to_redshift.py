import logging
import os
import sys

import boto3
import psycopg2
from dotenv import load_dotenv

def connect_to_redshift():
    '''Returns connection to a RedShift database using credentials in environment.'''
    load_dotenv()
    print ("Getting credentials...")
    # Get connection credentials
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    user = os.getenv("DB_USER")
    passwd = os.getenv("DB_PASS")
    db = os.getenv("DB_NAME")
    cluster = os.getenv("DB_CLUSTER")

    try:
        client = boto3.client('redshift')
        creds = client.get_cluster_credentials(  # Lambda needs these permissions as well DataAPI permissions
            DbUser=user,
            DbName=db,
            ClusterIdentifier=cluster,
            DurationSeconds=3600) # Length of time access is granted
    except Exception as ERROR:
        logging.error({
            'Credentials Issue': str(ERROR)
        })
        sys.exit(1)

    logging.info('Got credentials')

    # Connect to RedShift
    try:
        conn = psycopg2.connect(
            dbname=db,
            user=creds["DbUser"],
            password=creds["DbPassword"],
            port=port,
            host=host)
    except Exception as ERROR:
        logging.error({
            'Connection Issue': str(ERROR)
        })
        sys.exit(1)
    logging.info('Connected to RedShift')
    return conn
