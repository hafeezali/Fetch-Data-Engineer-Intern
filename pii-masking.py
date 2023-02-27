#!/usr/bin/env python
# coding: utf-8

# In[291]:


# PII-MASKING
# 1. Establish connection with SQS to read data from the SQS queue
# 2. Establish connection with PostgreSQL to write data to db
# 3. Use python's fernet library to encode and decode user data based on a private key
# 4. Post encrypted data to the database
# NOTE: Data items with same values will have the same encrypted value. Masked items can be recovered using decryption


# In[292]:


# python sdk for aws - boto3
import boto3
import json
import copy
# python postgres adapter
import psycopg2
import base64
import pandas as pd

from datetime import datetime
# python encryption library
from cryptography.fernet import Fernet


# In[293]:


# Declare SQS queue configs
REGION_NAME='eu-west-2'
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
SQS_SERVER_URL='http://localhost:4566/'
LOGIN_QUEUE_URL='http://localhost:4566/000000000000/login-queue'

# Declare SQS consumer configs
MAX_NUMBER_OF_MESSAGES=100
VISIBILITY_TIMEOUT=30
WAIT_TIME_SECONDS=20


# In[294]:


# Declare PostgreSQL configs
DB_HOST = 'localhost'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_PORT = '5431'


# In[295]:


# Initialize key and cipher for encryption and decryption
private_key = Fernet.generate_key()
cipher = Fernet(private_key)


# In[296]:


# Declare stucture of SQS message
SQS_SCHEMA = {
    "user_id": "", 
    "device_type": "", 
    "masked_ip": "", 
    "masked_device_id": "", 
    "locale": "", 
    "app_version": None, 
    "create_date" : datetime.today()
}


# In[297]:


# DB insert query
insert_query = """
    INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
# DB select query
select_query = """
    SELECT * FROM user_logins;
"""
# DB truncate query
truncate_query = """
    TRUNCATE TABLE user_logins;
"""


# In[298]:


def initialize():
    """
    This function initializes the postgres connection and SQS client
    """
    sqs = boto3.client("sqs", 
                       region_name=REGION_NAME, 
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                       endpoint_url=SQS_SERVER_URL)
    pg_conn = psycopg2.connect(host=DB_HOST,
                               dbname=DB_NAME,
                               user=DB_USER,
                               password=DB_PASSWORD,
                               port=DB_PORT)
    return sqs, pg_conn


# In[299]:


def read_messages(sqs):
    """
    This function reads data from the sqs queue and extracts the user login messages from it 
    """
    message_data = sqs.receive_message(QueueUrl=LOGIN_QUEUE_URL,
                                     MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
                                     VisibilityTimeout=VISIBILITY_TIMEOUT,
                                     WaitTimeSeconds=WAIT_TIME_SECONDS)
    return message_data.get("Messages", [])


# In[300]:


def encode64(bytes_):
    """
    This function encodes bytes to a base 64 string
    """
    return base64.b64encode(bytes_).decode('utf-8')


# In[301]:


def process_and_encrypt(message):
    """
    This function takes a queue message and converts it to a db message ready for getting persisted
    It encrypts user information like ip and device_id
    """
    body = json.loads(message["Body"])
    db_message = copy.deepcopy(SQS_SCHEMA)
    for key in body.keys():
        if key == "user_id":
            db_message["user_id"] = body[key]
        elif key == "device_type":
            db_message["device_type"] = body[key]
        elif key == "ip":
            db_message["masked_ip"] = encode64(cipher.encrypt(body[key].encode()))
        elif key == "device_id":
            db_message["masked_device_id"] = encode64(cipher.encrypt(body[key].encode()))
        elif key == "locale":
            db_message["locale"] = body[key]
        elif key == "app_version":
            db_message["app_version"] = int(body["app_version"].replace('.', ''))
    return db_message


# In[302]:


def write_db_message(conn, row):
    """
    This function writes row into postgres using conn
    """
    cur = conn.cursor()
    cur.execute(insert_query, (row["user_id"], row["device_type"], row["masked_ip"], row["masked_device_id"], row["locale"], row["app_version"], row["create_date"]))

    # committing it to the database
    conn.commit()
    cur.close()


# In[303]:


def query_db(pg_conn):
    """
    This function queries the db to extract all SQS messages
    """
    cur = pg_conn.cursor()

    # executing the query
    cur.execute(select_query)
    resp = cur.fetchall()
    cur.close()
    return resp


# In[304]:


def truncate(pg_conn):
    """
    This function deletes all rows from the table. Only to be used if it is required to drop data from user_logins
    """
    cur = pg_conn.cursor()
    
    cur.execute(truncate_query)
    pg_conn.commit()
    cur.close()


# In[309]:


def destructor(sqs, pg_conn):
    """
    This function closes the pg_conn and sqs client
    """
    del sqs
    pg_conn.close()


# In[306]:


def decrypt(string):
    """
    This function decrypts a string in base 64 back to original string
    """
    return cipher.decrypt(base64.b64decode(string)).decode()


# In[310]:


if __name__ == "__main__":
    sqs, pg_conn = initialize()
    messages = read_messages(sqs)
    for message in messages:
        db_message = process_and_encrypt(message)
        write_db_message(pg_conn, db_message)
    destructor(sqs, pg_conn)


# In[311]:


# Future enhancements:
# 1. SQS queue, server, and postgres configs can be moved to a dedicated config file
# 2. Private key must be generated only once and stored somewhere for later decryption. It should not be generated everytime
# 3. DB insert, select, and truncate queries must ideally sit in dedicated SQL files outside of core logic
# 4. An in-memory buffer queue can be used to serve as an interface between the SQS queue consumer and postgres db. The consumer and db writer should run on dedicated threads
#    The current implementation: reading everything, then processing, then writing is not ideal for queue based system event-driven systems 

