import logging
import argparse
import psycopg2
import psycopg2.extensions
import os
import stat
import pandas as pd
import datetime
# import openpyxl
# from google.cloud import storage
from io import StringIO
from ..utils.main import load_yml_env

load_yml_env('local/.env.yml')

# Function to wait for open connection when processing parallel
def wait_select(conn):

  while 1:
    state = conn.poll()
    if state == psycopg2.extensions.POLL_OK:
      break
    elif state == psycopg2.extensions.POLL_WRITE:
      pass
      select.select([], [conn.fileno()], [])
    elif state == psycopg2.extensions.POLL_READ:
      pass
      select.select([conn.fileno()], [], [])
    else:
      raise psycopg2.OperationalError("poll() returned %s" % state)

# Function which returns a connection which can be used for queries
def connect_to_db():

  sslrootcert = os.environ['DB_SSL_SERVER_CA']
  sslkey = os.environ['DB_SSL_CLIENT_KEY']
  sslcert = os.environ['DB_SSL_CLIENT_CERT']

  con = psycopg2.connect(
        host = os.environ['DB_HOST'],
        hostaddr = os.environ['DB_HOST'],
        dbname = os.environ['DB_DATABASE'],
        user = os.environ['DB_USER'],
        password = os.environ['DB_PASS']
        # sslmode = os.environ['DB_SSL_MODE'],
        # sslrootcert = sslrootcert,
        # sslcert = sslcert,
        # sslkey = sslkey
        )

  return con
