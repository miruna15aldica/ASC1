from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import os
import logging
from logging.handlers import RotatingFileHandler

if not os.path.exists("results"):
    os.mkdir("results") # Directorul results pentru afisarea outputurilor

webserver = Flask(__name__)
webserver.task_runner = ThreadPool()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

from app import routes
