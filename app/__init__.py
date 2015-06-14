from flask import Flask
import logging
from logging.handlers import RotatingFileHandler

app = Flask(__name__, static_url_path='')
#app.logger.addHandler()
app.debug = True
# file_handler = RotatingFileHandler('log.log', 'w', 1 * 1024 * 1024, 10)
# file_handler.setLevel(logging.DEBUG)
# app.logger.addHandler(file_handler)

from app import myMergerTreeRoutes