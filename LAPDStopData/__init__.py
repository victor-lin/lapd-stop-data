import logging

from flask import Flask
from . import config

app = Flask(__name__)
app.config.from_object(config)

app.logger.propagate = 0
app.logger.setLevel(logging.DEBUG)

app.url_map.strict_slashes = False

from . import views
