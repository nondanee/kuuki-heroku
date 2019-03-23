from flask import Blueprint
main = Blueprint('main',__name__)
from . import last, latest, rank, cities

def attach(app):
    app.register_blueprint(main, url_prefix = '/aqi')