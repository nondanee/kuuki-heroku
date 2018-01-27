from flask import Flask, g
from . import routes
import psycopg2

def create(db_config):
    app = Flask(__name__)

    @app.before_request
    def before_request():
        g.db = psycopg2.connect(db_config)

    @app.teardown_request
    def teardown_request(exception):
        db = getattr(g, 'db', None)
        if db is not None:
            db.close()

    routes.attach(app)

    return app