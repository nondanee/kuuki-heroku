from flask import Flask
from flaskext.mysql import MySQL
import urllib.parse, os

CLEARDB_DATABASE_URL = os.environ["CLEARDB_DATABASE_URL"]
urllib.parse.uses_netloc.append("mysql")
url = urllib.parse.urlparse(CLEARDB_DATABASE_URL)

app = Flask(__name__)
app.config["MYSQL_DATABASE_HOST"] = url.hostname
app.config["MYSQL_DATABASE_USER"] = url.username
app.config["MYSQL_DATABASE_PASSWORD"] = url.password
app.config["MYSQL_DATABASE_DB"] = url.path[1:]

mysql = MySQL()
mysql.init_app(app)

from . import utils
from . import routes
