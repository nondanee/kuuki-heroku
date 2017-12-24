from flask import Flask
from flaskext.mysql import MySQL
import urllib.parse, os

CLEARDB_DATABASE_URL = 'mysql://b3cca747395bff:8264fa51@us-cdbr-iron-east-05.cleardb.net/heroku_33676382519b921?reconnect=true'

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
