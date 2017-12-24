import server
import crawler
import initialization
import sys, pymysql, os, urllib.parse

CLEARDB_DATABASE_URL = 'mysql://b3cca747395bff:8264fa51@us-cdbr-iron-east-05.cleardb.net/heroku_33676382519b921?reconnect=true'

urllib.parse.uses_netloc.append("mysql")
url = urllib.parse.urlparse(CLEARDB_DATABASE_URL)

def runserver():
	server.app.run()

def runcrawler():
    connect = pymysql.connect(
        host = url.hostname,
        user = url.username,
        passwd = url.password,
        db = url.path[1:],
        charset = "utf8"
    )
    crawler.run(connect)
    connect.close()

def init():
    connect = pymysql.connect(
        host = url.hostname,
        user = url.username,
        passwd = url.password,
        db = url.path[1:],
        charset = "utf8"
    )
    initialization.run(connect)
    connect.close()


if __name__ == "__main__":
    try:
        command = sys.argv[1]
        print(command)
        eval(command)()
    except Exception as e:
        print(e)
        exit()
