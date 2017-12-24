import server
import crawler
import initialization
import sys, pymysql, os, urllib.parse

CLEARDB_DATABASE_URL = os.environ["CLEARDB_DATABASE_URL"]
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
