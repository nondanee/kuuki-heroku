import server
import crawler
import initialization
import os, sys, psycopg2
from urllib import parse

parse.uses_netloc.append("postgres")
url = parse.urlparse(os.environ["DATABASE_URL"])
db_config = "dbname={} user={} password={} host={} port={}".format(url.path[1:],url.username,url.password,url.hostname,url.port)

def runserver():
	app = server.create(db_config)
	app.run()

def runcrawler():
    connect = psycopg2.connect(db_config)
    crawler.run(connect)
    connect.close()

def init():
    connect = psycopg2.connect(db_config)
    initialization.run(connect)
    connect.close()

def deploy():
	return server.create(db_config)

if __name__ == "__main__":
    try:
        command = sys.argv[1]
        print(command)
        eval(command)()
    except Exception as e:
        print(e)
        exit()
