import server
import crawler
import initialization
import os, sys, psycopg2

try:
    from urllib import parse
except ImportError:
    import urlparse as parse

parse.uses_netloc.append('postgres')
url = parse.urlparse(os.environ['DATABASE_URL'])
db_config = 'dbname={} user={} password={} host={} port={}'.format(url.path[1:], url.username, url.password, url.hostname, url.port)
app = server.create(db_config)

def runserver():
    app.run()

def runcrawler():
    connect = psycopg2.connect(db_config)
    crawler.run(connect)
    connect.close()

def init():
    connect = psycopg2.connect(db_config)
    initialization.run(connect)
    connect.close()

if __name__ == '__main__':
    try:
        command = sys.argv[1]
        print(command)
        eval(command)()
    except Exception as e:
        print(e)
        exit()
