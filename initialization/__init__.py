from . import build

def run(connect):
    build.creat_tables(connect)
    build.fill_city_table(connect)
