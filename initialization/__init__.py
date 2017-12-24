from . import build

def run(connect):
    build.creatTables(connect)
    build.fillCityTable(connect)
    build.fillStationTable(connect)
