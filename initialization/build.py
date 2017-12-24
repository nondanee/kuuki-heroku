# -*- coding: utf-8 -*-
import pathlib

def creatTables(connect):

    cursor = connect.cursor()
    try:
        cursor.execute('''
            create table city(
                city_code int(6) not null,
                city_name_zh varchar(20) not null,
                city_name_en varchar(20) not null,
                province_code int(2) not null,
                PRIMARY KEY(city_code)
            )
        ''')
        cursor.execute('''
            create table station(
                station_code varchar(5) not null,
                city_code int(6) not null,
                position_name varchar(15) not null,
                longitude Decimal(10,7) not null,
                latitude Decimal(10,7) not null,
                PRIMARY KEY(station_code),
                FOREIGN KEY(city_code) references city(city_code)
            )
            ''')
        cursor.execute('''
            create table raw(
                time_point datetime not null,
                station_code varchar(5) not null,
                aqi int(3),
                o3 int(4),
                o3_24h int(4),
                o3_8h int(4),
                o3_8h_24h int(4),
                co Decimal(4,1),
                co_24h Decimal(4,1),
                so2 int(4),
                so2_24h int(4),
                no2 int(4),
                no2_24h int(4),
                pm2_5 int(3),
                pm2_5_24h int(3),
                pm10 int(3),
                pm10_24h int(3),
                primary_pollutant tinyint(2),
                PRIMARY KEY(time_point,station_code),
                FOREIGN KEY(station_code) references station(station_code)
            )
            ''')
        cursor.execute('''
            create table work (
                time_point datetime not null,
                city_code int(6) not null,
                aqi int(3),
                aqi_change int(3),
                o3 int(4),
                o3_24h int(4),
                o3_8h int(4),
                o3_8h_24h int(4),
                co Decimal(4,1),
                co_24h Decimal(4,1),
                so2 int(4),
                so2_24h int(4),
                no2 int(4),
                no2_24h int(4),
                pm2_5 int(3),
                pm2_5_24h int(3),
                pm10 int(3),
                pm10_24h int(3),
                PRIMARY KEY(time_point,city_code),
                FOREIGN KEY(city_code) references city(city_code)
            )
            ''')
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)


def fillCityTable(connect):
    f = open(str(pathlib.Path(__file__).parent.joinpath("cities.txt")),"r")
    data = f.read()
    f.close()

    sql = "INSERT INTO city VALUES (%s, %s, %s, %s)"
    params = []

    cities = data.split("\n")
    for city in cities:
        content = city.split("\t")
        city_code = content[0]
        city_name_zh = content[1]
        city_name_en = content[2]
        province_code = city_code[0:2]
        params.append([city_code,city_name_zh,city_name_en,province_code])

    cursor = connect.cursor()
    try:
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)
    
def fillStationTable(connect):
    f = open(str(pathlib.Path(__file__).parent.joinpath("stations.txt")),"r")
    data = f.read()
    f.close()

    sql = "INSERT INTO station VALUES (%s, %s, %s, %s, %s)"
    params = []

    stations = data.split("\n")
    for station in stations:
        content = station.split("\t")
        station_code = content[0]
        city_code = content[1]
        position_name = content[2]
        longitude = content[3]
        latitude = content[4]
        params.append([station_code,city_code,position_name,longitude,latitude])
    
    cursor = connect.cursor()
    try:
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)