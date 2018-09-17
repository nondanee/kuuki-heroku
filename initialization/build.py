# -*- coding: utf-8 -*-
import pathlib

def creatTables(connect):

    cursor = connect.cursor()
    try:
        cursor.execute('''
            create table city(
                city_code integer not null,
                city_name_zh varchar(20) not null,
                city_name_en varchar(20) not null,
                province_code integer not null,
                primary key(city_code)
            )
        ''')
    except Exception as e:
        print(e)

    try:
        cursor.execute('''
            create table station(
                station_code varchar(5) not null,
                city_code integer not null,
                position_name varchar(15) not null,
                longitude numeric(10,7) not null,
                latitude numeric(10,7) not null,
                primary key(station_code),
                foreign key(city_code) references city(city_code)
            )
            ''')
    except Exception as e:
        print(e)

    try:
        cursor.execute('''
            create table raw(
                time_point timestamp not null,
                station_code varchar(5) not null,
                aqi integer,
                o3 integer,
                o3_24h integer,
                o3_8h integer,
                o3_8h_24h integer,
                co numeric(4,1),
                co_24h numeric(4,1),
                so2 integer,
                so2_24h integer,
                no2 integer,
                no2_24h integer,
                pm2_5 integer,
                pm2_5_24h integer,
                pm10 integer,
                pm10_24h integer,
                primary_pollutant smallint,
                primary key(time_point,station_code),
                foreign key(station_code) references station(station_code)
            )
            ''')
    except Exception as e:
        print(e)

    try:
        cursor.execute('''
            create table work (
                time_point timestamp not null,
                city_code integer not null,
                aqi integer,
                aqi_change integer,
                o3 integer,
                o3_24h integer,
                o3_8h integer,
                o3_8h_24h integer,
                co numeric(4,1),
                co_24h numeric(4,1),
                so2 integer,
                so2_24h integer,
                no2 integer,
                no2_24h integer,
                pm2_5 integer,
                pm2_5_24h integer,
                pm10 integer,
                pm10_24h integer,
                primary key(time_point,city_code),
                foreign key(city_code) references city(city_code)
            )
            ''')
    except Exception as e:
        print(e)
    
    connect.commit()
    cursor.close()


def fillCityTable(connect):
    f = open(str(pathlib.Path(__file__).parent.joinpath("cities.txt")),"r")
    data = f.read()
    f.close()

    sql = "insert into city values (%s,%s,%s,%s)"
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