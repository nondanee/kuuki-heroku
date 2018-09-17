# -*- coding: utf-8 -*-
import re, base64, zlib, io
import requests
import xml.dom.minidom, datetime
from . import wcf

def get_all_stations_data():

    output = io.StringIO()
    output.write('<GetAllAQIPublishLive xmlns="http://tempuri.org/"></GetAllAQIPublishLive>')
    output.seek(0)

    data = wcf.xml2records.XMLParser.parse(output)
    data = wcf.records.dump_records(data)

    raw = requests.request(
        method = 'POST',
        url = 'http://106.37.208.233:20035/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/GetAllAQIPublishLive',
        data = data,
        headers = {'Content-Type': 'application/msbin1'}
    ).content

    raw = io.BytesIO(raw)
    raw = wcf.records.Record.parse(raw)

    wcf.records.print_records(raw, fp=output)
    output.seek(0)

    pattern = re.compile('<[^>]+>')
    records = pattern.sub('', output.readlines()[1][1:])[:-1]

    records = base64.b64decode(records)
    records = zlib.decompress(records)

    dom_tree = xml.dom.minidom.parseString(records)
    collection = dom_tree.documentElement
    return collection.getElementsByTagName('AQIDataPublishLive')    

def check_value(value,reserve=0):
    if check_empty(value) == '':
        return None
    elif reserve != 0:
        return round(float(value)+0.00000002,reserve)
    elif reserve == 0:
        return int(float(value))
        
def check_empty(value):
    if value == '\u2014' or value is None:
        return ''
    else:
        return value
        
def get_primary_pollutant(string):

    if string == '\u2014':
        return 0
        
    primary_pollutant = 0b000000
    
    if string.find(u'臭氧') != -1 :
        primary_pollutant += 0b100000
    if string.find(u'一氧化碳') != -1 :
        primary_pollutant += 0b010000
    if string.find(u'二氧化硫') != -1 :
        primary_pollutant += 0b001000
    if string.find(u'二氧化氮') != -1 :
        primary_pollutant += 0b000100
    if string.find(u'PM2.5') != -1 :
        primary_pollutant += 0b000010
    if string.find(u'PM10') != -1 :
        primary_pollutant += 0b000001
    
    return primary_pollutant

def calculate_iaqi(value,value_type):
    
    if value is None: return 0
    
    boundary = [0,50,100,150,200,300,400,500]

    if value_type == 'SO2':
        measure = [0,150,500,650,800]
    elif value_type == 'SO2_24h':
        measure = [0,50,150,475,800,1600,2100,2620]
    elif value_type == 'NO2':
        measure = [0,100,200,700,1200,2340,3090,3840]
    elif value_type == 'NO2_24h':
        measure = [0,40,80,180,280,565,750,940]
    elif value_type == 'CO':
        measure = [0,5,10,35,60,90,120,150]
    elif value_type == 'CO_24h':
        measure = [0,2,4,14,24,36,48,60]
    elif value_type == 'O3':
        measure = [0,160,200,300,400,800,1000,1200]
    elif value_type == 'O3_8h':
        measure = [0,100,160,215,265,800]
    elif value_type == 'PM2_5_24h':
        measure = [0,35,75,115,150,250,350,500]
    elif value_type == 'PM10_24h':
        measure = [0,50,150,250,350,420,500,600]

    for i in range(0,len(measure)-1):
        if measure[i] <= value and value < measure[i+1]:
            iaqi =  float(boundary[i+1] - boundary[i]) / float(measure[i+1] - measure[i]) * (value - measure[i]) + boundary[i]
            return int(round(iaqi))

    return 0

def get_tag_data(dom,tag_name):

    return dom.getElementsByTagName(tag_name)[0].childNodes[0].data

def update_stations_info(connect,all_stations_data):

    cursor = connect.cursor()
    cursor.execute('select station_code from station')
    data = cursor.fetchall()
    cities = {item[0]:'' for item in data}
    params = []
    
    for station_data in all_stations_data:
        station_code = get_tag_data(station_data,'StationCode')
        city_code = get_tag_data(station_data,'CityCode')
        position_name = get_tag_data(station_data,'PositionName')
        longitude = get_tag_data(station_data,'Longitude')
        latitude = get_tag_data(station_data,'Latitude')

        if station_code not in cities:
            params.append([station_code,city_code,position_name,longitude,latitude])

    try:
        cursor.executemany('insert into station values (%s,%s,%s,%s,%s)',params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print('update_stations_info',e)

def pull_raw_data(connect,all_stations_data=None):
    
    all_stations_data = get_all_stations_data() if not all_stations_data else all_stations_data
    params = []
    
    for station_data in all_stations_data:

        time_point = datetime.datetime.strptime(get_tag_data(station_data,'TimePoint'),'%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M')

        station_code = get_tag_data(station_data,'StationCode')
        
        aqi = check_value(get_tag_data(station_data,'AQI'))

        o3 = check_value(get_tag_data(station_data,'O3'))
        o3_24h = check_value(get_tag_data(station_data,'O3_24h'))
        o3_8h = check_value(get_tag_data(station_data,'O3_8h'))
        o3_8h_24h = check_value(get_tag_data(station_data,'O3_8h_24h'))
        
        co = check_value(get_tag_data(station_data,'CO'),1)
        co_24h = check_value(get_tag_data(station_data,'CO_24h'),1)

        so2 = check_value(get_tag_data(station_data,'SO2'))
        so2_24h = check_value(get_tag_data(station_data,'SO2_24h'))
        
        no2 = check_value(get_tag_data(station_data,'NO2'))
        no2_24h = check_value(get_tag_data(station_data,'NO2_24h'))
        
        pm2_5 = check_value(get_tag_data(station_data,'PM2_5'))
        pm2_5_24h = check_value(get_tag_data(station_data,'PM2_5_24h'))
        
        pm10 = check_value(get_tag_data(station_data,'PM10'))
        pm10_24h = check_value(get_tag_data(station_data,'PM10_24h'))
        
        primary_pollutant = get_primary_pollutant(get_tag_data(station_data,'PrimaryPollutant'))
        
        params.append([time_point,station_code,aqi,o3,o3_24h,o3_8h,o3_8h_24h,co,co_24h,so2,so2_24h,no2,no2_24h,pm2_5,pm2_5_24h,pm10,pm10_24h,primary_pollutant])

    cursor = connect.cursor()
    
    try:
        cursor.executemany('insert into raw values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print('pull_raw_data',e)
        if e.pgcode == '23503': 
            update_stations_info(connect,all_stations_data)
            pull_raw_data(connect,all_stations_data)
        else:
            print(e.pgcode,type(e.pgcode))
    else:
        process_data(connect)


def process_data(connect):

    cursor = connect.cursor()
    sql_last_hour = '''
        select
        work.city_code,
        work.aqi
        from work
        where work.time_point = (select max(time_point) - interval '1 hour' from raw)
        order by work.city_code
    '''
    sql_this_hour = '''
        select
        max(raw.time_point),
        station.city_code,
        avg(raw.aqi),
        avg(raw.o3),
        avg(raw.o3_24h),
        avg(raw.o3_8h),
        avg(raw.o3_8h_24h),
        avg(raw.co),
        avg(raw.co_24h),
        avg(raw.so2),
        avg(raw.so2_24h),
        avg(raw.no2),
        avg(raw.no2_24h),
        avg(raw.pm2_5),
        avg(raw.pm2_5_24h),
        avg(raw.pm10),
        avg(raw.pm10_24h)
        from raw,station 
        where raw.time_point = (select max(time_point) from raw)
        and raw.station_code = station.station_code 
        group by station.city_code
    '''
    
    try:        
        cursor.execute(sql_last_hour)
        last_hour_data = cursor.fetchall()
        cursor.execute(sql_this_hour)
        this_hour_data = cursor.fetchall()
    except Exception as e:
        print(e)

    last_hour_data = {item[0]:item[1] for item in last_hour_data}
    params = []

    for city_data in this_hour_data:

        param = list(city_data)
        for index in range(2,17):
            if index == 7 or index == 8: param[index] = check_value(param[index],1)
            else: param[index] = check_value(param[index])

        if not param[2]:
            aqi_change = None
        elif param[1] not in last_hour_data:
            aqi_change = None
        elif not last_hour_data[param[1]]:
            aqi_change = None
        else:
            aqi_change = param[2] - last_hour_data[param[1]]

        param.insert(3,aqi_change)
        
        params.append(param)
    
    try:        
        cursor.executemany('insert into work values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print('process_data',e)


def compact_table(connect):

    cursor = connect.cursor()
    sql_raw = '''
        delete from raw 
        where raw.time_point < (select max(time_point) from raw)
    '''
    sql_work = '''
        delete from work 
        where work.time_point < (select max(time_point) - interval '11 hour' from raw)
    '''

    try:
        cursor.execute(sql_raw)
        cursor.execute(sql_work)
        connect.commit()
        cursor.close()
    except Exception as e:
        print('compact_table',e)
    
