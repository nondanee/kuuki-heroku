# -*- coding: utf-8 -*-
import re, base64, zlib, io
import requests
import xml.dom.minidom, datetime
from . import wcf

def getAllStationsData():

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

    domTree = xml.dom.minidom.parseString(records)
    collection = domTree.documentElement
    return collection.getElementsByTagName("AQIDataPublishLive")    

def checkValue(value,reserve=0):
    if checkEmpty(value) == "":
        return None
    elif reserve != 0:
        return round(float(value)+0.00000002,reserve)
    elif reserve == 0:
        return int(float(value))
        
def checkEmpty(value):
    if value == '\u2014' or value is None:
        return ''
    else:
        return value
        
def getPrimaryPollutant(string):

    if string == '\u2014':
        return 0
        
    primaryPollutant = 0b000000
    
    if string.find(u"臭氧") != -1 :
        primaryPollutant += 0b100000
    if string.find(u"一氧化碳") != -1 :
        primaryPollutant += 0b010000
    if string.find(u"二氧化硫") != -1 :
        primaryPollutant += 0b001000
    if string.find(u"二氧化氮") != -1 :
        primaryPollutant += 0b000100
    if string.find(u"PM2.5") != -1 :
        primaryPollutant += 0b000010
    if string.find(u"PM10") != -1 :
        primaryPollutant += 0b000001
    
    return primaryPollutant

def calcIAQI(value,valueType):
    
    if value is None: return 0
    
    boundary = [0,50,100,150,200,300,400,500]

    if valueType == "SO2":
        measure = [0,150,500,650,800]
    elif valueType == "SO2_24h":
        measure = [0,50,150,475,800,1600,2100,2620]
    elif valueType == "NO2":
        measure = [0,100,200,700,1200,2340,3090,3840]
    elif valueType == "NO2_24h":
        measure = [0,40,80,180,280,565,750,940]
    elif valueType == "CO":
        measure = [0,5,10,35,60,90,120,150]
    elif valueType == "CO_24h":
        measure = [0,2,4,14,24,36,48,60]
    elif valueType == "O3":
        measure = [0,160,200,300,400,800,1000,1200]
    elif valueType == "O3_8h":
        measure = [0,100,160,215,265,800]
    elif valueType == "PM2_5_24h":
        measure = [0,35,75,115,150,250,350,500]
    elif valueType == "PM10_24h":
        measure = [0,50,150,250,350,420,500,600]

    for i in range(0,len(measure)-1):
        if measure[i] <= value and value < measure[i+1]:
            IAQI =  float(boundary[i+1] - boundary[i]) / float(measure[i+1] - measure[i]) * (value - measure[i]) + boundary[i]
            return int(round(IAQI))

    return 0

def getTagData(dom,tagName):

    return dom.getElementsByTagName(tagName)[0].childNodes[0].data

def updateStationsInfo(connect,allStationsData):

    sql = "insert into station values (%s,%s,%s,%s,%s)"
    cursor = connect.cursor()

    for stationData in allStationsData:
        stationCode = getTagData(stationData,"StationCode")
        cityCode = getTagData(stationData,"CityCode")
        positionName = getTagData(stationData,"PositionName")
        longitude = getTagData(stationData,"Longitude")
        latitude = getTagData(stationData,"Latitude")

        try:
            cursor.execute(sql,(stationCode,cityCode,positionName,longitude,latitude))
        except:
            pass

    connect.commit()
    cursor.close()


def pullRawData(connect,allStationsData=None):
    
    allStationsData = getAllStationsData() if not allStationsData else allStationsData
    params = []
    
    for stationData in allStationsData:

        timePoint = datetime.datetime.strptime(getTagData(stationData,"TimePoint"),"%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M")

        stationCode = getTagData(stationData,"StationCode")
        
        AQI = checkValue(getTagData(stationData,"AQI"))

        o3 = checkValue(getTagData(stationData,"O3"))
        o3_24h = checkValue(getTagData(stationData,"O3_24h"))
        o3_8h = checkValue(getTagData(stationData,"O3_8h"))
        o3_8h_24h = checkValue(getTagData(stationData,"O3_8h_24h"))
        
        co = checkValue(getTagData(stationData,"CO"),1)
        co_24h = checkValue(getTagData(stationData,"CO_24h"),1)

        so2 = checkValue(getTagData(stationData,"SO2"))
        so2_24h = checkValue(getTagData(stationData,"SO2_24h"))
        
        no2 = checkValue(getTagData(stationData,"NO2"))
        no2_24h = checkValue(getTagData(stationData,"NO2_24h"))
        
        pm2_5 = checkValue(getTagData(stationData,"PM2_5"))
        pm2_5_24h = checkValue(getTagData(stationData,"PM2_5_24h"))
        
        pm10 = checkValue(getTagData(stationData,"PM10"))
        pm10_24h = checkValue(getTagData(stationData,"PM10_24h"))
        
        primaryPollutant = getPrimaryPollutant(getTagData(stationData,"PrimaryPollutant"))
        
        params.append([timePoint,stationCode,AQI,o3,o3_24h,o3_8h,o3_8h_24h,co,co_24h,so2,so2_24h,no2,no2_24h,pm2_5,pm2_5_24h,pm10,pm10_24h,primaryPollutant])

    
    sql = "insert into raw values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor = connect.cursor()
    
    try:
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        if e.pgcode == 23503: 
            updateStationsInfo(connect,allStationsData)
            pullRawData(connect,allStationsData)
    else:
        processData(connect)


def processData(connect):
    
    sqlLastHour = '''
        select
        work.city_code,
        work.aqi
        from work
        where work.time_point = (select max(time_point) - interval '1 hour' from raw)
        order by work.city_code
    '''
    
    sqlThisHour = '''
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
    
    cursor = connect.cursor()
    
    try:        
        cursor.execute(sqlLastHour)
        lastHourData = cursor.fetchall()
        cursor.execute(sqlThisHour)
        thisHourData = cursor.fetchall()
    except Exception as e:
        print(e)

    lastHourData = {item[0]:item[1] for item in lastHourData}
    params = []

    for cityData in thisHourData:

        param = list(cityData)
        for index in range(2,17):
            if index == 7 or index == 8: param[index] = checkValue(param[index],1)
            else: param[index] = checkValue(param[index])

        if not param[2]:
            aqiChange = None
        elif param[1] not in lastHourData:
            aqiChange = None
        elif not lastHourData[param[1]]:
            aqiChange = None
        else:
            aqiChange = param[2] - lastHourData[param[1]]

        param.insert(3,aqiChange)
        
        params.append(param)
        
    sql = "insert into work values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    try:        
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)


def compactTable(connect):

    sqlRaw = '''
        delete
        from raw 
        where raw.time_point < (select max(time_point) from raw)
    '''

    sqlWork = '''
        delete
        from work 
        where work.time_point < (select max(time_point) - interval '11 hour' from raw)
    '''

    cursor = connect.cursor()

    try:
        cursor.execute(sqlRaw)
        cursor.execute(sqlWork)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)
    
