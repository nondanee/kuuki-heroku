# -*- coding: utf-8 -*-
import re, base64, zlib, io
import urllib.request
import xml.dom.minidom, datetime
from . import wcf

def getAllStationsData():

    output = io.StringIO()
    output.write('<GetAllAQIPublishLive xmlns="http://tempuri.org/"></GetAllAQIPublishLive>')
    output.seek(0)

    r = wcf.xml2records.XMLParser.parse(output)
    rec = wcf.records.dump_records(r)
    
    req = urllib.request.Request(
        url = "http://106.37.208.233:20035/ClientBin/Env-CnemcPublish-RiaServices-EnvCnemcPublishDomainService.svc/binary/GetAllAQIPublishLive",
        data = rec,
        headers = {"Content-Type": "application/msbin1"})
        
    res = urllib.request.urlopen(req,timeout = 10)
    dat = res.read()

    buf = io.BytesIO(dat)
    r = wcf.records.Record.parse(buf)

    wcf.records.print_records(r, fp=output)
    output.seek(0)

    pat = re.compile('<[^>]+>')
    enc = pat.sub('', output.readlines()[1][1:])[:-1]

    enc = base64.b64decode(enc)
    enc = zlib.decompress(enc)

    domTree = xml.dom.minidom.parseString(enc)
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
    if value == u'\u2014' or value is None:
        return ''
    else:
        return value
        
def getPrimaryPollutant(string):

    if string == u'\u2014':
        return 0
        
    PrimaryPollutant = 0b000000
    
    if string.find(u"臭氧") != -1 :
        PrimaryPollutant += 0b100000
    if string.find(u"一氧化碳") != -1 :
        PrimaryPollutant += 0b010000
    if string.find(u"二氧化硫") != -1 :
        PrimaryPollutant += 0b001000
    if string.find(u"二氧化氮") != -1 :
        PrimaryPollutant += 0b000100
    if string.find(u"PM2.5") != -1 :
        PrimaryPollutant += 0b000010
    if string.find(u"PM10") != -1 :
        PrimaryPollutant += 0b000001
    
    return PrimaryPollutant

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

def pullRawData(connect):
    
    allStationsData = getAllStationsData()
    length = len(allStationsData)
    params = []
    
    for i in range(0,length):

        stationData = allStationsData[i]

        TimePoint = datetime.datetime.strptime(getTagData(stationData,"TimePoint"),"%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M")

        StationCode = getTagData(stationData,"StationCode")
        
        AQI = checkValue(getTagData(stationData,"AQI"))

        O3 = checkValue(getTagData(stationData,"O3"))
        O3_24h = checkValue(getTagData(stationData,"O3_24h"))
        O3_8h = checkValue(getTagData(stationData,"O3_8h"))
        O3_8h_24h = checkValue(getTagData(stationData,"O3_8h_24h"))
        
        CO = checkValue(getTagData(stationData,"CO"),1)
        CO_24h = checkValue(getTagData(stationData,"CO_24h"),1)

        SO2 = checkValue(getTagData(stationData,"SO2"))
        SO2_24h = checkValue(getTagData(stationData,"SO2_24h"))
        
        NO2 = checkValue(getTagData(stationData,"NO2"))
        NO2_24h = checkValue(getTagData(stationData,"NO2_24h"))
        
        PM2_5 = checkValue(getTagData(stationData,"PM2_5"))
        PM2_5_24h = checkValue(getTagData(stationData,"PM2_5_24h"))
        
        PM10 = checkValue(getTagData(stationData,"PM10"))
        PM10_24h = checkValue(getTagData(stationData,"PM10_24h"))
        
        PrimaryPollutant = getPrimaryPollutant(getTagData(stationData,"PrimaryPollutant"))
        
        params.append([TimePoint,StationCode,AQI,O3,O3_24h,O3_8h,O3_8h_24h,CO,CO_24h,SO2,SO2_24h,NO2,NO2_24h,PM2_5,PM2_5_24h,PM10,PM10_24h,PrimaryPollutant])

    
    sql = "INSERT INTO raw VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    cursor = connect.cursor()
    
    try:
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)
    else:
        processData(connect)


def processData(connect):
    
    sqlLastHour = '''
        SELECT 
        aqi 
        FROM work 
        WHERE work.time_point = (SELECT DATE_SUB( MAX(time_point), INTERVAL 1 HOUR) FROM raw)
        ORDER BY work.city_code
    '''
    
    sqlThisHour = '''
        SELECT
        raw.time_point,
        station.city_code,
        AVG(raw.aqi),
        AVG(raw.o3),
        AVG(raw.o3_24h),
        AVG(raw.o3_8h),
        AVG(raw.o3_8h_24h),
        AVG(raw.co),
        AVG(raw.co_24h),
        AVG(raw.so2),
        AVG(raw.so2_24h),
        AVG(raw.no2),
        AVG(raw.no2_24h),
        AVG(raw.pm2_5),
        AVG(raw.pm2_5_24h),
        AVG(raw.pm10),
        AVG(raw.pm10_24h)
        FROM raw,station 
        WHERE raw.time_point = (SELECT MAX(time_point) FROM raw) 
        AND raw.station_code = station.station_code 
        GROUP BY station.city_code
    '''
    
    
    cursor = connect.cursor()
    
    try:        
        cursor.execute(sqlLastHour)
        lastHourData = cursor.fetchall()
        cursor.execute(sqlThisHour)
        thisHourData = cursor.fetchall()
    except Exception as e:
        print(e)
    
    length = len(thisHourData)
    
    params = []

    for i in range(0,length):

        param = list(thisHourData[i])
        for j in range(2,17):
            if j == 7 or j == 8: param[j] = checkValue(param[j],1)
            else: param[j] = checkValue(param[j])

        if len(lastHourData) - 1 < i: 
            aqiChange = None
        elif param[2] == None or lastHourData[i][0] == None: 
            aqiChange = None
        else: 
            aqiChange = param[2] - lastHourData[i][0]      

        param.insert(3,aqiChange)
        
        params.append(param)
        
        
    sql = "INSERT INTO work VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    try:        
        cursor.executemany(sql,params)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)


def compactTable(connect):

    sqlRaw = '''
        DELETE
        FROM raw 
        WHERE raw.time_point < (SELECT MAX(time_point) FROM work)
    '''

    sqlWork = '''
        DELETE
        FROM work 
        WHERE work.time_point < (SELECT DATE_SUB( MAX(time_point), INTERVAL 11 HOUR) FROM raw)
    '''

    cursor = connect.cursor()

    try:
        cursor.execute(sqlRaw)
        cursor.execute(sqlWork)
        connect.commit()
        cursor.close()
    except Exception as e:
        print(e)
    
