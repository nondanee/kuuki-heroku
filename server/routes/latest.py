from flask import g, request, abort, jsonify
from . import main, code

@main.route('/latest')
def latest():

    cities = request.args.get('cities')
    if not cities: abort(400)

    cities = cities.split(',')
    if len(list(set(cities))) != len(cities): abort(400)

    for city in cities: 
        if not code.available(city): 
            abort(400)

    sql = '''
        SELECT 
        time_point,
        city_code,
        aqi,
        aqi_change,
        o3,
        o3_24h,
        o3_8h,
        o3_8h_24h,
        co,
        co_24h,
        so2,
        so2_24h,
        no2,
        no2_24h,
        pm2_5,
        pm2_5_24h,
        pm10,
        pm10_24h 
        FROM (
            SELECT 
            time_point,
            city_code,
            aqi,
            aqi_change,
            o3,
            o3_24h,
            o3_8h,
            o3_8h_24h,
            co,
            co_24h,
            so2,
            so2_24h,
            no2,
            no2_24h,
            pm2_5,
            pm2_5_24h,
            pm10,
            pm10_24h
            FROM work
            WHERE time_point = (SELECT MAX(time_point) FROM work)
        ) nearest_data
        WHERE city_code IN ({})
    '''.format(','.join(cities))

    cursor = g.db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    body = {'cities': [None] * len(cities)}
    body['time_point'] = data[0][0].strftime('%Y-%m-%d %H:%M') if data else None
    
    for city_data in data:

        city = {
            'city_code': city_data[1],
            'aqi': city_data[2],
            'aqi_change': city_data[3],
            'o3': city_data[4],
            'o3_24h': city_data[5],
            'o3_8h': city_data[6],
            'o3_8h_24h': city_data[7],
            'co': float(city_data[8]) if city_data[8] is not None else city_data[8],
            'co_24h': float(city_data[9]) if city_data[9] is not None else city_data[9],
            'so2': city_data[10],
            'so2_24h': city_data[11],
            'no2': city_data[12],
            'no2_24h': city_data[13],
            'pm2_5': city_data[14],
            'pm2_5_24h': city_data[15],
            'pm10': city_data[16],
            'pm10_24h': city_data[17]
        }
        
        body['cities'][cities.index(str(city_data[1]))] = city

    return jsonify(body)