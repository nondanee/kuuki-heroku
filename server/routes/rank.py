from flask import g, request, jsonify
from . import main

@main.route("/rank")
def rank():

    order = "ASC"
    reverse = request.args.get("reverse")
    if reverse in ["1","true"]: order = "DESC"

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
        FROM work 
        WHERE time_point = (SELECT MAX(time_point) FROM work) 
        ORDER BY aqi {}
    '''.format(order)

    cursor = g.db.cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = {
        "time_point": out[0][0].strftime('%Y-%m-%d %H:%M'),
        "cities":[]
    }


    for city_data in out:

        if city_data[2] == None: continue

        city = {
            "city_code": city_data[1],
            "aqi": city_data[2],
            "aqi_change": city_data[3],
            "o3": city_data[4],
            "o3_24h": city_data[5],
            "o3_8h": city_data[6],
            "o3_8h_24h": city_data[7],
            "co": float(city_data[8]) if city_data[8] else city_data[8],
            "co_24h": float(city_data[9]) if city_data[9] else city_data[9],
            "so2": city_data[10],
            "so2_24h": city_data[11],
            "no2": city_data[12],
            "no2_24h": city_data[13],
            "pm2_5": city_data[14],
            "pm2_5_24h": city_data[15],
            "pm10": city_data[16],
            "pm10_24h": city_data[17],
        }

        json_back["cities"].append(city)

    return jsonify(json_back)