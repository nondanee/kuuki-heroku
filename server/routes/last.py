from flask import g, request, abort, jsonify
from . import main, codes

@main.route('/last<int:hours>h')
def last(hours):

    if hours < 1 or hours > 12: abort(400)

    city = request.args.get("city")
    if city is None: abort(400)
    if not codes.available(city): abort(400)

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
        WHERE city_code = {}
        AND time_point > (SELECT MAX(time_point) - INTERVAL '{}' HOUR FROM work)
    '''.format(city,hours)

    cursor = g.db.cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = [None] * hours

    for hour_data in out:

        hour = {
            "time_point": hour_data[0].strftime('%Y-%m-%d %H:%M'),
            "aqi": hour_data[2],
            "aqi_change": hour_data[3],
            "o3": hour_data[4],
            "o3_24h": hour_data[5],
            "o3_8h": hour_data[6],
            "o3_8h_24h": hour_data[7],
            "co": float(hour_data[8]) if hour_data[8] != None else hour_data[8],
            "co_24h": float(hour_data[9]) if hour_data[9] != None else hour_data[9],
            "so2": hour_data[10],
            "so2_24h": hour_data[11],
            "no2": hour_data[12],
            "no2_24h": hour_data[13],
            "pm2_5": hour_data[14],
            "pm2_5_24h": hour_data[15],
            "pm10": hour_data[16],
            "pm10_24h": hour_data[17]
        }

        json_back[hours - int((out[-1][0] - hour_data[0]).total_seconds()//3600) - 1] = hour

    return jsonify(json_back)