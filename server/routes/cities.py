from flask import jsonify
from . import app, mysql

@app.route("/aqi/cities")
def cities():
    
    sql = '''
       SELECT 
       city_code, 
       city_name_zh, 
       city_name_en 
       FROM city 
       ORDER BY city_code
    '''

    cursor = mysql.get_db().cursor()
    cursor.execute(sql)
    out = cursor.fetchall()
    cursor.close()

    json_back = {}

    for city_data in out:

        json_back[city_data[0]] = [city_data[1],city_data[2]]

    return jsonify(json_back)