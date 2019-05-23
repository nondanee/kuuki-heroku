from flask import g, jsonify
from . import main

@main.route('/cities')
def cities():
    
    sql = '''
       SELECT 
       city_code, 
       city_name_zh, 
       city_name_en 
       FROM city 
       ORDER BY city_code
    '''

    cursor = g.db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()

    body = {}

    for city_data in data:

        body[city_data[0]] = [city_data[1], city_data[2]]

    return jsonify(body)