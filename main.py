from flask import Flask, request
import psycopg2

app = Flask(__name__)

conn = psycopg2.connect(user='postgres', password='Catans666', host='localhost', port='5432',
                        database='rpp1kopbru')  # Подключение к базе данных
cursor = conn.cursor()  # создаём курсор для выполнения SQL-запросов


@app.route("/v1/add/region", methods=['POST'])
def add_region():
    try:
        if 'id' not in request.json:
            error_body = {'reason': 'id пуст'}
            return (error_body), 400

        if 'name' not in request.json:
            error_body = {'reason': 'name пуст'}
            return (error_body), 400

        id = request.json['id']
        name = request.json['name']

        cursor.execute("SELECT * FROM region WHERE id = %(id)s", {'id': id})

        row = cursor.fetchone()

        if row is not None:
            return ({'error': 'Регион с указанным id уже существует'}), 400

        cursor.execute("INSERT INTO region(id, name) VALUES (%(id)s, %(name)s)", {'name': name, 'id': id})
        conn.commit()
    except Exception as e:
        return ({'error': f'Ошибка при добавлении региона: {str(e)}'}), 400

    return ({'message': 'Регион успешно добавлен'}), 200


@app.route("/v1/add/tax-param", methods=['POST'])
def add_tax_param():
    try:
        # Извлекаем данные из тела запроса
        data = request.json

        # Проверяем обязательные поля в запросе
        required_fields = ['city_id', 'from_hp_car', 'to_hp_car', 'from_production_year_car', 'to_production_year_car', 'rate']
        for field in required_fields:
            if field not in data:
                error_body = {'reason': f'{field} пуст'}
                return (error_body), 400

        city_id = data['city_id']
        from_hp_car = data['from_hp_car']
        to_hp_car = data['to_hp_car']
        from_production_year_car = data['from_production_year_car']
        to_production_year_car = data['to_production_year_car']
        rate = data['rate']

        cursor.execute("SELECT * FROM region WHERE id = %(city_id)s", {'city_id': city_id})
        region_row = cursor.fetchone()
        if region_row is None:
            return ({'error': 'Регион с указанным id не найден'}), 400

        cursor.execute("SELECT * FROM tax_param WHERE city_id = %(city_id)s AND from_hp_car = %(from_hp_car)s AND to_hp_car = %(to_hp_car)s AND from_production_year_car = %(from_production_year_car)s AND to_production_year_car = %(to_production_year_car)s",
                       {'city_id': city_id, 'from_hp_car': from_hp_car, 'to_hp_car': to_hp_car, 'from_production_year_car': from_production_year_car, 'to_production_year_car': to_production_year_car})

        tax_param_row = cursor.fetchone()
        if tax_param_row is not None:
            return ({'error': 'Объект налогообложения с такими параметрами уже существует'}), 400

        cursor.execute("INSERT INTO tax_param(city_id, from_hp_car, to_hp_car, from_production_year_car, to_production_year_car, rate) VALUES (%(city_id)s, %(from_hp_car)s, %(to_hp_car)s, %(from_production_year_car)s, %(to_production_year_car)s, %(rate)s)",
                       {'city_id': city_id, 'from_hp_car': from_hp_car, 'to_hp_car': to_hp_car, 'from_production_year_car': from_production_year_car, 'to_production_year_car': to_production_year_car, 'rate': rate})
        conn.commit()
    except Exception as e:
        return ({'error': f'Ошибка при добавлении объекта налогообложения: {str(e)}'}), 400

    return ({'message': 'Объект налогообложения успешно добавлен'}), 200


@app.route("/v1/add/auto", methods=['POST'])
def add_auto():
    try:
        # Извлекаем данные из тела запроса
        data = request.json

        # Проверяем обязательные поля в запросе
        required_fields = ['city_id', 'name', 'horse_power', 'production_year']
        for field in required_fields:
            if field not in data:
                error_body = {'reason': f'{field} пуст'}
                return (error_body), 400

        city_id = data['city_id']
        name = data['name']
        horse_power = data['horse_power']
        production_year = data['production_year']

        cursor.execute("SELECT * FROM region WHERE id = %(city_id)s", {'city_id': city_id})
        region_row = cursor.fetchone()
        if region_row is None:
            return ({'error': 'Регион с указанным id не найден'}), 400

        cursor.execute("SELECT * FROM tax_param WHERE city_id = %(city_id)s AND from_hp_car <= %(horse_power)s AND to_hp_car >= %(horse_power)s AND from_production_year_car <= %(production_year)s AND to_production_year_car >= %(production_year)s",
                       {'city_id': city_id, 'horse_power': horse_power, 'production_year': production_year})

        tax_param_row = cursor.fetchone()
        if tax_param_row is None:
            return ({'error': 'Объект налогообложения не найден'}), 400

        rate = tax_param_row[6]
        tax = rate * horse_power

        cursor.execute("INSERT INTO auto(city_id, tax_id, name, horse_power, production_year, tax) VALUES (%(city_id)s, %(tax_id)s, %(name)s, %(horse_power)s, %(production_year)s, %(tax)s)",
                       {'city_id': city_id, 'tax_id': tax_param_row[0], 'name': name, 'horse_power': horse_power, 'production_year': production_year, 'tax': tax})
        conn.commit()
    except Exception as e:
        return ({'error': f'Ошибка при добавлении автомобиля: {str(e)}'}), 400

    return ({'message': 'Автомобиль успешно добавлен и налог рассчитан'}), 200


@app.route("/v1/auto", methods=['GET'])
def get_auto():
    try:
        # Извлекаем параметры запроса
        auto_id = request.args.get('auto_id')

        # Выполняем запрос к базе данных для получения информации по автомобилю по его идентификатору
        cursor.execute("SELECT * FROM auto WHERE id = %(auto_id)s", {'auto_id': auto_id})
        auto_data = cursor.fetchone()

        if auto_data is None:
            return ({'error': 'Автомобиль с указанным идентификатором не найден'}), 404

        # Формируем ответ
        response_data = {
            'id': auto_data[0],
            'city_id': auto_data[1],
            'tax_id': auto_data[2],
            'name': auto_data[3],
            'horse_power': auto_data[4],
            'production_year': auto_data[5],
            'tax': auto_data[6]
        }

        return (response_data), 200
    except Exception as e:
        return ({'error': f'Ошибка при запросе информации по автомобилю: {str(e)}'}), 400

if __name__ == "__main__":
    app.run(debug=True)