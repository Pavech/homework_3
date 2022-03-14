import json
import sqlite3
from typing import Any
from jsonschema import validate
from jsonschema.exceptions import ValidationError


def read_json(json_file: str) -> Any:
    """Чтение файла и его валидация со схемой"""
    try:
        with open(json_file, 'r', encoding='utf-8') as file:
            json_dict = json.load(file)
        with open('goods.schema_.json', 'r', encoding='utf-8') as schema:
            json_schema = json.load(schema)
        validate(json_dict, json_schema)
        return json_dict
    except ValidationError:
        return 'Ошибка валидации'


def create_table(db: Any) -> None:
    cursor = db.cursor()
    cursor.executescript('''
CREATE TABLE IF NOT EXISTS shops (id INTEGER PRIMARY KEY AUTOINCREMENT, address VARCHAR(100));
CREATE TABLE IF NOT EXISTS shops_goods (id INTEGER PRIMARY KEY AUTOINCREMENT, id_good INT, 
        id_shop INT, amount INT, FOREIGN KEY (id_shop) REFERENCES shops (id));
CREATE TABLE IF NOT EXISTS goods (id INTEGER UNIQUE PRIMARY KEY NOT NULL, name VARCHAR(50), 
        package_id INT, FOREIGN KEY (id) REFERENCES shops_goods (id_good));
CREATE TABLE IF NOT EXISTS packages (id INTEGER PRIMARY KEY AUTOINCREMENT, type PACKAGE_TYPE,
        height FLOAT, width FLOAT, depth FLOAT, FOREIGN KEY (id) REFERENCES goods (package_id));;
''')


def insert(db: Any, json_data: dict) -> Any:
    """Запись данных в БД"""
    cursor = db.cursor()
    type_package = json_data["package_params"]["type"]
    width_package = json_data["package_params"]["width"]
    height_package = json_data["package_params"]["height"]
    depth_package = json_data["package_params"]["depth"]
    id_package = "SELECT MAX(id) FROM packages"
    id_goods = json_data["id"]
    name_goods = json_data["name"]
    data_shops = json_data["location_and_quantity"]
    try:
        cursor.execute(f"""INSERT INTO packages (type, height, width, depth)
            VALUES ('{type_package}', {height_package}, {width_package}, {depth_package});""")
        cursor.execute(f"""INSERT INTO goods (id, name, package_id)
            VALUES ({id_goods}, '{name_goods}', ({id_package}));""")
        for item in data_shops:
            cursor.execute(f"""INSERT INTO shops (address) VALUES ("{item["location"]}");""")
            id_shops = "SELECT MAX(id) FROM shops"
            cursor.execute(f"""INSERT INTO shops_goods (id_good, id_shop, amount)
            VALUES ({id_goods}, ({id_shops}), {item["amount"]});""")
        db.commit()
    except sqlite3.Error:
        return "Вносимые данные уже существуют в БД"
    return "Данные добавлены"


def main(json_file: str) -> None:
    """Основная функция."""
    json_data = read_json(json_file)
    db_conn = sqlite3.connect('db.sqlite')
    create_table(db_conn)
    insert(db_conn, json_data)
    db_conn.close()


if __name__ == "__main__":
    main('input_example.json')
