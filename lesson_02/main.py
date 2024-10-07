from flask import Flask, request, jsonify
import os
import requests
import json
from dotenv import load_dotenv
from datetime import date

# Завантаження змінних середовища
load_dotenv()
auth_token = os.getenv("AUTH_TOKEN")

app = Flask(__name__)

# Функція для витягу даних з API
def fetch_sales_data(date_str, page):
    url = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"
    headers = {'Authorization': f'Token {auth_token}'}
    params = {'date': date_str, 'page': page}
    response = requests.get(url, headers=headers, params=params)
    return response.json(), response.status_code

# Функція для збереження даних у raw-директорію
def save_to_raw_dir(data, raw_dir, date_str, page=None):
    # Очищення директорії перед записом
    if os.path.exists(raw_dir):
        for file in os.listdir(raw_dir):
            os.remove(os.path.join(raw_dir, file))

    # Формування імені файлу
    if page is None:
        file_name = f'sales_{date_str}.json'
    else:
        file_name = f'sales_{date_str}_{page}.json'

    file_path = os.path.join(raw_dir, file_name)

    # Запис даних у файл
    with open(file_path, 'w') as f:
        json.dump(data, f)

# Маршрут для обробки POST-запиту
@app.route('/sales', methods=['POST'])
def get_sales():
    data = request.get_json()
    date_str = data.get("date")
    raw_dir = data.get("raw_dir")

    # Витягуємо дані по сторінках
    page = 1
    while True:
        sales_data, status_code = fetch_sales_data(date_str, page)
        if status_code == 200 and sales_data:
            save_to_raw_dir(sales_data, raw_dir, date_str, page)
            page += 1
        else:
            break

    return jsonify({"message": "Data saved successfully"}), 200

# Маршрут для обробки GET-запиту на кореневу сторінку
@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Sales API", 200

# Запуск Flask-сервера
if __name__ == '__main__':
    app.run(port=8081)
