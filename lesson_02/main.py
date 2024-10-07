from flask import Flask, request, jsonify
import os
import requests
from dotenv import load_dotenv
import json
from datetime import date

# Завантаження токену зі змінної середовища
load_dotenv()
auth_token = os.getenv("AUTH_TOKEN")

app = Flask(__name__)

# Функція для отримання даних з API
def fetch_sales_data():
    url = "https://fake-api-vycpfa6oca-uc.a.run.app/sales"
    headers = {'Authorization': f'Token {auth_token}'}
    response = requests.get(url, headers=headers)
    return response.json()

# Функція для збереження даних у raw-директорію
def save_to_raw_dir(data, raw_dir):
    today = date.today().strftime('%Y-%m-%d')
    file_path = os.path.join(raw_dir, f'sales_{today}.json')

    # Очищення старих файлів перед записом нових
    if os.path.exists(raw_dir):
        for file in os.listdir(raw_dir):
            os.remove(os.path.join(raw_dir, file))

    # Запис даних у JSON-файл
    with open(file_path, 'w') as f:
        json.dump(data, f)

# Маршрут для прийому POST-запитів
@app.route('/sales', methods=['POST'])
def get_sales():
    data = request.get_json()
    raw_dir = data.get("raw_dir")
    sales_data = fetch_sales_data()
    save_to_raw_dir(sales_data, raw_dir)
    return jsonify({"message": "Data saved successfully"}), 200

# Запуск Flask-сервера
if __name__ == '__main__':
    app.run(port=8081)
