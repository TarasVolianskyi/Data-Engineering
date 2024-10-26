import os
import json
from flask import Flask, request, jsonify
from fastavro import writer, parse_schema

app = Flask(__name__)

# Avro schema
schema = {
    "type": "record",
    "name": "Sales",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "date", "type": "string"},
        {"name": "amount", "type": "float"}
    ]
}


def convert_json_to_avro(json_data, avro_file_path):
    with open(avro_file_path, 'wb') as avro_file:
        writer(avro_file, parse_schema(schema), json_data)


@app.route('/convert_to_avro', methods=['POST'])
def convert_to_avro():
    data = request.get_json()

    raw_dir = data.get('raw_dir')
    stg_dir = data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return jsonify({'error': 'raw_dir або stg_dir не надано'}), 400

    os.makedirs(stg_dir, exist_ok=True)

    for file_name in os.listdir(raw_dir):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(raw_dir, file_name)

            with open(json_file_path, 'r') as json_file:
                try:
                    sales_data = json.load(json_file)
                except json.JSONDecodeError as e:
                    return jsonify({'error': f'Помилка при зчитуванні JSON: {file_name}, помилка: {str(e)}'}), 500

            avro_file_name = file_name.replace('.json', '.avro')
            avro_file_path = os.path.join(stg_dir, avro_file_name)

            try:
                convert_json_to_avro(sales_data, avro_file_path)
            except Exception as e:
                return jsonify({'error': f'Помилка при записі Avro-файлу: {avro_file_name}, помилка: {str(e)}'}), 500

    return jsonify({'message': 'Дані успішно конвертовані у Avro'}), 200

if __name__ == '__main__':
    app.run(port=8082)
