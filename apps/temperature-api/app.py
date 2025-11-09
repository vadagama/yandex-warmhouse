#!/usr/bin/env python3
"""
Temperature API - простое приложение для получения случайных значений температуры
"""

import random
import json
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# Маппинг между location и sensorId
LOCATION_TO_SENSOR_ID = {
    "Living Room": "1",
    "Bedroom": "2",
    "Kitchen": "3"
}

SENSOR_ID_TO_LOCATION = {
    "1": "Living Room",
    "2": "Bedroom",
    "3": "Kitchen"
}


def get_sensor_id_from_location(location: str) -> str:
    """Получить sensor ID на основе location"""
    return LOCATION_TO_SENSOR_ID.get(location, "0")


def get_location_from_sensor_id(sensor_id: str) -> str:
    """Получить location на основе sensor ID"""
    return SENSOR_ID_TO_LOCATION.get(sensor_id, "Unknown")


def generate_random_temperature() -> float:
    """Генерировать случайное значение температуры в диапазоне 18-25°C"""
    return round(random.uniform(18.0, 25.0), 2)


@app.route('/temperature', methods=['GET'])
def get_temperature():
    """
    Эндпоинт для получения температуры
    Параметры:
    - location: название комнаты (Living Room, Bedroom, Kitchen)
    - sensorId: идентификатор датчика (1, 2, 3)
    """
    location = request.args.get('location', '')
    sensor_id = request.args.get('sensorId', '')
    
    # Если location не указан, используем sensor ID
    if not location and sensor_id:
        location = get_location_from_sensor_id(sensor_id)
    
    # Если sensor ID не указан, генерируем на основе location
    if not sensor_id and location:
        sensor_id = get_sensor_id_from_location(location)
    
    # Если оба не указаны, используем значения по умолчанию
    if not location and not sensor_id:
        location = "Unknown"
        sensor_id = "0"
    
    # Генерируем случайное значение температуры
    temperature_value = generate_random_temperature()
    
    # Формируем ответ в формате, ожидаемом Go-приложением
    response = {
        "value": temperature_value,
        "unit": "celsius",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": location,
        "status": "active",
        "sensor_id": sensor_id,
        "sensor_type": "temperature",
        "description": f"Temperature sensor reading for {location}"
    }
    
    return jsonify(response), 200


@app.route('/temperature/<sensor_id>', methods=['GET'])
def get_temperature_by_sensor_id(sensor_id):
    """
    Эндпоинт для получения температуры по sensor ID
    Параметры:
    - sensor_id: идентификатор датчика (1, 2, 3)
    """
    location = get_location_from_sensor_id(sensor_id)
    
    # Генерируем случайное значение температуры
    temperature_value = generate_random_temperature()
    
    # Формируем ответ в формате, ожидаемом Go-приложением
    response = {
        "value": temperature_value,
        "unit": "celsius",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": location,
        "status": "active",
        "sensor_id": sensor_id,
        "sensor_type": "temperature",
        "description": f"Temperature sensor reading for {location}"
    }
    
    return jsonify(response), 200


@app.route('/health', methods=['GET'])
def health_check():
    """Health check эндпоинт"""
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=False)

