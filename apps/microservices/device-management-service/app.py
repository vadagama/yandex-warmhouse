"""
Device Management Service - простейший микросервис для управления устройствами
"""
from flask import Flask, jsonify, request
from datetime import datetime
import uuid
import os
import pika
import json

app = Flask(__name__)

# Простейшее хранилище в памяти (в реальности - БД)
devices = {}
device_types = [
    {"id": str(uuid.uuid4()), "name": "Датчик температуры", "category": "sensor", "protocol": "HTTP"},
    {"id": str(uuid.uuid4()), "name": "Модуль управления котлом", "category": "heating", "protocol": "MQTT"},
]

# Подключение к RabbitMQ
rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))

def publish_event(event_type: str, payload: dict):
    """Публикация события в RabbitMQ"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
        )
        channel = connection.channel()
        channel.exchange_declare(exchange='smarthome_events', exchange_type='topic', durable=True)
        
        channel.basic_publish(
            exchange='smarthome_events',
            routing_key=event_type,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Сохранять сообщения
                content_type='application/json'
            )
        )
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Ошибка подключения к RabbitMQ: {e}")
        # В production здесь можно добавить retry логику или отправку в очередь для повторной попытки
    except pika.exceptions.AMQPChannelError as e:
        print(f"Ошибка канала RabbitMQ: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при публикации события в RabbitMQ: {e}")
        # Логируем, но не прерываем выполнение основного потока

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"}), 200

@app.route('/api/v1/devices/device-types', methods=['GET'])
def get_device_types():
    """Получить список типов устройств"""
    return jsonify({"device_types": device_types}), 200

@app.route('/api/v1/devices', methods=['GET'])
def get_devices():
    """Получить список устройств"""
    room_id = request.args.get('room_id')
    device_type_id = request.args.get('device_type_id')
    status = request.args.get('status')
    
    filtered_devices = list(devices.values())
    
    if room_id:
        filtered_devices = [d for d in filtered_devices if d.get('room_id') == room_id]
    if device_type_id:
        filtered_devices = [d for d in filtered_devices if d.get('device_type_id') == device_type_id]
    if status:
        filtered_devices = [d for d in filtered_devices if d.get('status') == status]
    
    return jsonify({"devices": filtered_devices, "total": len(filtered_devices)}), 200

@app.route('/api/v1/devices/<device_id>', methods=['GET'])
def get_device(device_id):
    """Получить информацию об устройстве"""
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found", "message": f"Device {device_id} not found"}), 404
    return jsonify(device), 200

@app.route('/api/v1/devices', methods=['POST'])
def register_device():
    """Зарегистрировать новое устройство"""
    data = request.json
    
    if not data or not data.get('serial_number') or not data.get('device_type_id') or not data.get('name'):
        return jsonify({"error": "Bad Request", "message": "Missing required fields"}), 400
    
    device_id = str(uuid.uuid4())
    device = {
        "id": device_id,
        "serial_number": data.get('serial_number'),
        "name": data.get('name'),
        "device_type_id": data.get('device_type_id'),
        "room_id": data.get('room_id'),
        "status": "active",
        "ip_address": data.get('ip_address'),
        "mac_address": data.get('mac_address'),
        "firmware_version": "1.0.0",
        "registered_at": datetime.utcnow().isoformat() + "Z",
        "last_seen_at": datetime.utcnow().isoformat() + "Z"
    }
    
    devices[device_id] = device
    
    # Публикация события регистрации устройства
    event_payload = {
        "device_id": device_id,
        "user_id": data.get('user_id', str(uuid.uuid4())),  # В реальности из токена
        "device_type": "temperature_sensor",  # Упрощенно
        "serial_number": data.get('serial_number'),
        "name": data.get('name'),
        "room_id": data.get('room_id'),
        "registered_at": device["registered_at"]
    }
    publish_event("device.registered", event_payload)
    
    return jsonify(device), 201

@app.route('/api/v1/devices/<device_id>', methods=['PUT'])
def update_device(device_id):
    """Обновить информацию об устройстве"""
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found", "message": f"Device {device_id} not found"}), 404
    
    data = request.json
    if data.get('name'):
        device['name'] = data['name']
    if data.get('room_id'):
        device['room_id'] = data['room_id']
    if data.get('status'):
        device['status'] = data['status']
    
    device['last_seen_at'] = datetime.utcnow().isoformat() + "Z"
    devices[device_id] = device
    
    return jsonify(device), 200

@app.route('/api/v1/devices/<device_id>', methods=['DELETE'])
def delete_device(device_id):
    """Удалить устройство"""
    if device_id not in devices:
        return jsonify({"error": "Device not found", "message": f"Device {device_id} not found"}), 404
    
    del devices[device_id]
    return "", 204

@app.route('/api/v1/devices/<device_id>/status', methods=['GET'])
def get_device_status(device_id):
    """Получить статус устройства"""
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found", "message": f"Device {device_id} not found"}), 404
    
    return jsonify({
        "device_id": device_id,
        "status": device.get('status', 'offline'),
        "last_seen": device.get('last_seen_at'),
        "battery_level": 85,  # Упрощенно
        "signal_strength": -65  # Упрощенно
    }), 200

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8082))
    app.run(host='0.0.0.0', port=port, debug=True)

