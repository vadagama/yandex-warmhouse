/**
 * Telemetry Service - простейший микросервис для управления телеметрией
 */
const express = require('express');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

// Простейшее хранилище в памяти (в реальности - БД)
const telemetryData = {};

// Конфигурация RabbitMQ
const RABBITMQ_HOST = process.env.RABBITMQ_HOST || 'localhost';
const RABBITMQ_PORT = process.env.RABBITMQ_PORT || 5672;
let rabbitmqConnection = null;
let rabbitmqChannel = null;

/**
 * Подключение к RabbitMQ и подписка на события
 */
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(`amqp://${RABBITMQ_HOST}:${RABBITMQ_PORT}`);
    rabbitmqConnection = connection;
    rabbitmqChannel = await connection.createChannel();
    
    // Объявление exchange
    await rabbitmqChannel.assertExchange('smarthome_events', 'topic', { durable: true });
    
    // Подписка на события регистрации устройств
    const queue = await rabbitmqChannel.assertQueue('', { exclusive: true });
    await rabbitmqChannel.bindQueue(queue.queue, 'smarthome_events', 'device.registered');
    
    rabbitmqChannel.consume(queue.queue, (msg) => {
      if (msg) {
        const event = JSON.parse(msg.content.toString());
        console.log('Получено событие регистрации устройства:', event);
        // Инициализация хранилища телеметрии для нового устройства
        if (!telemetryData[event.device_id]) {
          telemetryData[event.device_id] = [];
        }
        rabbitmqChannel.ack(msg);
      }
    });
    
    console.log('Подключено к RabbitMQ и подписано на события');
  } catch (error) {
    console.error('Ошибка подключения к RabbitMQ:', error);
  }
}

/**
 * Публикация события в RabbitMQ
 */
async function publishEvent(eventType, payload) {
  try {
    if (!rabbitmqChannel) {
      await connectRabbitMQ();
    }
    
    await rabbitmqChannel.publish(
      'smarthome_events',
      eventType,
      Buffer.from(JSON.stringify(payload)),
      { persistent: true }
    );
  } catch (error) {
    console.error('Ошибка публикации события:', error);
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Получить последние показания устройства
app.get('/api/v1/telemetry/devices/:deviceId/metrics', (req, res) => {
  const { deviceId } = req.params;
  const metricType = req.query.metric_type;
  const limit = parseInt(req.query.limit) || 10;
  
  if (!telemetryData[deviceId] || telemetryData[deviceId].length === 0) {
    return res.status(404).json({
      error: 'Not Found',
      message: `No telemetry data found for device ${deviceId}`
    });
  }
  
  let metrics = telemetryData[deviceId];
  
  if (metricType) {
    metrics = metrics.filter(m => m.metric_type === metricType);
  }
  
  metrics = metrics.slice(-limit);
  
  res.json({
    device_id: deviceId,
    metrics: metrics
  });
});

// Получить историю показаний устройства
app.get('/api/v1/telemetry/devices/:deviceId/metrics/history', (req, res) => {
  const { deviceId } = req.params;
  const metricType = req.query.metric_type;
  const from = req.query.from;
  const to = req.query.to;
  const limit = parseInt(req.query.limit) || 100;
  
  if (!telemetryData[deviceId] || telemetryData[deviceId].length === 0) {
    return res.status(404).json({
      error: 'Not Found',
      message: `No telemetry data found for device ${deviceId}`
    });
  }
  
  let metrics = [...telemetryData[deviceId]];
  
  if (metricType) {
    metrics = metrics.filter(m => m.metric_type === metricType);
  }
  
  if (from) {
    metrics = metrics.filter(m => new Date(m.timestamp) >= new Date(from));
  }
  
  if (to) {
    metrics = metrics.filter(m => new Date(m.timestamp) <= new Date(to));
  }
  
  metrics = metrics.slice(-limit);
  
  res.json({
    device_id: deviceId,
    metric_type: metricType || 'all',
    data_points: metrics
  });
});

// Получить показания всех устройств в комнате
app.get('/api/v1/telemetry/rooms/:roomId/metrics', (req, res) => {
  const { roomId } = req.params;
  const metricType = req.query.metric_type;
  
  // Упрощенно: возвращаем данные для всех устройств
  // В реальности нужно получать список устройств из Device Management Service
  const devices = Object.keys(telemetryData);
  const result = devices.map(deviceId => {
    let metrics = telemetryData[deviceId];
    if (metricType) {
      metrics = metrics.filter(m => m.metric_type === metricType);
    }
    return {
      device_id: deviceId,
      device_name: `Device ${deviceId}`,
      metrics: metrics.slice(-10) // Последние 10 записей
    };
  });
  
  res.json({
    room_id: roomId,
    devices: result
  });
});

// Внутренний endpoint для приема телеметрии (вызывается монолитом или другими сервисами)
app.post('/api/v1/telemetry/internal/receive', (req, res) => {
  const { device_id, metric_type, value, unit, timestamp, metadata } = req.body;
  
  if (!device_id || !metric_type || value === undefined) {
    return res.status(400).json({
      error: 'Bad Request',
      message: 'Missing required fields: device_id, metric_type, value'
    });
  }
  
  const telemetryEntry = {
    id: uuidv4(),
    device_id: device_id,
    metric_type: metric_type,
    value: value,
    unit: unit || '°C',
    timestamp: timestamp || new Date().toISOString(),
    metadata: metadata || {}
  };
  
  if (!telemetryData[device_id]) {
    telemetryData[device_id] = [];
  }
  
  telemetryData[device_id].push(telemetryEntry);
  
  // Ограничиваем историю последними 1000 записями
  if (telemetryData[device_id].length > 1000) {
    telemetryData[device_id] = telemetryData[device_id].slice(-1000);
  }
  
  // Публикация события получения телеметрии
  publishEvent('telemetry.received', telemetryEntry);
  
  res.status(201).json(telemetryEntry);
});

// Инициализация
const PORT = process.env.PORT || 8083;

connectRabbitMQ().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Telemetry Service запущен на порту ${PORT}`);
  });
});

