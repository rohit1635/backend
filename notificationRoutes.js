const express = require('express');
const router = express.Router();
const { sendNotification } = require('./kafkaProducer');

router.get('/', (req, res) => {
  const db = req.db;
  const query = 'SELECT * FROM notifications';
  db.query(query, (error, results) => {
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    res.json(results);
  });
});

router.post('/send', (req, res) => {
  const { notification_id, flight_id, message, method, recipient } = req.body;

  const notification = {
    notification_id,
    flight_id,
    message,
    method,
    recipient
  };

  sendNotification(notification);

  res.status(200).json({ message: 'Notification sent successfully' });
});

module.exports = router;

