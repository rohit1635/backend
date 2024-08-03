const express = require('express');
const router = express.Router();
const { sendNotification } = require('./kafkaProducer');

router.get('/', (req, res) => {
  const db = req.db;
  const sql = 'SELECT * FROM flights';
  db.query(sql, (err, results) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(results);
  });
});

router.post('/update', (req, res) => {
  const db = req.db;
  const { flight_id, status, departure_gate, arrival_gate, new_departure_time } = req.body;
  
  let updateFlightSql;
  let updateFlightParams;

  if (status === 'Delayed') {
    updateFlightSql = `UPDATE flights 
                       SET status = ?, departure_gate = ?, arrival_gate = ?, actual_departure = ?
                       WHERE flight_id = ?`;
    updateFlightParams = [status, departure_gate, arrival_gate, new_departure_time, flight_id];
  } else {
    updateFlightSql = `UPDATE flights 
                       SET status = ?, departure_gate = ?, arrival_gate = ?
                       WHERE flight_id = ?`;
    updateFlightParams = [status, departure_gate, arrival_gate, flight_id];
  }

  db.query(updateFlightSql, updateFlightParams, (err, result) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }

    // Fetch updated notifications for the given flight_id
    const getNotificationsSql = 'SELECT * FROM notifications WHERE flight_id = ?';
    db.query(getNotificationsSql, [flight_id], (err, notifications) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }

      // Update each notification in the notifications table and send it
      notifications.forEach(notification => {
        let message = `Your flight ${flight_id} is ${status}. Departure gate: ${departure_gate}. Arrival gate: ${arrival_gate}.`;
        if (status === 'Delayed') {
          message += ` New departure time: ${new_departure_time}.`;
        }

        const updateNotificationSql = `UPDATE notifications 
                                       SET message = ?
                                       WHERE notification_id = ?`;

        db.query(updateNotificationSql, [message, notification.notification_id], (err, result) => {
          if (err) {
            console.error('Error updating notification:', err.message);
          } else {
            // Send the notification using the method in the notifications table
            const updatedNotification = { ...notification, message };
            sendNotification(updatedNotification);
          }
        });
      });

      res.status(200).json({ message: 'Flight and notifications updated successfully' });
    });
  });
});

module.exports = router;
