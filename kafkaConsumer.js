const kafka = require('kafka-node');
const { KafkaClient, Consumer } = kafka;
const nodemailer = require('nodemailer');
const twilio = require('twilio');
require('dotenv').config();

const twilioClient = twilio(process.env.TWILIO_SID, process.env.TWILIO_AUTH_TOKEN);
const twilioPhoneNumber = process.env.TWILIO_PHONE;

const client = new KafkaClient({ kafkaHost: 'localhost:9092' });
const consumer = new Consumer(
  client,
  [{ topic: 'notifications', partition: 0 }],
  { autoCommit: true }
);

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL,
    pass: process.env.GMAIL_PASSWORD
  }
});

const sendEmail = (to, subject, message) => {
  const mailOptions = {
    from: process.env.GMAIL,
    to: to,
    subject: subject,
    text: message
  };

  transporter.sendMail(mailOptions, (error, info) => {
    if (error) {
      console.error('Error sending email:', error);
    } else {
      console.log('Email sent:', info.response);
    }
  });
};

const sendSMS = (to, message) => {
  twilioClient.messages.create({
    body: message,
    from: twilioPhoneNumber,
    to: to
  })
  .then((message) => {
    console.log('SMS sent:', message.sid);
  })
  .catch((error) => {
    console.error('Error sending SMS:', error);
  });
};

consumer.on('message', (message) => {
  console.log('Raw message from Kafka:', message);
  const notification = JSON.parse(message.value);
  console.log('Parsed notification:', notification);

  const { method, recipient, message: notificationMessage } = notification;

  if (method === 'Email') {
    console.log(`Sending email to ${recipient} with message: ${notificationMessage}`);
    sendEmail(recipient, 'Flight Notification', notificationMessage);
  } else if (method === 'SMS') {
    console.log(`Sending SMS to ${recipient} with message: ${notificationMessage}`);
    sendSMS(recipient, notificationMessage);
  } else {
    console.error('Unsupported notification method:', method);
  }
});

consumer.on('error', (error) => {
  console.error('Error in Kafka Consumer', error);
});
