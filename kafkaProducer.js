const kafka = require('kafka-node');
const { KafkaClient, Producer } = kafka;

const client = new KafkaClient({ kafkaHost: 'localhost:9092' }); 
const producer = new Producer(client);

producer.on('ready', () => {
  console.log('Kafka Producer is connected and ready.');
});

producer.on('error', (error) => {
  console.error('Error in Kafka Producer', error);
});

const sendNotification = (message) => {
  const payloads = [
    { topic: 'notifications', messages: JSON.stringify(message) }
  ];

  producer.send(payloads, (error, data) => {
    if (error) {
      console.error('Error sending notification to Kafka', error);
    } else {
      console.log('Notification sent to Kafka', data);
    }
  });
};

module.exports = { sendNotification };
