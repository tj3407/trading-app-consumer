const express = require("express");
const app = express();
const http = require("http").Server(app);
const bodyParser = require("body-parser");
const { Kafka } = require('kafkajs');
// const WebSocket = require('ws');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function(req, res, next) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});
const PORT = process.env.PORT || 5000;

// const wss = new WebSocket.Server({ port: 3030 });

app.get("/", function(req, res) {
  res.send("Service is running")
})

var server = app.listen(PORT, () =>
    console.log("Express server is running on localhost:" + PORT)
);

const io = require("socket.io").listen(server);

const config = {
    kafka: {
      TOPIC: 'clicks',
      BROKERS: ['localhost:9092'],
      GROUPID: 'clicks-consumer-group',
      CLIENTID: 'sample-kafka-client'
    }
}

const kafka = new Kafka({
  clientId: config.kafka.CLIENTID,
  brokers: config.kafka.BROKERS
})

const topic = config.kafka.TOPIC
const consumer = kafka.consumer({
  groupId: config.kafka.GROUPID
})

var messages = [];

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const jsonObj = JSON.parse(message.value.toString())
        console.log(jsonObj)
        messages.push(jsonObj);
        io.sockets.emit('message', messages);
      } catch (error) {
        console.log('err=', error)
      }
    }
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

