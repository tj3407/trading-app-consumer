require("dotenv").config();
const express = require("express");
const app = express();
const http = require("http").Server(app);
const bodyParser = require("body-parser");
const Kafka = require("node-rdkafka");
const WebSocket = require('ws');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function(req, res, next) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});
const PORT = process.env.PORT || 5000;

const wss = new WebSocket.Server({ port: 3030 });

app.get("/", function(req, res) {
  res.send("Service is running")
})

var server = app.listen(PORT, () =>
    console.log("Express server is running on localhost:" + PORT)
);

const io = require("socket.io").listen(server);

var kafkaConf = {
  "group.id": "cloudkarafka-clicks",
  "metadata.broker.list": process.env.CLOUDKARAFKA_BROKERS,
  "socket.keepalive.enable": true,
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "SCRAM-SHA-256",
  "sasl.username": process.env.CLOUDKARAFKA_USERNAME,
  "sasl.password": process.env.CLOUDKARAFKA_PASSWORD,
  "debug": "generic,broker,security"
};

const prefix = process.env.CLOUDKARAFKA_USERNAME;
const topics = [`${prefix}-clicks`];
const consumer = new Kafka.KafkaConsumer(kafkaConf, {
  "auto.offset.reset": "beginning"
});
var messages = [];

consumer.on("error", function(err) {
  console.error(err);
});
consumer.on("ready", function(arg) {
  console.log(`Consumer ${arg.name} ready`);
  consumer.subscribe(topics);
  consumer.consume();
});
consumer.on("data", function(m) {
  const message = JSON.parse(m.value.toString());
  console.log(message);
  messages.push(message);
  io.sockets.emit('message', messages);
  consumer.commit(m);
});
consumer.on("disconnected", function(arg) {
  process.exit();
});
consumer.on('event.error', function(err) {
  console.error(err);
  process.exit(1);
});
consumer.on('event.log', function(log) {
  console.log(log);
});
consumer.connect();

