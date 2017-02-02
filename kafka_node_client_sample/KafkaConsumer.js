var kafka = require('kafka-node')
var Consumer = kafka.Consumer
var client = new kafka.Client("ubuntu:2181/")

var consumer = new Consumer(
  client,
  [],
  {fromOffset: true}
);

consumer.on('message', function (message) {
  console.log("received message", message);
});

consumer.addTopics([
  { topic: 'test', partition: 0, offset: 0}
], () => console.log("topic added"));