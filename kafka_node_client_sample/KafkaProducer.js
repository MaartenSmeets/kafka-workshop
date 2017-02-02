// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application

var kafka = require('kafka-node')
var Producer = kafka.Producer
var client = new kafka.Client("ubuntu:2181/")

    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    payloads = [
        { topic: 'test', messages: 'hi from Windows Host', partition: 0 },
        { topic: 'test', messages: 'hi from node producer', partition: 0 },
        { topic: 'test2', messages: ['hello', 'world', km] }
    ];
producer.on('ready', function () {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
});
 
producer.on('error', function (err) {})