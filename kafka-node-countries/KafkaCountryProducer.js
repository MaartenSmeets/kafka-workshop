/*

This program reads and parses all lines from csv files countries2.csv into an array (countriesArray) of arrays; each nested array represents a country.
The initial file read is synchronous. The country records are kept in memory.
After the the initial read is performed, a function is invoked to publish a message to Kafka for the first country in the array. This function then uses a time out with a random delay 
to schedule itself to process the next country record in the same way. Depending on how the delays pan out, this program will publish country messages to Kafka every 3 seconds for about 10 minutes.

*/

var fs = require('fs');
var parse = require('csv-parse');

// Kafka configuration
var kafka = require('kafka-node')
var Producer = kafka.Producer
var client = new kafka.Client("ubuntu:2181/")

var countriesTopic = "countries";

    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client),
    km = new KeyedMessage('key', 'message'),
    countryProducerReady = false ;


producer.on('ready', function () {
    console.log("Producer for countries is ready");
    countryProducerReady = true;
});
 
producer.on('error', function (err) {
  console.error("Problem with producing Kafka message "+err);

})


var inputFile='countries2.csv';
var averageDelay = 3000;  // in miliseconds
var spreadInDelay = 2000; // in miliseconds

console.log("Processing Countries file");

var countriesArray ;
//var currentCountry=0;

var parser = parse({delimiter: ';'}, function (err, data) {
    countriesArray = data;
    // when all countries are available,then process the first one
    // note: array element at index 0 contains the row of headers that we should skip
    handleCountry(1);
});

// read the inputFile, feed the contents to the parser
fs.createReadStream(inputFile).pipe(parser);

// handle the current coountry record
function handleCountry( currentCountry) {   
    var line = countriesArray[currentCountry];
    var country = { "name" : line[0]
                  , "code" : line[1]
                  , "continent" : line[2]
                  , "population" : line[4]
                  , "size" : line[5]
                  };
     console.log(JSON.stringify(country));
     // produce country message to Kafka
     produceCountryMessage(country)
     // schedule this function to process next country after a random delay of between 1 and 5 seconds (3 +- 2 , approx 10 min worth of events)
     var delay = averageDelay + (Math.random() -0.5) * spreadInDelay;
     //note: use bind to pass in the value for the input parameter currentCountry     
     setTimeout(handleCountry.bind(null, currentCountry+1), delay);             
}//handleCountry


function produceCountryMessage(country) {

    payloads = [
        { topic: countriesTopic, messages: JSON.stringify(country), partition: 0 },
    ];
    if (countryProducerReady) {
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
    } else {
        console.error("sorry, CountryProducer is not ready yet, failed to produce message to Kafka.");
    }


}//produceCountryMessage

