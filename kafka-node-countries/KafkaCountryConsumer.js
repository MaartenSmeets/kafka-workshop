/*

This program consumes Kafka messages from topic countries.
Each message contains a JSON message, such as:
{"name":"Aland Islands","code":"AX","continent":"Europe","population":"29013","size":"1580"}
with the name of a country, its continent, the population size and the area.

This program reports:
- running count of all countries
- top 3 population sizes
- population total per continent
- largest country in the world
- ...

these running aggregates can easily be determined using Kafka Streams. This program shows a naive, traditional approach.

*/


var kafka = require('kafka-node')
var Consumer = kafka.Consumer
var client = new kafka.Client("ubuntu:2181/")

var countriesTopic = "countries";

var consumer = new Consumer(
  client,
  [],
  {fromOffset: true}
);

consumer.on('message', function (message) {
  handleCountryMessage(message);
});

consumer.addTopics([
  { topic: countriesTopic, partition: 0, offset: 0}
], () => console.log("topic "+countriesTopic+" added to consumer for listening"));


/*
This program reports:
- running count of all countries
- top 3 population sizes
- population total per continent
- largest country in the world
- ...
*/


var countryCount = 0;
var largestCountry = {size : 0};
var populationPerContinent = {};
var top3Populations = [];
var oldTop3 = '';

function handleCountryMessage(countryMessage) {
    var country = JSON.parse(countryMessage.value);
    console.log("Welcome "+country.name);
    // update running aggregates

    // running count of all countries
    countryCount++;
    console.log("Country Count = "+ countryCount);
    // largest country in the world
    if (Number(country.size) > largestCountry.size) {
        largestCountry.name = country.name;
        largestCountry.size = country.size;
        console.log("New largest country: "+largestCountry.name+ " - size = "+ largestCountry.size);        
    }
    // population total per continent
    if (!(country.continent in populationPerContinent)) { populationPerContinent[country.continent] = 0 }
    populationPerContinent[country.continent] = populationPerContinent[country.continent] + 1* country.population;
    console.log(JSON.stringify(populationPerContinent));

    // top 3 population sizes
    top3Populations.push(country);
    // sort array by population
    top3Populations.sort(function (a,b) {return b.population - a.population});
    // retain top 3 (element zero is included, element 3 is not)
    top3Populations = top3Populations.slice(0,3);
    var newTop3=JSON.stringify(top3Populations);
    if (newTop3 != oldTop3) {
      console.log("New Top 3 populations: "+ newTop3);
      oldTop3 = newTop3;
    }   


}