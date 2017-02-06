package nl.amis.streams.countries;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

import java.util.HashMap;
import java.util.Map;
        import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.StreamsConfig;

public class App 
{

static public class CountryMessage {
public String code;
public String name;
public int population;
public int area;
public int size;
public String continent;
}

    public static void main( String[] args )
    {
        System.out.println( "Hello World and doeiaasd!" );
        


Properties settings = new Properties();
// Set a few key parameters
settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "countries-streamin-analysis-app-2");
settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "ubuntu:2181");
settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
settings.put(StreamsConfig.STATE_DIR_CONFIG , "C:\\data\\Kafka-9feb2017\\kafka-workshop\\kafka-streams-countries\\tmp");
// to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
 // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
// see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);



// Create an instance of StreamsConfig from the Properties instance
StreamsConfig config = new StreamsConfig(settings);
final Serde<String> stringSerde = Serdes.String();

//CountryDeserializer countryJsonDeserializer = new CountryDeserializer();
//Serde<Country> countrySerde = Serdes.serdeFrom(StringSerializer,purchaseJsonDeserializer);

  //      Serde<String> stringSerde = Serdes.String();
 Map<String, Object> serdeProps = new HashMap<>();
final Serializer<CountryMessage> countryMessageSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageSerializer.configure(serdeProps, false);

        final Deserializer<CountryMessage> countryMessageDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageDeserializer.configure(serdeProps, false);
        final Serde<CountryMessage> countryMessageSerde = Serdes.serdeFrom(countryMessageSerializer, countryMessageDeserializer );


// building Kafka Streams Model

KStreamBuilder kStreamBuilder = new KStreamBuilder();
// the source of all streaming analysis is the topic with country messages
KStream<String, CountryMessage> countriesStream = kStreamBuilder.stream(stringSerde, countryMessageSerde, "countries");

//countriesStream.selectKey((k, country) -> country.continent).groupByKey(Serdes.String(), countryMessageSerde ).count().toStream().print();


// running count of countries per continent
KTable<String,Long> runningCountriesCountPerContinent = countriesStream.selectKey((k, country) -> country.continent).countByKey("Counts");
runningCountriesCountPerContinent.print();

// running sum of population sizes per continent
KTable<String,HashMap<String,Integer>> runningPopulationSumPerContinent = countriesStream
                      .selectKey((k, country) -> country.continent)
                      .aggregateByKey( 
                          HashMap<String,Integer>::new
/*                          new Initializer<HashMap<String,Integer>>() {
                                  @Override
                               public HashMap<String,Integer> apply() {
                                  return new HashMap<String,Integer>();
                               }
                          }
  */                      , (continent, countryMsg, aggregate) -> {
                                   int currentPopulation = aggregate.containsKey(continent)? aggregate.get(continent):0;
                            aggregate.put(continent, currentPopulation + countryMsg.population );
                            return aggregate;
                        }
/*                          , new Aggregator<String, CountryMessage, HashMap<String,Long>>() {
                               @Override
                               public HashMap<String,Long> apply(String continent, CountryMessage countryMsg, HashMap<String,Long> aggregate) {
                                   System.out.println("In Aggregator");
                                  return aggregate;
                               }
                          }                          
  */
                        ,  "PopulationSums");
//runningPopulationSumPerContinent.print();


// running sum of population sizes per continent
// selectKey assigns the key to group by to each record, in this case continent
// the aggregation itself is a simple summation of populations (per continent)
KTable<String,Integer> runningPopulationSumPerContinent2 = countriesStream
                      .selectKey((k, country) -> country.continent)
                      .aggregateByKey( 
                          () -> { return 0;}
                        , (continent, countryMsg, aggregate) -> {
                            return aggregate + countryMsg.population;
                        }
                        ,  "PopulationSums2");
runningPopulationSumPerContinent2.print();

/*
KTable<Integer, Integer> sumOfCountryPopulations =countriesStream 
        // We are only interested in countries outside of Europe.
        .filter((k, country) -> ! "Europe".equalsIgnoreCase( country.continent) )
        // We want to compute the total sum across ALL numbers, so we must re-key all records to the
        // same key.  This re-keying is required because in Kafka Streams a data record is always a
        // key-value pair, and KStream aggregations such as `reduceByKey` operate on a per-key basis.
        // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
        // all records.
        .selectKey((k, country) -> 1)
        // Add the numbers to compute the sum.
        .reduceByKey((v1, v2) -> v1.population + v2.population, "sum")
        ;


 sumOfCountryPopulations.print(Serdes.Integer(), Serdes.Integer());
//countriesStream.countByKey("Counts").to(Serdes.String(), Serdes.Long(), "CountryCount");

//        KStream<String,Country> purchaseKStream = kStreamBuilder.stream(stringSerde,purchaseSerde,"src-topic")
  //              .mapValues(p -> Purcha/se.builder(p).maskCreditCard().build());

*/
System.out.println("Created config");
System.out.println("Starting Streams Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,config);
        kafkaStreams.start();
System.out.println("Now started CountriesStreams Example");
    }
}
