package nl.amis.streams.countries;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;


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

static public class CountryTop3 {

  public  CountryMessage[]  nrs = new CountryMessage[4] ;
  public CountryTop3() {}
}


public static void main( String[] args ) {
  System.out.println( "Kafka Streams Demonstration" );
  String APP_ID ="countries-streaming-analysis-app";        


Properties settings = new Properties();
// Set a few key parameters
settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
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
final Serde<Long> longSerde = Serdes.Long();
final Serde<Integer> integerSerde = Serdes.Integer();


// define countryMessageSerde
Map<String, Object> serdeProps = new HashMap<>();
final Serializer<CountryMessage> countryMessageSerializer = new JsonPOJOSerializer<>();
serdeProps.put("JsonPOJOClass", CountryMessage.class);
countryMessageSerializer.configure(serdeProps, false);

final Deserializer<CountryMessage> countryMessageDeserializer = new JsonPOJODeserializer<>();
serdeProps.put("JsonPOJOClass", CountryMessage.class);
countryMessageDeserializer.configure(serdeProps, false);
final Serde<CountryMessage> countryMessageSerde = Serdes.serdeFrom(countryMessageSerializer, countryMessageDeserializer );


// define countryTop3Serde
serdeProps = new HashMap<>();
final Serializer<CountryTop3> countryTop3Serializer = new JsonPOJOSerializer<>();
serdeProps.put("JsonPOJOClass", CountryTop3[].class);
countryTop3Serializer.configure(serdeProps, false);

final Deserializer<CountryTop3> countryTop3Deserializer = new JsonPOJODeserializer<>();
serdeProps.put("JsonPOJOClass", CountryTop3[].class);
countryTop3Deserializer.configure(serdeProps, false);
final Serde<CountryTop3> countryTop3Serde = Serdes.serdeFrom(countryTop3Serializer, countryTop3Deserializer );



// building Kafka Streams Model

KStreamBuilder kStreamBuilder = new KStreamBuilder();
// the source of all streaming analysis is the topic with country messages
KStream<String, CountryMessage> countriesStream = kStreamBuilder.stream(stringSerde, countryMessageSerde, "countries");

// running count of countries per continent, published in topic RunningCountryCountPerContinent
KTable<String,Long> runningCountriesCountPerContinent = countriesStream.selectKey((k, country) -> country.continent).countByKey("Counts");
runningCountriesCountPerContinent.toStream().to(stringSerde, longSerde,  "RunningCountryCountPerContinent");
runningCountriesCountPerContinent.print(stringSerde, longSerde);


// running sum of population sizes per continent, published in topic RunningPopulationSumPerContinent
// selectKey assigns the key to group by to each record, in this case continent
// the aggregation itself is a simple summation of populations (per continent)
KTable<String,Integer> runningPopulationSumPerContinent = countriesStream
                      .selectKey((k, country) -> country.continent)
                      .aggregateByKey( 
                          () -> { return 0;}
                        , (continent, countryMsg, aggregate) -> {
                            return aggregate + countryMsg.population;
                        }
                        , stringSerde, integerSerde
                        ,  "PopulationSumsPerContinent"
                        );
runningPopulationSumPerContinent.print(stringSerde,integerSerde);
runningPopulationSumPerContinent.toStream().to(stringSerde, integerSerde,  "RunningPopulationSumPerContinent");


// top 3 largest countries per continent, published top topic , published to topic Top3CountrySizePerContinent
KTable<String,CountryTop3> top3PerContinent = countriesStream
                      .selectKey((k, country) -> country.continent)
                      .aggregateByKey( 
                          CountryTop3::new
                      , (continent, countryMsg, top3) -> {
                                   top3.nrs[3]=countryMsg;
                                   //  sort the array by country size
                                   Arrays.sort(
                                       top3.nrs, (a, b) -> {
                                         if (a==null)  return 1;
                                         if (b==null)  return -1;
                                         return Integer.compare(b.size, a.size);
                                       }
                                   );
                                   // lose nr 4, only top 3 is relevant
                                   top3.nrs[3]=null;
                           return (top3);
                        }
                        ,  stringSerde, countryTop3Serde
                        ,  "Top3LargestCountriesPerContinent");

top3PerContinent.<String>mapValues((top3) -> {
    String rank = " 1. "+top3.nrs[0].name+" - "+top3.nrs[0].size                                   
             + ((top3.nrs[1]!=null)? ", 2. "+top3.nrs[1].name+" - "+top3.nrs[1].size:"")
             + ((top3.nrs[2]!=null) ? ", 3. "+top3.nrs[2].name+" - "+top3.nrs[2].size:"")
             ;                                 
    return "List for "+ top3.nrs[0].continent +rank;
  }  
  )
  .print(stringSerde,stringSerde);
top3PerContinent.to(stringSerde, countryTop3Serde,  "Top3CountrySizePerContinent");




System.out.println("Starting Kafka Streams Countries Example");
KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,config);
kafkaStreams.start();
System.out.println("Now started CountriesStreams Example");
}// main

}
