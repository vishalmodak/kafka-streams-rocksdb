package com.kstreams.purchase;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.kstreams.purchase.model.Purchase;
import com.kstreams.purchase.model.PurchasePattern;
import com.kstreams.purchase.model.RewardAccumulator;
import com.kstreams.serializer.JsonDeserializer;
import com.kstreams.serializer.JsonSerializer;


public class PurchaseKStreamDriver {

    public void main(String[] args) {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        
        JsonSerializer<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializer<>(RewardAccumulator.class);

        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorJsonSerializer, rewardAccumulatorJsonDeserializer);
        
        JsonSerializer<PurchasePattern> purchasePatternJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializer<>(PurchasePattern.class);

        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternJsonSerializer,purchasePatternJsonDeserializer);
        
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer,purchaseJsonDeserializer);

        Serde<String> stringSerde = Serdes.String();
        
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        
        KStream<String, Purchase> purchaseKStream = kStreamBuilder.stream(stringSerde, purchaseSerde, "src-topic")
                                                                    .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build()).to(stringSerde,purchasePatternSerde,"patterns");
        purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build()).to(stringSerde,rewardAccumulatorSerde,"rewards");
        
        purchaseKStream.to(stringSerde, purchaseSerde, "purchases");
        
        System.out.println("Starting PurchaseStreams Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamsConfig);
        kafkaStreams.start();
        System.out.println("Now started PurchaseStreams Example");
    }
    
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Kafka-Streams-Job");
        props.put("group.id", "streams-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-streams-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
