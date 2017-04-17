package com.kstreams.purchase.processor;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.kstreams.purchase.model.Purchase;
import com.kstreams.purchase.model.PurchasePattern;
import com.kstreams.purchase.model.RewardAccumulator;
import com.kstreams.serializer.JsonDeserializer;
import com.kstreams.serializer.JsonJacksonSerializer;
import com.kstreams.serializer.JsonSerializer;


public class PurchaseProcessorDriver {
    
    public void main(String[] args) {
        StreamsConfig streamingConfig = new StreamsConfig(getProperties());
        
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        JsonJacksonSerializer<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonJacksonSerializer<>();
        JsonSerializer<PurchasePattern> purchasePatternJsonSerializer = new JsonSerializer<>();
        
        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();
        
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("SOURCE", stringDeserializer, purchaseJsonDeserializer, "src-topic")
                        .addProcessor("PROCESS", CreditCardAnonymizer::new, "SOURCE")
                        .addProcessor("PROCESS2", CustomerRewards::new, "PROCESS")
                        .addProcessor("PROCESS3", PurchasePatterns::new, "PROCESS")
                        
                        .addSink("SINK", "purchases", stringSerializer, purchaseJsonSerializer, "PROCESS")
                        .addSink("SINK2", "rewards", stringSerializer, rewardAccumulatorJsonSerializer, "PROCESS2")
                        .addSink("SINK3", "patterns", stringSerializer, purchasePatternJsonSerializer, "PROCESS3")
        ;
        
        System.out.println("Starting PurchaseProcessor Example");
        KafkaStreams streaming = new KafkaStreams(topologyBuilder, streamingConfig);
        streaming.start();
        System.out.println("Now started PurchaseProcessor Example");
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Purchase-Processor-Job");
        props.put("group.id", "purchase-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-processor-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
