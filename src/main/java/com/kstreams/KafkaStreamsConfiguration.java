package com.kstreams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kstreams.serializer.JsonDeserializer;
import com.kstreams.serializer.JsonSerializer;
import com.kstreams.stocks.model.StockTransaction;
import com.kstreams.stocks.model.StockTransactionSummary;
import com.kstreams.stocks.processor.StockProcessor;

@Configuration
public class KafkaStreamsConfiguration {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsConfiguration.class);
    
    @Autowired
    TomcatEmbeddedServletContainerFactory servletFactory;

    @Bean
    public StreamsConfig streamsConfig() {
        return new StreamsConfig(getProperties());
    }
    
    private Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Sample-Stateful-Processor");
        props.put("group.id", "stocks-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful_processor_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + servletFactory.getPort());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
    
    @Bean
    public TopologyBuilder topologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();

        JsonSerializer<StockTransactionSummary> stockTxnSummarySerializer = new JsonSerializer<>();
        JsonDeserializer<StockTransactionSummary> stockTxnSummaryDeserializer = new JsonDeserializer<>(StockTransactionSummary.class);
        JsonDeserializer<StockTransaction> stockTxnDeserializer = new JsonDeserializer<>(StockTransaction.class);
        JsonSerializer<StockTransaction> stockTxnJsonSerializer = new JsonSerializer<>();
        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();

        Serde<StockTransactionSummary> stockTxnSummarySerde = Serdes.serdeFrom(stockTxnSummarySerializer, stockTxnSummaryDeserializer);
        
        builder.addSource("stocks-source", stringDeserializer, stockTxnDeserializer, "stocks")
                .addProcessor("summary", StockProcessor::new, "stocks-source")
                .addStateStore(Stores.create("stock-transaction")
                                        .withStringKeys()
                                        .withValues(stockTxnSummarySerde)
//                                        .inMemory().maxEntries(100)
                                        .persistent()
                                        .build(), "summary")
                .addSink("sink", "stocks-out", stringSerializer,stockTxnJsonSerializer,"stocks-source")
                .addSink("sink-2", "transaction-summary", stringSerializer, stockTxnSummarySerializer, "summary")
        ;
        return builder;
    }
    
    @Bean
    public KafkaStreams kafkaStreams(StreamsConfig streamsConfig, TopologyBuilder topologyBuilder) {
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, streamsConfig);
        kafkaStreams.start();
        return kafkaStreams;
    }

}
