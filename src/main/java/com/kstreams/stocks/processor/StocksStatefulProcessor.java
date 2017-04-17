package com.kstreams.stocks.processor;

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

import com.kstreams.serializer.JsonDeserializer;
import com.kstreams.serializer.JsonSerializer;
import com.kstreams.stocks.model.StockTransaction;
import com.kstreams.stocks.model.StockTransactionSummary;


public class StocksStatefulProcessor {
    
    public void main(String[] args) {
        StreamsConfig streamingConfig = new StreamsConfig(getProperties());

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
        System.out.println("Starting StockSummaryStatefulProcessor Example");
        KafkaStreams streaming = new KafkaStreams(builder, streamingConfig);
        streaming.start();
        System.out.println("StockSummaryStatefulProcessor Example now started");
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Sample-Stateful-Processor");
        props.put("group.id", "stocks-consumer-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful_processor_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
//        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
