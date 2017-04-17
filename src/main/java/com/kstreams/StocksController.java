package com.kstreams;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.kstreams.stocks.model.StockTransactionSummary;

@RestController
public class StocksController {
    
    private static final Logger LOG = LoggerFactory.getLogger(StocksController.class);
    
    @Autowired
    private KafkaStreams kafkaStreams;
    
    @RequestMapping(value = "/lookup/stock/{code}", method = RequestMethod.GET)
    public StockTransactionSummary lookup(@PathVariable String code) throws Exception {
        final ReadOnlyKeyValueStore<String, StockTransactionSummary> stockTxnSummary =
                kafkaStreams.store("stock-transaction", 
                                    QueryableStoreTypes.<String, StockTransactionSummary>keyValueStore());
        return stockTxnSummary.get(code);
    }

    @RequestMapping(value = "/list/stocks", method = RequestMethod.GET)
    public List<String> list() throws Exception {
        final ReadOnlyKeyValueStore<String, StockTransactionSummary> stockTxnSummary =
                kafkaStreams.store("stock-transaction", 
                                    QueryableStoreTypes.<String, StockTransactionSummary>keyValueStore());
        LOG.info(kafkaStreams.allMetadataForStore("stock-transaction").toString());
        List<String> stocks = new ArrayList<>();
        KeyValueIterator<String, StockTransactionSummary> iter = stockTxnSummary.all();
        for (int i=0; iter.hasNext() && i<10; i++) {
            KeyValue<String, StockTransactionSummary> txnSummary = iter.next();
            StreamsMetadata metadata = kafkaStreams.metadataForKey("stock-transaction", txnSummary.key, new StringSerializer());
            stocks.add(txnSummary.key + "[" + txnSummary.value + "]" + "[" + metadata.hostInfo().toString() + "] ");
        }
        return stocks;
    }
}
