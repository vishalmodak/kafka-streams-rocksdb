package com.kstreams.stocks.processor;

import java.util.Objects;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import com.kstreams.stocks.model.StockTransaction;
import com.kstreams.stocks.model.StockTransactionSummary;

public class StockProcessor extends AbstractProcessor<String, StockTransaction>{

    private KeyValueStore<String, StockTransactionSummary> summaryStore;
    private ProcessorContext context;
    
    @Override
    public void process(String key, StockTransaction stockTransaction) {
        String currentSymbol = stockTransaction.getSymbol();
        StockTransactionSummary txnSummary = summaryStore.get(currentSymbol);
        if (txnSummary == null) {
            txnSummary = StockTransactionSummary.fromTransaction(stockTransaction);
        } else {
            txnSummary.update(stockTransaction);
        }
        summaryStore.put(currentSymbol, txnSummary);
        this.context.commit();
    }
    
    public void init(ProcessorContext context) {
        this.context = context;
        //schedules a periodic call (punctuate) for this processor
        this.context.schedule(10000);
        summaryStore = (KeyValueStore<String, StockTransactionSummary>)this.context.getStateStore("stock-transaction");
        Objects.requireNonNull(summaryStore, "State store can't be null");
    }
    
    //The punctuate method iterates over all the values in the store and 
    //if they have been updated with the last 11 seconds, 
    //the StockTransactionSummary object is sent to consumers.
    public void punctuate(long streamline) {
        KeyValueIterator<String, StockTransactionSummary> it = summaryStore.all();
        long currentTime = System.currentTimeMillis();
        while(it.hasNext()) {
            StockTransactionSummary summary = it.next().value;
            if (summary.updatedWithinLastMillis(currentTime, 11000)) {
                this.context.forward(summary.tickerSymbol, summary);
            }
        }
    }
}
