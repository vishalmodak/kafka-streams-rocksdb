package com.kstreams.purchase.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.kstreams.purchase.model.Purchase;
import com.kstreams.purchase.model.RewardAccumulator;

public class CustomerRewards extends AbstractProcessor<String, Purchase>{

    @Override
    public void process(String key, Purchase value) {
        RewardAccumulator accumulator = RewardAccumulator.builder(value).build();
        context().forward(key, accumulator);
        context().commit();
    }

}
