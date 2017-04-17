package com.kstreams.purchase.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.kstreams.purchase.model.Purchase;
import com.kstreams.purchase.model.PurchasePattern;

public class PurchasePatterns extends AbstractProcessor<String, Purchase>{

    @Override
    public void process(String key, Purchase value) {
        PurchasePattern purchasePattern = PurchasePattern.newBuilder()
                                                            .date(value.getPurchaseDate())
                                                            .item(value.getItemPurchased())
                                                            .zipCode(value.getZipCode())
                                                            .build();
        context().forward(key, purchasePattern);
        context().commit();
    }

}
