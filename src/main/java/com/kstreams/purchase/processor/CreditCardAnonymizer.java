package com.kstreams.purchase.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.kstreams.purchase.model.Purchase;

public class CreditCardAnonymizer extends AbstractProcessor<String, Purchase>{
    
    private static final String CC_NUMBER_REPLACEMENT="xxxx-xxxx-xxxx-";

    @Override
    public void process(String key, Purchase purchase) {
        String last4Digits = purchase.getCreditCardNumber().split("-")[3];
        Purchase updated = Purchase.builder(purchase).creditCardNumber(CC_NUMBER_REPLACEMENT+last4Digits).build();
        context().forward(key, updated);
        context().commit();
    }
    

}
