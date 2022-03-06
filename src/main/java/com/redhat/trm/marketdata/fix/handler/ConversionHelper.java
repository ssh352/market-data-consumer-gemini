package com.redhat.trm.marketdata.fix.handler;

import com.redhat.trm.shared.proto.marketdata.MarketDataEntryType;
import quickfix.field.MDEntryType;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ConversionHelper {

    public MarketDataEntryType convertMDEntryType(MDEntryType mdEntryType) {
        switch (mdEntryType.getValue()) {
            case MDEntryType.BID:
                return MarketDataEntryType.BID;
            case MDEntryType.OFFER:
                return MarketDataEntryType.OFFER;
            case MDEntryType.TRADE:
                return MarketDataEntryType.TRADE;
            default:
                throw new IllegalStateException("Unexpected value: " + mdEntryType);
        }
    }

}