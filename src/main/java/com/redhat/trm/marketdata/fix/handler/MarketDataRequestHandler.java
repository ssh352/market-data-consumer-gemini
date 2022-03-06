package com.redhat.trm.marketdata.fix.handler;

import com.redhat.trm.marketdata.Constants;
import com.redhat.trm.marketdata.fix.SessionIDReference;
import io.quarkus.vertx.ConsumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.SessionID;
import quickfix.field.*;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.MarketDataRequestReject;

import javax.enterprise.context.ApplicationScoped;
import java.util.UUID;

@ApplicationScoped
public class MarketDataRequestHandler implements MessageHandler<MarketDataRequestReject> {
    
    private static final Logger log = LoggerFactory.getLogger(MarketDataRequestHandler.class);

    @ConsumeEvent(Constants.EVENT_BUS_MARKET_DATA_REQUEST)
    public void consume(String symbol) {
        this.sendMarketDataRequest(symbol);
    }

    public void sendMarketDataRequest(String symbol) {
        log.debug("{}", symbol);
        MarketDataRequest marketDataRequest = new MarketDataRequest(
                new MDReqID(UUID.randomUUID().toString()),
                new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES),
                // This is not a char, it's an int
                new MarketDepth(0)
        );
        MarketDataRequest.NoRelatedSym noRelatedSym = new MarketDataRequest.NoRelatedSym();
        noRelatedSym.set(new Symbol(symbol));
        marketDataRequest.addGroup(noRelatedSym);
        MarketDataRequest.NoMDEntryTypes noMDEntryTypes = new MarketDataRequest.NoMDEntryTypes();
        noMDEntryTypes.set(new MDEntryType(MDEntryType.BID));
        marketDataRequest.addGroup(noMDEntryTypes);
        noMDEntryTypes.set(new MDEntryType(MDEntryType.OFFER));
        marketDataRequest.addGroup(noMDEntryTypes);
        this.sendToTarget(marketDataRequest, SessionIDReference.getCurrent());
    }

    @Override
    public void handle(MarketDataRequestReject message, SessionID sessionID) {
        log.error("{}", message);
    }

}
