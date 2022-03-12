package com.redhat.trm.marketdata.fix.handler;

import com.redhat.trm.shared.proto.common.CacheConstants;
import com.redhat.trm.shared.proto.common.Exchange;
import com.redhat.trm.shared.proto.marketdata.MarketDepth;
import com.redhat.trm.shared.proto.marketdata.MarketDepthEntry;
import com.redhat.trm.shared.proto.marketdata.MarketDepthKey;
import io.micrometer.core.annotation.Timed;
import io.quarkus.infinispan.client.Remote;
import org.infinispan.client.hotrod.RemoteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.FieldNotFound;
import quickfix.SessionID;
import quickfix.field.MDEntryType;
import quickfix.field.MDUpdateAction;
import quickfix.fix44.MarketDataIncrementalRefresh;

import javax.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;

@ApplicationScoped
public class MarketDataIncrementalRefreshHandler implements MessageHandler<MarketDataIncrementalRefresh> {
    
    private static final Logger log = LoggerFactory.getLogger(MarketDataIncrementalRefreshHandler.class);

    private final RemoteCache<MarketDepthKey, MarketDepth> marketDepthRemoteCache;
    private final ConversionHelper conversionHelper;

    public MarketDataIncrementalRefreshHandler(
            @Remote(CacheConstants.CACHE_NAME_MARKET_DEPTH) RemoteCache<MarketDepthKey, MarketDepth> marketDepthRemoteCache,
            ConversionHelper conversionHelper) {
        this.marketDepthRemoteCache = marketDepthRemoteCache;
        this.conversionHelper = conversionHelper;
    }

    @Override
    @Timed
    public void handle(MarketDataIncrementalRefresh marketDataIncrementalRefresh, SessionID sessionID) {
        try {
            MarketDataIncrementalRefresh.NoMDEntries noMDEntries = new MarketDataIncrementalRefresh.NoMDEntries();
            for (int i = 1; i <= marketDataIncrementalRefresh.getNoMDEntries().getValue(); i++) {
                marketDataIncrementalRefresh.getGroup(i, noMDEntries);
                MarketDepthKey marketDepthKey = new MarketDepthKey(Exchange.GEMINI, noMDEntries.getSymbol().getValue());
                MDEntryType mdEntryType = noMDEntries.getMDEntryType();
                char mdUpdateAction = noMDEntries.getMDUpdateAction().getValue();
                log.debug("{} - {} - {}", "MarketDataIncrementalRefresh", noMDEntries.getMDUpdateAction(), marketDepthKey);
                MarketDepth marketDepth = this.marketDepthRemoteCache.get(marketDepthKey);
                switch (mdUpdateAction) {
                    case MDUpdateAction.NEW:
                    case MDUpdateAction.CHANGE:
                        marketDepth.add(this.conversionHelper.convertMDEntryType(mdEntryType), new MarketDepthEntry(BigDecimal.valueOf(noMDEntries.getMDEntryPx().getValue()), BigDecimal.valueOf(noMDEntries.getMDEntrySize().getValue())));
                        break;
                    case MDUpdateAction.DELETE:
                        marketDepth.delete(this.conversionHelper.convertMDEntryType(mdEntryType), BigDecimal.valueOf(noMDEntries.getMDEntryPx().getValue()));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + mdUpdateAction);
                }
                this.marketDepthRemoteCache.put(marketDepthKey, marketDepth);
            }
        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }
    }

}
