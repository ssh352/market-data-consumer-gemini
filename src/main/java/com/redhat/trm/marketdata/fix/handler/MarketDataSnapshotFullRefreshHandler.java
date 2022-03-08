package com.redhat.trm.marketdata.fix.handler;

import com.redhat.trm.shared.proto.CacheConstants;
import com.redhat.trm.shared.proto.Exchange;
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
import quickfix.fix44.MarketDataSnapshotFullRefresh;

import javax.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;

@ApplicationScoped
public class MarketDataSnapshotFullRefreshHandler implements MessageHandler<MarketDataSnapshotFullRefresh> {

    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotFullRefreshHandler.class);

    private final RemoteCache<MarketDepthKey, MarketDepth> marketDepthRemoteCache;
    private final ConversionHelper conversionHelper;

    public MarketDataSnapshotFullRefreshHandler(
            @Remote(CacheConstants.CACHE_NAME_MARKET_DEPTH) RemoteCache<MarketDepthKey, MarketDepth> marketDepthRemoteCache,
            ConversionHelper conversionHelper) {
        this.marketDepthRemoteCache = marketDepthRemoteCache;
        this.conversionHelper = conversionHelper;
    }

    @Override
    @Timed
    public void handle(MarketDataSnapshotFullRefresh marketDataSnapshotFullRefresh, SessionID sessionID) {
        try {
            MarketDepthKey marketDepthKey = new MarketDepthKey(Exchange.GEMINI, marketDataSnapshotFullRefresh.getSymbol().getValue());
            log.info("{} - {}", marketDepthKey, marketDataSnapshotFullRefresh);
            MarketDepth marketDepth = new MarketDepth(marketDepthKey.getExchange(), marketDepthKey.getSymbol());
            MarketDataSnapshotFullRefresh.NoMDEntries noMDEntries = new MarketDataSnapshotFullRefresh.NoMDEntries();
            for (int i = 1; i <= marketDataSnapshotFullRefresh.getNoMDEntries().getValue(); i++) {
                marketDataSnapshotFullRefresh.getGroup(i, noMDEntries);
                MarketDepthEntry marketDepthEntry = new MarketDepthEntry(BigDecimal.valueOf(noMDEntries.getMDEntryPx().getValue()), BigDecimal.valueOf(noMDEntries.getMDEntrySize().getValue()));
                marketDepth.add(conversionHelper.convertMDEntryType(noMDEntries.getMDEntryType()), marketDepthEntry);
            }
            this.marketDepthRemoteCache.put(marketDepthKey, marketDepth);
        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }
    }

}
