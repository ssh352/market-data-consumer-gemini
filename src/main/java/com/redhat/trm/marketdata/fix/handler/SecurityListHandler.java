package com.redhat.trm.marketdata.fix.handler;

import com.redhat.trm.marketdata.Constants;
import com.redhat.trm.shared.proto.CacheConstants;
import com.redhat.trm.shared.proto.Exchange;
import com.redhat.trm.shared.proto.instrument.Instrument;
import com.redhat.trm.shared.proto.instrument.InstrumentKey;
import io.micrometer.core.annotation.Timed;
import io.quarkus.infinispan.client.Remote;
import io.vertx.core.eventbus.EventBus;
import org.infinispan.client.hotrod.RemoteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.FieldNotFound;
import quickfix.SessionID;
import quickfix.field.SecurityListRequestType;
import quickfix.field.SecurityReqID;
import quickfix.fix44.SecurityList;
import quickfix.fix44.SecurityListRequest;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class SecurityListHandler implements MessageHandler<SecurityList> {
    
    private static final Logger log = LoggerFactory.getLogger(SecurityListHandler.class);

    private final EventBus eventBus;
    private final RemoteCache<InstrumentKey, Instrument> instrumentRemoteCache;

    public SecurityListHandler(EventBus eventBus, @Remote(CacheConstants.CACHE_NAME_INSTRUMENT) RemoteCache<InstrumentKey, Instrument> instrumentRemoteCache) {
        this.eventBus = eventBus;
        this.instrumentRemoteCache = instrumentRemoteCache;
    }

    public void requestSecurityList(SessionID sessionID) {
        SecurityListRequest securityListRequest = new SecurityListRequest(
                new SecurityReqID(UUID.randomUUID().toString()),
                new SecurityListRequestType(SecurityListRequestType.SYMBOL)
        );
        log.debug("{} - {}", sessionID, securityListRequest);
        this.sendToTarget(securityListRequest, sessionID);
    }

    @Override
    @Timed
    public void handle(SecurityList securityList, SessionID sessionID) {
        log.debug("{} - {}", sessionID, securityList);
        try {
            SecurityList.NoRelatedSym noRelatedSym = new SecurityList.NoRelatedSym();
            for (int i = 1; i < securityList.getNoRelatedSym().getValue(); i++) {
                securityList.getGroup(i, noRelatedSym);
                String symbol = noRelatedSym.getSymbol().getValue();
                InstrumentKey instrumentKey = new InstrumentKey(Exchange.GEMINI, symbol);
                Instrument instrument = new Instrument(Exchange.GEMINI, symbol, Instant.now());
                log.debug("CachePut: {} - {}", instrumentKey, instrument);
                this.instrumentRemoteCache.put(instrumentKey, instrument);
                this.eventBus.publish(Constants.EVENT_BUS_MARKET_DATA_REQUEST, symbol);
            }
        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }
    }

}
