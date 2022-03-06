package com.redhat.trm.marketdata.fix;

import com.redhat.trm.marketdata.fix.handler.MarketDataIncrementalRefreshHandler;
import com.redhat.trm.marketdata.fix.handler.MarketDataRequestHandler;
import com.redhat.trm.marketdata.fix.handler.MarketDataSnapshotFullRefreshHandler;
import com.redhat.trm.marketdata.fix.handler.SecurityListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.*;
import quickfix.fix44.MessageCracker;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class QuickfixApplicationGemini extends MessageCracker implements Application {

    private static final Logger log = LoggerFactory.getLogger(QuickfixApplicationGemini.class);

    private final SecurityListHandler securityListHandler;
    private final MarketDataSnapshotFullRefreshHandler marketDataSnapshotFullRefreshHandler;
    private final MarketDataIncrementalRefreshHandler marketDataIncrementalRefreshHandler;
    private final MarketDataRequestHandler marketDataRequestHandler;

    public QuickfixApplicationGemini(
            SecurityListHandler securityListHandler,
            MarketDataSnapshotFullRefreshHandler marketDataSnapshotFullRefreshHandler,
            MarketDataIncrementalRefreshHandler marketDataIncrementalRefreshHandler,
            MarketDataRequestHandler marketDataRequestHandler
    ) {
        this.securityListHandler = securityListHandler;
        this.marketDataSnapshotFullRefreshHandler = marketDataSnapshotFullRefreshHandler;
        this.marketDataIncrementalRefreshHandler = marketDataIncrementalRefreshHandler;
        this.marketDataRequestHandler = marketDataRequestHandler;
    }

    @Override
    public void onCreate(SessionID sessionID) {
        log.debug("{}", sessionID);
    }

    @Override
    public void onLogon(SessionID sessionID) {
        log.debug("{}", sessionID);
        SessionIDReference.setCurrent(sessionID);
        this.securityListHandler.requestSecurityList(sessionID);
    }

    @Override
    public void onLogout(SessionID sessionID) {
        log.debug("{}", sessionID);
        SessionIDReference.setCurrent(null);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        log.debug("{} - {}", sessionID, message);
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        log.debug("{} - {}", sessionID, message);
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        log.debug("{} - {}", sessionID, message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        log.debug("{} - {}", sessionID, message);
        crack(message, sessionID);
    }

    //CRACKING
    public void onMessage(quickfix.fix44.SecurityList securityList, SessionID sessionID) throws FieldNotFound {
        log.debug("{} - {}", sessionID, securityList);
        this.securityListHandler.handle(securityList, sessionID);
    }

    public void onMessage(quickfix.fix44.MarketDataRequestReject marketDataRequestReject, SessionID sessionID) {
        log.debug("{} - {}", sessionID, marketDataRequestReject);
        this.marketDataRequestHandler.handle(marketDataRequestReject, sessionID);
    }

    public void onMessage(quickfix.fix44.MarketDataSnapshotFullRefresh marketDataSnapshotFullRefresh, SessionID sessionID) {
        log.debug("{} - {}", sessionID, marketDataSnapshotFullRefresh);
        this.marketDataSnapshotFullRefreshHandler.handle(marketDataSnapshotFullRefresh, sessionID);
    }

    public void onMessage(quickfix.fix44.MarketDataIncrementalRefresh marketDataIncrementalRefresh, SessionID sessionID) {
        log.debug("{} - {}", sessionID, marketDataIncrementalRefresh);
        this.marketDataIncrementalRefreshHandler.handle(marketDataIncrementalRefresh, sessionID);
    }

}
