package com.redhat.trm.marketdata.fix.handler;

import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;

public interface MessageHandler<M extends Message> {

    void handle(M message, SessionID sessionID);

    default boolean sendToTarget(Message message, SessionID sessionID) {
        try {
            return Session.sendToTarget(message, sessionID);
        } catch (SessionNotFound e) {
            throw new RuntimeException(e);
        }
    }

}
