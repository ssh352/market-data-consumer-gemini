package com.redhat.trm.marketdata.fix;

import quickfix.SessionID;

import java.util.concurrent.atomic.AtomicReference;

public class SessionIDReference {

    private static AtomicReference<SessionID> sessionIDAtomicReference = new AtomicReference<>();

    public static SessionID getCurrent() {
        return sessionIDAtomicReference.get();
    }

    public static void setCurrent(SessionID sessionID) {
        sessionIDAtomicReference.set(sessionID);
    }

}
