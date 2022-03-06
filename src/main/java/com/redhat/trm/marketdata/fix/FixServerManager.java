package com.redhat.trm.marketdata.fix;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Objects;

@ApplicationScoped
public class FixServerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixServerManager.class);

    private final String settingsFilePath;
    private final Application application;

    private Initiator initiator;

    public FixServerManager(@ConfigProperty(name = "qfj.settings") String settingsFilePath, Application application) {
        this.settingsFilePath = settingsFilePath;
        this.application = application;
    }

    public void onStartupEvent(@Observes StartupEvent event) {
        LOGGER.info("The application is starting...");
        SessionSettings sessionSettings = this.createSessionSettings();
        MessageStoreFactory messageStoreFactory = new MemoryStoreFactory();
        LogFactory logFactory = new SLF4JLogFactory(sessionSettings);
        MessageFactory messageFactory = new quickfix.fix44.MessageFactory();
        try {
            this.initiator = new SocketInitiator(application, messageStoreFactory, sessionSettings, logFactory, messageFactory);
            initiator.start();
        } catch (ConfigError e) {
            //TODO Handle error
            throw new RuntimeException(e);
        }
    }

    void onShutdownEvent(@Observes ShutdownEvent event) {
        LOGGER.info("The application is stopping...");
        if (Objects.nonNull(initiator)) {
            this.initiator.stop();
        }
    }

    private SessionSettings createSessionSettings() {
        try {
            InputStream input = new FileInputStream(settingsFilePath);
            SessionSettings settings = new SessionSettings(input);
            return settings;
        } catch (FileNotFoundException | ConfigError e) {
            //TODO Handle error
            throw new RuntimeException(e);
        }
    }

}