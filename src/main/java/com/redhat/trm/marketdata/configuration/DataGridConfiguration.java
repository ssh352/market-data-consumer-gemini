package com.redhat.trm.marketdata.configuration;

import com.redhat.trm.shared.proto.*;
import com.redhat.trm.shared.proto.instrument.InstrumentSchema;
import com.redhat.trm.shared.proto.instrument.InstrumentSchemaImpl;
import io.quarkus.runtime.StartupEvent;
import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContext;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.List;

@ApplicationScoped
public class DataGridConfiguration {

    private final RemoteCacheManager remoteCacheManager;

    public DataGridConfiguration(RemoteCacheManager remoteCacheManager) {
        this.remoteCacheManager = remoteCacheManager;
    }

    void onStart(@Observes StartupEvent ev) {
        this.ensureCaches();
        this.registerSchemas();
    }

    private void ensureCaches() {
        RemoteCacheManagerAdmin remoteCacheManagerAdmin = this.remoteCacheManager.administration();
        remoteCacheManagerAdmin.getOrCreateCache(CacheConstants.CACHE_NAME_INSTRUMENT, DefaultTemplate.DIST_SYNC);
        remoteCacheManagerAdmin.getOrCreateCache(CacheConstants.CACHE_NAME_MARKET_DATA_SUBSCRIPTION, DefaultTemplate.DIST_SYNC);
        remoteCacheManagerAdmin.getOrCreateCache(CacheConstants.CACHE_NAME_MARKET_DEPTH, DefaultTemplate.DIST_SYNC);
    }

    private void registerSchemas() {
        SerializationContext serializationContext = MarshallerUtil.getSerializationContext(remoteCacheManager);
        List<GeneratedSchema> generatedSchemas = List.of(new CommonSchemaImpl(), new InstrumentSchemaImpl(), new MarketDataSchemaImpl());
        for (GeneratedSchema generatedSchema: generatedSchemas) {
            generatedSchema.registerSchema(serializationContext);
            generatedSchema.registerMarshallers(serializationContext);
        }
    }

}
