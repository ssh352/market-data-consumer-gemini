package com.redhat.trm.marketdata.configuration;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

@Singleton
public class MicrometerConfiguration {

    @Produces
    @Singleton
    public MeterFilter enableHistogram() {
        return new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                if (id.getName().startsWith("method.timed")) {
                    return DistributionStatisticConfig.builder()
                            .percentiles(0.5, 0.95, 0.99, 0.999)
                            .build()
                            .merge(config);
                }
                return config;
            }
        };
    }

}
