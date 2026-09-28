/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Duration;
import java.time.Instant;

public class SinkLatencyMetrics {
    public static final String INTERNAL_LATENCY = "PipelineLatency";
    public static final String EXTERNAL_LATENCY = "EndToEndLatency";
    public static final String FUTURE_EXTERNAL_ORIGINATION_TIME = "FutureExternalOriginationTime";
    public static final String NEGATIVE_LATENCY = "NegativeLatency";
    private final Timer internalLatencyTimer;
    private final Timer externalLatencyTimer;
    private final Counter futureExternalOriginationTimeCounter;
    private final Counter negativeLatencyCounter;

    public SinkLatencyMetrics(PluginMetrics pluginMetrics) {
        internalLatencyTimer = pluginMetrics.timer(INTERNAL_LATENCY);
        externalLatencyTimer = pluginMetrics.timer(EXTERNAL_LATENCY);
        futureExternalOriginationTimeCounter = pluginMetrics.counter(FUTURE_EXTERNAL_ORIGINATION_TIME);
        negativeLatencyCounter = pluginMetrics.counter(NEGATIVE_LATENCY);
    }
    public void update(final EventHandle eventHandle) {
        Instant now = Instant.now();
        Instant internalOriginationTime = eventHandle.getInternalOriginationTime();
        Duration internalLatency = Duration.between(internalOriginationTime, now);
        if (internalLatency.isNegative()) {
            negativeLatencyCounter.increment();
        } else {
            internalLatencyTimer.record(internalLatency);
        }
        Instant externalOriginationTime = eventHandle.getExternalOriginationTime();
        if (externalOriginationTime == null) {
            return;
        }
        if (externalOriginationTime.isAfter(internalOriginationTime)) {
            futureExternalOriginationTimeCounter.increment();
            // A future source timestamp cannot measure time spent in the pipeline.
            externalOriginationTime = internalOriginationTime;
        }
        Duration externalLatency = Duration.between(externalOriginationTime, now);
        if (externalLatency.isNegative()) {
            negativeLatencyCounter.increment();
        } else {
            externalLatencyTimer.record(externalLatency);
        }
    }
}
