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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

class SinkLatencyMetricsTest {

    private PluginMetrics pluginMetrics;
    private EventHandle eventHandle;
    private SinkLatencyMetrics latencyMetrics;
    private Timer internalLatencyTimer;
    private Timer externalLatencyTimer;
    private Counter futureExternalOriginationTimeCounter;
    private Counter negativeLatencyCounter;

    public SinkLatencyMetrics createObjectUnderTest() {
        return new SinkLatencyMetrics(pluginMetrics);
    }

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        internalLatencyTimer = Timer
              .builder("internalLatency")
              .register(registry);
        externalLatencyTimer = Timer
              .builder("externalLatency")
              .register(registry);
        futureExternalOriginationTimeCounter = Counter.builder("futureExternalOriginationTime").register(registry);
        negativeLatencyCounter = Counter.builder("negativeLatency").register(registry);
        when(pluginMetrics.timer(SinkLatencyMetrics.INTERNAL_LATENCY)).thenReturn(internalLatencyTimer);
        when(pluginMetrics.timer(SinkLatencyMetrics.EXTERNAL_LATENCY)).thenReturn(externalLatencyTimer);
        when(pluginMetrics.counter(SinkLatencyMetrics.FUTURE_EXTERNAL_ORIGINATION_TIME)).thenReturn(futureExternalOriginationTimeCounter);
        when(pluginMetrics.counter(SinkLatencyMetrics.NEGATIVE_LATENCY)).thenReturn(negativeLatencyCounter);
        eventHandle = mock(EventHandle.class);
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics = createObjectUnderTest();
    }

    @Test
    public void testInternalOriginationTime() {
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencyTimer.count(), equalTo(1L));
    }

    @Test
    public void testExternalOriginationTime() {
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now().minusMillis(10));
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.max(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(10.0));
        assertThat(futureExternalOriginationTimeCounter.count(), equalTo(0.0));
        assertThat(negativeLatencyCounter.count(), equalTo(0.0));
    }

    @Test
    public void testExternalTimeAfterIngestionUsesInternalTimeForLatency() {
        Instant internalTime = Instant.now().minusSeconds(5);
        when(eventHandle.getInternalOriginationTime()).thenReturn(internalTime);
        when(eventHandle.getExternalOriginationTime()).thenReturn(internalTime.plusSeconds(3));

        latencyMetrics.update(eventHandle);

        assertThat(externalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.max(TimeUnit.SECONDS), greaterThanOrEqualTo(5.0));
        assertThat(futureExternalOriginationTimeCounter.count(), equalTo(1.0));
        assertThat(negativeLatencyCounter.count(), equalTo(0.0));
    }

    @Test
    public void testFutureExternalTimeCannotMakeLatencyNegative() {
        Instant internalTime = Instant.now().minusSeconds(5);
        when(eventHandle.getInternalOriginationTime()).thenReturn(internalTime);
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now().plusSeconds(60));

        latencyMetrics.update(eventHandle);

        assertThat(externalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.max(TimeUnit.SECONDS), greaterThanOrEqualTo(5.0));
        assertThat(futureExternalOriginationTimeCounter.count(), equalTo(1.0));
        assertThat(negativeLatencyCounter.count(), equalTo(0.0));
    }

    @Test
    public void testNegativeDurationsAreCountedAndNotRecorded() {
        Instant futureTime = Instant.now().plusSeconds(60);
        when(eventHandle.getInternalOriginationTime()).thenReturn(futureTime);
        when(eventHandle.getExternalOriginationTime()).thenReturn(futureTime.plusSeconds(1));

        latencyMetrics.update(eventHandle);

        assertThat(internalLatencyTimer.count(), equalTo(0L));
        assertThat(externalLatencyTimer.count(), equalTo(0L));
        assertThat(futureExternalOriginationTimeCounter.count(), equalTo(1.0));
        assertThat(negativeLatencyCounter.count(), equalTo(2.0));
    }
}

