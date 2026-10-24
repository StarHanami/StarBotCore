package com.starlwr.bot.core.live;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringWriter;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CompactLiveEventStoreTest {
    @ParameterizedTest
    @ValueSource(strings = {"heap", "memory-segment"})
    void oversizedEventUsesDedicatedCapacityAndStillReleasesAfterReset(String backend) {
        if (backend.equals("memory-segment")) assumeTrue(Runtime.version().feature() >= 22);
        try (CompactLiveEventStore store = store(backend)) {
            String content = "large😀" + "x".repeat(1_500_000);
            store.add("test", "1", "Danmu", event(content, 2L));
            long largeCapacity = store.usage().heapBytes() + store.usage().nativeBytes();
            assertEquals(store.usage().payloadBytes(), largeCapacity);
            store.add("test", "1", "Like", event("small", 3L));
            assertEquals(largeCapacity + (1 << 20), store.usage().heapBytes() + store.usage().nativeBytes());
            assertEquals(content, store.read("test", "1", "Danmu").get(0).getString("content"));
            store.deactivate("test", "1", "ended");
            assertEquals(0, store.usage().nativeBytes());
            assertEquals(store.usage().payloadBytes(), store.usage().heapBytes());
            store.reset("test", "1");
            assertEquals(0, store.usage().heapBytes() + store.usage().nativeBytes());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"heap", "memory-segment"})
    void repeatedResetReleasesSupersededSessionsAndCountsAllocatedCapacity(String backend) {
        if (backend.equals("memory-segment")) assumeTrue(Runtime.version().feature() >= 22);
        try (CompactLiveEventStore store = store(backend)) {
            for (int i = 0; i < 40; i++) {
                store.reset("test", "1");
                store.add("test", "1", "Danmu", event("event " + i, 2L));
                LiveEventStorageUsage usage = store.usage();
                assertEquals(1, usage.sessionCount());
                assertEquals(1, usage.eventCount());
                assertEquals(1 << 20, usage.heapBytes() + usage.nativeBytes());
                assertTrue(usage.payloadBytes() < usage.heapBytes() + usage.nativeBytes());
                assertTrue(usage.indexBytes() > 0);
                assertEquals(usage.estimatedBytes(), store.estimatedBytes());
            }
        }
    }

    @Test
    void reportReaderPinsOldNativeSessionAcrossResetAndStoreClose() throws Exception {
        assumeTrue(Runtime.version().feature() >= 22);
        CompactLiveEventStore store = store("memory-segment");
        store.add("test", "1", "Danmu", event("old", 2L));
        store.add("test", "1", "Danmu", event("old second", 2L));
        CountDownLatch reading = new CountDownLatch(1), resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> report = executor.submit(() -> store.forEach("test", "1", "Danmu", json -> {
                reading.countDown();
                try { assertTrue(resume.await(10, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new IllegalStateException(e); }
                assertTrue(json.getString("content").startsWith("old"), "stale read during seal must see pre-reset data");
            }));
            assertTrue(reading.await(5, TimeUnit.SECONDS));
            store.reset("test", "1");
            store.add("test", "1", "Danmu", event("new", 2L));
            assertEquals("new", store.read("test", "1", "Danmu").get(0).getString("content"));
            assertEquals(2, store.usage().sessionCount());
            assertEquals(2L << 20, store.usage().nativeBytes());
            store.close();
            assertEquals(1, store.usage().sessionCount());
            assertEquals(1 << 20, store.usage().nativeBytes());
            resume.countDown(); report.get(5, TimeUnit.SECONDS);
            assertEquals(LiveEventStorageUsage.empty(), store.usage());
        } finally {
            resume.countDown(); executor.shutdownNow(); store.close();
        }
    }

    @Test
    void deactivationReleasesNativeChunksWithoutZstdAndPreservesPayloads() {
        assumeTrue(Runtime.version().feature() >= 22);
        try (CompactLiveEventStore store = store("memory-segment")) {
            String content = "猫😀\n" + "x".repeat(700_000);
            for (int i = 0; i < 4; i++) store.add("test", "1", "Danmu", event(content + i, 2L));
            long payloadBytes = store.usage().payloadBytes();
            assertEquals(4L << 20, store.usage().nativeBytes());
            store.deactivate("test", "1", "removed");
            assertEquals(0, store.usage().nativeBytes());
            assertEquals(payloadBytes, store.usage().heapBytes());
            assertEquals(payloadBytes, store.usage().payloadBytes());
            assertEquals(4, store.usage().eventCount());
            for (int i = 0; i < 4; i++) assertEquals(content + i,
                    store.read("test", "1", "Danmu").get(i).getString("content"));
            store.reset("test", "1");
            assertEquals(0, store.usage().heapBytes());
            assertEquals(0, store.usage().eventCount());
        }
    }

    @Test
    void snapshotPinsOnlyExistingEventsAndMergesChangedSenderProfilesByUid() throws Exception {
        try (CompactLiveEventStore store = store("heap")) {
            store.add("test", "1", "Danmu", event("first", 2L));
            JSONObject changedSender = event("second", 2L);
            changedSender.getJSONObject("sender").put("uname", "renamed");
            store.add("test", "1", "Danmu", changedSender);
            try (CompactLiveEventStore.Snapshot snapshot = store.snapshot()) {
                store.add("test", "1", "Danmu", event("after snapshot", 2L));
                store.reset("test", "1");
                StringWriter writer = new StringWriter();
                snapshot.write(writer, new JSONObject());
                var events = JSONObject.parseObject(writer.toString()).getJSONObject("test")
                        .getJSONObject("Danmu").getJSONObject("1").getJSONArray("2");
                assertEquals(2, events.size());
                assertEquals("renamed", events.getJSONObject(1).getJSONObject("sender").getString("uname"));
            }
            assertEquals(0, store.usage().eventCount());
            assertEquals(0, store.usage().heapBytes());
            assertEquals(1, store.usage().sessionCount());
        }
    }

    @Test
    void optionalZstdArchivesAndDecodesEveryChunk() {
        boolean available;
        try { Class.forName("com.github.luben.zstd.Zstd"); available = true; }
        catch (ClassNotFoundException ignored) { available = false; }
        assumeTrue(available, "optional zstd-jni is not installed");
        StarBotCoreProperties.Live properties = new StarBotCoreProperties.Live();
        properties.setMemoryBackend("heap"); properties.setColdCompression("zstd");
        properties.setColdCompressionThresholdBytes(1);
        try (CompactLiveEventStore store = new CompactLiveEventStore(properties)) {
            String content = "重复😀" + "x".repeat(700_000);
            for (int i = 0; i < 3; i++) store.add("test", "1", "Danmu", event(content + i, 2L));
            store.deactivate("test", "1", "ended");
            assertTrue(store.usage().heapBytes() < store.usage().payloadBytes() / 10);
            var decoded = store.read("test", "1", "Danmu");
            for (int i = 0; i < 3; i++) assertEquals(content + i, decoded.get(i).getString("content"));
            store.reset("test", "1");
            assertEquals(0, store.usage().heapBytes());
        }
    }

    @Test
    void callbackFailureReleasesPinnedSessionAndClosedStoreRejectsFurtherAllocations() {
        CompactLiveEventStore store = store("heap");
        store.add("test", "1", "Danmu", event("old", 2L));
        assertThrows(IllegalStateException.class, () -> store.forEach("test", "1", "Danmu", json -> {
            store.reset("test", "1");
            throw new IllegalStateException("report failed");
        }));
        assertEquals(1, store.usage().sessionCount());
        assertEquals(0, store.usage().heapBytes());
        store.close();
        store.add("test", "1", "Danmu", event("ignored", 2L));
        store.reset("test", "1"); store.deactivate("test", "1", "ignored");
        assertEquals(LiveEventStorageUsage.empty(), store.usage());
    }

    private CompactLiveEventStore store(String backend) {
        StarBotCoreProperties.Live properties = new StarBotCoreProperties.Live();
        properties.setMemoryBackend(backend); properties.setColdCompression("none");
        return new CompactLiveEventStore(properties);
    }

    private JSONObject event(String content, long sender) {
        return new JSONObject().fluentPut("content", content).fluentPut("timestamp", 1700000000000L)
                .fluentPut("sender", new JSONObject().fluentPut("uid", sender).fluentPut("uname", "viewer"));
    }
}
