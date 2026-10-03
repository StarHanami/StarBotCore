package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.live.common.DanmuEvent;
import com.starlwr.bot.core.model.LiveStreamerInfo;
import com.starlwr.bot.core.model.UserInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LiveDataPersistenceReliabilityTest {
    @TempDir Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"heap", "memory-segment"})
    void truncatedFileDoesNotPublishPartiallyImportedEvents(String backend) throws Exception {
        Path path = tempDir.resolve("truncated-events.json");
        String broken = "{\"test\":{\"Danmu\":{\"1\":{\"2\":[{\"content\":\"partial\",\"timestamp\":10}]}}";
        Files.writeString(path, broken);
        StarBotCoreProperties properties = properties(path); properties.getLive().setMemoryBackend(backend);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        try {
            service.addDanmu(event("existing"));
            LiveEventStorageUsage before = service.getEventStorageUsage();
            service.onApplicationReadyEvent();
            assertEquals(before, service.getEventStorageUsage());
            assertEquals("existing", service.getEvents("test", 1L, "Danmu").get(0).getString("content"));
            assertThrows(IOException.class, () -> service.saveCache(path, false, false));
        } finally { service.onContextClosedEvent(); }
        assertEquals(broken, Files.readString(path));
        assertEquals(LiveEventStorageUsage.empty(), service.getEventStorageUsage());
    }

    @Test
    void reportPinsAllSeriesAndMetadataUntilItCloses() {
        StarBotCoreProperties properties = properties(tempDir.resolve("report.json"));
        properties.getLive().setSaveLiveData(false);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        try {
            service.setLiveStartTime("test", 1L, 100L);
            service.setCustomObject(7, "test", "BeforeFansCount", "1");
            service.addDanmu(event("old"));
            LiveDataReadSession report = service.openReadSession("test", 1L).orElseThrow();
            try (report) {
                service.resetLiveData("test", 1L);
                service.setLiveStartTime("test", 1L, 200L);
                service.deleteCustomObject("test", "BeforeFansCount", "1");
                service.addDanmu(event("new"));
                assertEquals(2, service.getEventStorageUsage().sessionCount());
                assertEquals(100L, report.getCustomObject(Long.class, "LiveStartTime", "1").orElseThrow());
                assertEquals(7, report.getCustomObject(Integer.class, "BeforeFansCount", "1").orElseThrow());
                java.util.List<JSONObject> events = new java.util.ArrayList<>();
                report.forEachEvent("Danmu", events::add);
                assertEquals("old", events.get(0).getString("content"));
                report.forEachEvent("Like", ignored -> fail("empty old series must stay empty"));
            }
            assertEquals(1, service.getEventStorageUsage().sessionCount());
            assertThrows(IllegalStateException.class, () -> report.forEachEvent("Danmu", ignored -> {}));
        } finally { service.onContextClosedEvent(); }
    }

    @Test
    void writesLegacyCompatibleDocumentLargerThanFastjsonWriterLimit() throws Exception {
        Path path = tempDir.resolve("large.json");
        StarBotCoreProperties properties = properties(path);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        try {
            String content = "x".repeat(450_000);
            for (int i = 0; i < 80; i++) service.addDanmu(event(content));
            assertTrue(service.saveCache(path, false, false));
            assertTrue(Files.size(path) > 64L * 1024 * 1024);
            try (var input = Files.newInputStream(path)) {
                var events = JSON.parseObject(input, StandardCharsets.UTF_8).getJSONObject("test")
                        .getJSONObject("Danmu").getJSONObject("1").getJSONArray("2");
                assertEquals(80, events.size());
                assertEquals(content, events.getJSONObject(79).getString("content"));
                assertEquals(2L, events.getJSONObject(79).getJSONObject("sender").getLongValue("uid"));
            }
        } finally { properties.getLive().setSaveLiveData(false); service.onContextClosedEvent(); }
    }

    @Test
    void failedWriteKeepsPreviousFileAndReleasesSnapshotAfterConcurrentReset() throws Exception {
        Path path = tempDir.resolve("atomic.json");
        Files.writeString(path, "{\"previous\":true}");
        StarBotCoreProperties properties = properties(path);
        DefaultLiveDataService service = new DefaultLiveDataService(properties) {
            @Override protected void writeSnapshot(Writer writer, JSONObject metadata, CompactLiveEventStore.Snapshot events) throws Exception {
                resetLiveData("test", 1L);
                writer.write("{\"partial\":");
                throw new IOException("simulated write failure");
            }
        };
        try {
            service.addDanmu(event("old"));
            assertThrows(IOException.class, () -> service.saveCache(path, false, false));
            assertEquals("{\"previous\":true}", Files.readString(path));
            assertEquals(0, service.getEventStorageUsage().heapBytes());
            assertEquals(0, service.getEventStorageUsage().nativeBytes());
            try (var files = Files.list(tempDir)) {
                assertFalse(files.anyMatch(p -> p.getFileName().toString().startsWith(".starbot-live-data-")));
            }
        } finally { properties.getLive().setSaveLiveData(false); service.onContextClosedEvent(); }
    }

    @Test
    void periodicSaveSurvivesErrorAndDoesNotCreateDuplicateSchedules() throws Exception {
        Path path = tempDir.resolve("retry.json");
        StarBotCoreProperties properties = properties(path);
        properties.getLive().setAutoSaveLiveDataInterval(1);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch saved = new CountDownLatch(1);
        DefaultLiveDataService service = new DefaultLiveDataService(properties) {
            @Override boolean saveCache(Path target, boolean closing, boolean empty) throws Exception {
                if (attempts.incrementAndGet() == 1) throw new OutOfMemoryError("simulated writer size limit");
                boolean result = super.saveCache(target, closing, empty); saved.countDown(); return result;
            }
        };
        try {
            service.addDanmu(event("saved after Error"));
            service.autoSave();
            Object task = ReflectionTestUtils.getField(service, "autoSaveTask");
            service.autoSave();
            assertSame(task, ReflectionTestUtils.getField(service, "autoSaveTask"));
            assertTrue(saved.await(10, TimeUnit.SECONDS));
            assertEquals("saved after Error", readEvent(path).getString("content"));
            assertFalse(((ScheduledFuture<?>) task).isDone());
        } finally { service.onContextClosedEvent(); }
    }

    @Test
    void closingSaveErrorStillReleasesStoreAndPreservesExistingFile() throws Exception {
        Path path = tempDir.resolve("closing.json");
        Files.writeString(path, "{\"previous\":true}");
        DefaultLiveDataService service = new DefaultLiveDataService(properties(path)) {
            @Override protected void writeSnapshot(Writer writer, JSONObject metadata, CompactLiveEventStore.Snapshot events) {
                throw new AssertionError("simulated serializer Error");
            }
        };
        service.addDanmu(event("old"));
        assertDoesNotThrow(service::onContextClosedEvent);
        assertEquals("{\"previous\":true}", Files.readString(path));
        assertEquals(LiveEventStorageUsage.empty(), service.getEventStorageUsage());
    }

    @Test
    void failedLoadDoesNotOverwriteExistingDataOnShutdown() throws Exception {
        Path path = tempDir.resolve("invalid.json");
        Files.writeString(path, "{\"unfinished\":");
        DefaultLiveDataService service = new DefaultLiveDataService(properties(path));
        service.onApplicationReadyEvent(); service.addDanmu(event("new")); service.onContextClosedEvent();
        assertEquals("{\"unfinished\":", Files.readString(path));
        assertEquals(LiveEventStorageUsage.empty(), service.getEventStorageUsage());
    }

    @Test
    void streamsEscapedMetadataAlongsidePinnedEventsAcrossReset() throws Exception {
        Path path = tempDir.resolve("snapshot.json");
        StarBotCoreProperties properties = properties(path);
        DefaultLiveDataService service = new DefaultLiveDataService(properties) {
            @Override protected void writeSnapshot(Writer writer, JSONObject metadata, CompactLiveEventStore.Snapshot events) throws Exception {
                resetLiveData("test", 1L); addDanmu(event("new"));
                super.writeSnapshot(writer, metadata, events);
            }
        };
        try {
            String text = "猫😀\"\\\n\t\b\f\r\u0001";
            service.setCustomObject(text, "escaped\"key"); service.addDanmu(event("old"));
            assertTrue(service.saveCache(path, false, false));
            assertEquals(text, JSONObject.parseObject(Files.readString(path)).getString("escaped\"key"));
            assertEquals("old", readEvent(path).getString("content"));
            assertEquals("new", service.getEvents("test", 1L, "Danmu").get(0).getString("content"));
            assertEquals(1, service.getEventStorageUsage().sessionCount());
        } finally { properties.getLive().setSaveLiveData(false); service.onContextClosedEvent(); }
    }

    @Test
    void loadsScalarCustomMetadataAlongsideLegacyEventRecords() throws Exception {
        Path path = tempDir.resolve("custom.json");
        Files.writeString(path, "{\"custom\":\"猫😀\",\"test\":{\"Danmu\":{\"1\":{\"2\":[{\"content\":\"saved\",\"timestamp\":10}]}}}}");
        StarBotCoreProperties properties = properties(path);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        try {
            service.onApplicationReadyEvent();
            assertEquals("猫😀", service.getCustomObject(String.class, "custom").orElseThrow());
            assertEquals("saved", service.getEvents("test", 1L, "Danmu").get(0).getString("content"));
            assertTrue(service.saveCache(path, false, false));
            assertEquals("猫😀", JSONObject.parseObject(Files.readString(path)).getString("custom"));
        } finally { properties.getLive().setSaveLiveData(false); service.onContextClosedEvent(); }
    }

    private JSONObject readEvent(Path path) throws IOException {
        return JSONObject.parseObject(Files.readString(path)).getJSONObject("test")
                .getJSONObject("Danmu").getJSONObject("1").getJSONArray("2").getJSONObject(0);
    }
    private StarBotCoreProperties properties(Path path) {
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getLive().setLiveDataPath(path.toString());
        properties.getLive().setMemoryBackend("heap"); properties.getLive().setColdCompression("none");
        return properties;
    }
    private DanmuEvent event(String content) {
        return new DanmuEvent("test", new LiveStreamerInfo(1L, "streamer", 3L), new UserInfo(2L, "viewer"), content, Instant.now());
    }
}
