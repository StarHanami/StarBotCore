package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.live.common.DanmuEvent;
import com.starlwr.bot.core.model.LiveStreamerInfo;
import com.starlwr.bot.core.model.UserInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultLiveDataServiceConcurrencyTest {
    private static final String PLATFORM = "test";
    private static final long SOURCE_UID = 1001L;
    private static final long SENDER_UID = 2001L;

    @TempDir
    Path tempDir;

    @Test
    void concurrentReadWriteAndSaveProducesCompleteValidSnapshots() throws Exception {
        Path dataPath = tempDir.resolve("concurrent-data.json");
        DefaultLiveDataService service = createService(dataPath);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        int writers = 4;
        int eventsPerWriter = 250;

        try {
            for (int writer = 0; writer < writers; writer++) {
                int writerId = writer;
                futures.add(executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < eventsPerWriter; i++) {
                        service.addDanmu(createDanmu(writerId + "-" + i));
                        service.setLiveStatus(PLATFORM, SOURCE_UID, i % 2 == 0);
                        service.setCustomObject(Map.of("writer", writerId, "value", i),
                                PLATFORM, "Custom", String.valueOf(writerId));
                    }
                }));
            }
            for (int reader = 0; reader < 2; reader++) {
                futures.add(executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < 250; i++) {
                        service.getDanmu(PLATFORM, SOURCE_UID, JSONObject.class);
                        service.getUserDanmu(PLATFORM, SOURCE_UID, SENDER_UID, DanmuEvent.class);
                        service.getCustomObject(Map.class, PLATFORM, "Custom", "0");
                    }
                }));
            }
            for (int saver = 0; saver < 2; saver++) {
                futures.add(executor.submit(() -> {
                    await(start);
                    for (int i = 0; i < 60; i++) {
                        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(
                                service, "saveCache", dataPath, false, false));
                        try {
                            assertNotNull(JSONObject.parseObject(Files.readString(dataPath, StandardCharsets.UTF_8)));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }));
            }

            start.countDown();
            for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);

            assertEquals(writers * eventsPerWriter,
                    service.getUserDanmu(PLATFORM, SOURCE_UID, SENDER_UID, DanmuEvent.class).size());
            assertNotNull(JSONObject.parseObject(Files.readString(dataPath, StandardCharsets.UTF_8)));
        } finally {
            service.onContextClosedEvent();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void eventQueriesReturnDetachedJsonAndTypedObjects() throws Exception {
        Path dataPath = tempDir.resolve("query-data.json");
        DefaultLiveDataService service = createService(dataPath);
        service.addDanmu(createDanmu("hello"));

        JSONObject json = service.getDanmu(PLATFORM, SOURCE_UID, JSONObject.class).get(0);
        DanmuEvent typed = service.getUserDanmu(
                PLATFORM, SOURCE_UID, SENDER_UID, DanmuEvent.class).get(0);
        assertEquals(SENDER_UID, typed.getSender().getUid());
        json.put("mutated", true);
        json.getJSONObject("sender").put("uid", 9999L);
        typed.getSender().setUid(9999L);

        assertEquals(SENDER_UID, service.getDanmu(PLATFORM, SOURCE_UID, DanmuEvent.class)
                .get(0).getSender().getUid());
        service.onContextClosedEvent();

        JSONObject stored = storedEvent(dataPath);
        assertFalse(stored.containsKey("mutated"));
        assertEquals(SENDER_UID, stored.getJSONObject("sender").getLongValue("uid"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void customObjectsAreDetachedOnWriteAndRead() {
        DefaultLiveDataService service = createService(tempDir.resolve("custom-data.json"));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("value", 1);
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("nested", nested);

        service.setCustomObject(original, "Custom");
        nested.put("value", 2);
        original.put("added", true);
        Map<String, Object> first = service.getCustomObject(Map.class, "Custom").orElseThrow();
        assertEquals(1, ((Map<String, Object>) first.get("nested")).get("value"));
        assertFalse(first.containsKey("added"));

        ((Map<String, Object>) first.get("nested")).put("value", 3);
        assertEquals(1, ((Map<String, Object>) service.getCustomObject(Map.class, "Custom")
                .orElseThrow().get("nested")).get("value"));
        service.setCustomObject(42, "Integer");
        assertEquals(42, service.getCustomObject(Integer.class, "Integer").orElseThrow());
        service.onContextClosedEvent();
    }

    @Test
    void closingSaveIsFinalAndRejectsSubsequentMutation() throws Exception {
        Path dataPath = tempDir.resolve("closing-data.json");
        DefaultLiveDataService service = createService(dataPath);
        service.setCustomObject("final", "State");
        service.onContextClosedEvent();

        service.setCustomObject("too-late", "State");
        service.addDanmu(createDanmu("too-late"));
        assertEquals(Boolean.FALSE, ReflectionTestUtils.invokeMethod(
                service, "saveCache", dataPath, false, false));
        service.onContextClosedEvent();

        JSONObject stored = JSONObject.parseObject(Files.readString(dataPath, StandardCharsets.UTF_8));
        assertEquals("final", stored.getString("State"));
        assertNull(stored.getJSONObject(PLATFORM));
        ScheduledExecutorService scheduler = (ScheduledExecutorService) ReflectionTestUtils.getField(service, "scheduler");
        assertNotNull(scheduler);
        assertTrue(scheduler.isShutdown());
    }

    private DefaultLiveDataService createService(Path dataPath) {
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getLive().setSaveLiveData(true);
        properties.getLive().setLiveDataPath(dataPath.toString());
        return new DefaultLiveDataService(properties);
    }

    private DanmuEvent createDanmu(String content) {
        return new DanmuEvent(PLATFORM,
                new LiveStreamerInfo(SOURCE_UID, "streamer", 3001L),
                new UserInfo(SENDER_UID, "viewer"), content, Instant.now());
    }

    private JSONObject storedEvent(Path dataPath) throws Exception {
        return JSONObject.parseObject(Files.readString(dataPath, StandardCharsets.UTF_8))
                .getJSONObject(PLATFORM).getJSONObject("Danmu")
                .getJSONObject(String.valueOf(SOURCE_UID))
                .getJSONArray(String.valueOf(SENDER_UID)).getJSONObject(0);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
