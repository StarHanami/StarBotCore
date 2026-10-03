package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class LiveDataStreamingLoadTest {
    @Test void importsLargeDocumentIncrementallyBeforeTheInputHasBeenFullyRead() throws Exception {
        StarBotCoreProperties.Live properties = new StarBotCoreProperties.Live();
        properties.setMemoryBackend("heap"); properties.setColdCompression("none");
        try (CompactLiveEventStore store = new CompactLiveEventStore(properties)) {
            byte[] event = JSON.toJSONBytes(new JSONObject().fluentPut("content", "x".repeat(900_000)).fluentPut("timestamp", 10));
            Enumeration<InputStream> pieces = new Enumeration<>() {
                int part;
                public boolean hasMoreElements() { return part < 83; }
                public InputStream nextElement() {
                    int index = part++;
                    if(index == 0) return bytes("{\"test\":{\"LiveStartTime\":{\"1\":10},\"Danmu\":{\"1\":{\"2\":[");
                    if(index == 82) return bytes("]}}},\"custom\":\"猫😀\"}");
                    // Allow one token-buffer lookahead; document-wide buffering fails this invariant.
                    if(index > 2) assertTrue(store.usage().eventCount() >= index - 2, "events must be imported while bytes arrive");
                    return index == 1 ? new ByteArrayInputStream(event)
                            : new SequenceInputStream(bytes(","), new ByteArrayInputStream(event));
                }
            };
            JSONObject metadata = LiveDataJsonReader.read(new SequenceInputStream(pieces), store);
            assertEquals(81, store.usage().eventCount());
            assertTrue(store.usage().payloadBytes() > 64L * 1024 * 1024);
            assertEquals("猫😀", metadata.getString("custom"));
            assertFalse(metadata.getJSONObject("test").containsKey("Danmu"));
            AtomicInteger count = new AtomicInteger();
            store.forEach("test",1L,"Danmu", json -> { assertEquals(2L,json.getJSONObject("sender").getLongValue("uid")); count.incrementAndGet(); });
            assertEquals(81, count.get());
        }
    }

    @Test void rejectsTrailingDataAndTruncatedArraysInsteadOfOverwritingTheLastSave() {
        StarBotCoreProperties.Live properties = new StarBotCoreProperties.Live(); properties.setMemoryBackend("heap");
        try (CompactLiveEventStore store = new CompactLiveEventStore(properties)) {
            assertThrows(Exception.class, () -> LiveDataJsonReader.read(bytes("{}{}"),store));
            assertThrows(Exception.class, () -> LiveDataJsonReader.read(bytes("{\"test\":{\"Danmu\":{\"1\":{\"2\":["),store));
        }
    }

    private static InputStream bytes(String text) { return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)); }
}
