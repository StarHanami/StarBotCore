package com.starlwr.bot.core.live;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.live.base.StarBotLiveInteractionEvent;
import com.starlwr.bot.core.event.live.base.StarBotLiveOperationEvent;
import com.starlwr.bot.core.event.live.base.StarBotLiveUserEvent;
import com.starlwr.bot.core.event.live.common.DanmuEvent;
import com.starlwr.bot.core.event.live.common.LikeEvent;
import com.starlwr.bot.core.event.model.LiveStreamerInfo;
import com.starlwr.bot.core.event.model.UserInfo;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

class DefaultLiveDataServiceTest {
    @Test
    void migratesLegacyFlatLiveKeysIntoNestedPlatformLayout() {
        JSONObject loaded = JSONObject.parseObject("""
                {
                  "LiveStatus:BILIBILI": {"123": true},
                  "LiveStartTime:BILIBILI": {"123": 1700000000000},
                  "LiveEndTime:BILIBILI": {"123": 1700003600000}
                }
                """);

        JSONObject normalized = DefaultLiveDataService.normalizeCache(loaded);

        assertThat(normalized).doesNotContainKeys(
                "LiveStatus:BILIBILI", "LiveStartTime:BILIBILI", "LiveEndTime:BILIBILI");
        assertThat(normalized.getJSONObject("BILIBILI").getJSONObject("LiveStatus").getBooleanValue("123"))
                .isTrue();
        assertThat(normalized.getJSONObject("BILIBILI").getJSONObject("LiveStartTime").getLongValue("123"))
                .isEqualTo(1700000000000L);
        assertThat(normalized.getJSONObject("BILIBILI").getJSONObject("LiveEndTime").getLongValue("123"))
                .isEqualTo(1700003600000L);
    }

    @Test
    void canonicalValuesWinWhenLegacyAndNestedKeysBothExist() {
        JSONObject loaded = JSONObject.parseObject("""
                {
                  "BILIBILI": {"LiveStatus": {"123": false}},
                  "LiveStatus:BILIBILI": {"123": true, "456": true}
                }
                """);

        JSONObject normalized = DefaultLiveDataService.normalizeCache(loaded);

        JSONObject statuses = normalized.getJSONObject("BILIBILI").getJSONObject("LiveStatus");
        assertThat(statuses.getBooleanValue("123")).isFalse();
        assertThat(statuses.getBooleanValue("456")).isTrue();
    }

    @Test
    void persistsAndResetsUserEventsUsingTheNewNestedApi() {
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getLive().setSaveLiveData(false);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        LiveStreamerInfo streamer = new LiveStreamerInfo("123", "streamer", "456");
        DanmuEvent event = new DanmuEvent("bilibili", streamer, new UserInfo("789", "viewer"),
                "hello", Instant.ofEpochMilli(1700000000000L));

        service.addDanmu(event);

        assertThat(service.getDanmu("bilibili", "123", DanmuEvent.class))
                .singleElement().extracting(DanmuEvent::getContent).isEqualTo("hello");
        assertThat(service.getUserDanmu("bilibili", "123", "789", DanmuEvent.class)).hasSize(1);

        service.resetLiveData("bilibili", "123");

        assertThat(service.getDanmu("bilibili", "123", DanmuEvent.class)).isEmpty();
        service.onContextClosedEvent();
    }

    @Test
    void likeEventIsAnOperationAndUserEventButNotAnInteractionEvent() {
        LikeEvent event = new LikeEvent("bilibili", new LiveStreamerInfo("123", "streamer", "456"),
                new UserInfo("789", "viewer"));

        assertThat(event).isInstanceOf(StarBotLiveOperationEvent.class)
                .isInstanceOf(StarBotLiveUserEvent.class)
                .isNotInstanceOf(StarBotLiveInteractionEvent.class);
    }

    @Test
    void loadsHistoricalCompactSenderRecordsAsTypedEvents() throws Exception {
        Path dataPath = Files.createTempFile("starbot-live-data", ".json");
        Files.writeString(dataPath, """
                {"bilibili":{"Danmu":{"123":{"789":[
                  {"content":"legacy","timestamp":1700000000000}
                ]}}}}
                """, StandardCharsets.UTF_8);
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getLive().setSaveLiveData(true);
        properties.getLive().setLiveDataPath(dataPath.toString());
        DefaultLiveDataService service = new DefaultLiveDataService(properties);

        try {
            service.onApplicationReadyEvent();
            DanmuEvent event = service.getDanmu("bilibili", "123", DanmuEvent.class).get(0);
            assertThat(event.getContent()).isEqualTo("legacy");
            assertThat(event.getSender().getUid()).isEqualTo("789");
            assertThat(service.getUserDanmu("bilibili", "123", "789", JSONObject.class).get(0)
                    .getJSONObject("sender").getString("uid")).isEqualTo("789");
        } finally {
            service.onContextClosedEvent();
            Files.deleteIfExists(dataPath);
        }
    }

    @Test
    void streamsEventsWithoutChangingPayloadAndSeparatesResetSessions() {
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getLive().setSaveLiveData(false);
        DefaultLiveDataService service = new DefaultLiveDataService(properties);
        LiveStreamerInfo streamer = new LiveStreamerInfo("123", "streamer", "456");
        DanmuEvent first = new DanmuEvent("bilibili", streamer, new UserInfo("789", "viewer"),
                "emoji 😀", Instant.ofEpochMilli(1700000000000L));
        service.addDanmu(first);

        List<JSONObject> streamed = new ArrayList<>();
        service.forEachEvent("bilibili", "123", "Danmu", streamed::add);
        assertThat(streamed).singleElement().satisfies(json -> {
            assertThat(json.getString("content")).isEqualTo("emoji 😀");
            assertThat(json.getJSONObject("sender").getString("uid")).isEqualTo("789");
        });

        service.resetLiveData("bilibili", "123");
        assertThat(service.getDanmu("bilibili", "123", DanmuEvent.class)).isEmpty();
        service.addDanmu(new DanmuEvent("bilibili", streamer, new UserInfo("789", "viewer"),
                "second", Instant.ofEpochMilli(1700000001000L)));
        service.deactivate("bilibili", "123", "test");
        assertThat(service.getDanmu("bilibili", "123", DanmuEvent.class)).singleElement()
                .extracting(DanmuEvent::getContent).isEqualTo("second");
        service.onContextClosedEvent();
    }
}
