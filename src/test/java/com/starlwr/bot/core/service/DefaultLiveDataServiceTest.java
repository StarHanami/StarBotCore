package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.event.live.base.StarBotLiveInteractionEvent;
import com.starlwr.bot.core.event.live.base.StarBotLiveOperationEvent;
import com.starlwr.bot.core.event.live.base.StarBotLiveUserEvent;
import com.starlwr.bot.core.event.live.common.DanmuEvent;
import com.starlwr.bot.core.event.live.common.LikeEvent;
import com.starlwr.bot.core.model.LiveStreamerInfo;
import com.starlwr.bot.core.model.UserInfo;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

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
        DefaultLiveDataService service = new DefaultLiveDataService(new com.starlwr.bot.core.config.StarBotCoreProperties());
        LiveStreamerInfo streamer = new LiveStreamerInfo(123L, "streamer", 456L);
        DanmuEvent event = new DanmuEvent("bilibili", streamer, new UserInfo(789L, "viewer"),
                "hello", Instant.ofEpochMilli(1700000000000L));

        service.addDanmu(event);

        assertThat(service.getDanmu("bilibili", 123L, DanmuEvent.class))
                .singleElement().extracting(DanmuEvent::getContent).isEqualTo("hello");
        assertThat(service.getUserDanmu("bilibili", 123L, 789L, DanmuEvent.class)).hasSize(1);

        service.resetLiveData("bilibili", 123L);

        assertThat(service.getDanmu("bilibili", 123L, DanmuEvent.class)).isEmpty();
    }

    @Test
    void likeEventIsAnOperationAndUserEventButNotAnInteractionEvent() {
        LikeEvent event = new LikeEvent("bilibili", new LiveStreamerInfo(123L, "streamer", 456L),
                new UserInfo(789L, "viewer"));

        assertThat(event).isInstanceOf(StarBotLiveOperationEvent.class)
                .isInstanceOf(StarBotLiveUserEvent.class)
                .isNotInstanceOf(StarBotLiveInteractionEvent.class);
    }
}
