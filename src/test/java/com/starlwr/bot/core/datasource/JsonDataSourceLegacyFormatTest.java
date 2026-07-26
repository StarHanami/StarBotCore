package com.starlwr.bot.core.datasource;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.StarBotExternalBaseEvent;
import com.starlwr.bot.core.enums.PushTargetType;
import com.starlwr.bot.core.exception.DataSourceException;
import com.starlwr.bot.core.handler.DefaultHandlerForEvent;
import com.starlwr.bot.core.handler.StarBotEventHandler;
import com.starlwr.bot.core.model.PushMessage;
import com.starlwr.bot.core.model.PushUser;
import com.starlwr.bot.core.service.StarBotEventHandlerService;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JsonDataSourceLegacyFormatTest {
    @Test
    void resolvesLegacyEventOnlyMessageToItsDefaultHandler() {
        LegacyHandler handler = new LegacyHandler();
        JsonDataSource dataSource = dataSourceWith(handler);

        List<PushUser> users = dataSource.parse("""
                [{
                  "uid": 1,
                  "platform": "test",
                  "targets": [{
                    "platform": "test-sender",
                    "type": 0,
                    "num": 2,
                    "messages": [{"event": "legacy-event"}]
                  }]
                }]
                """);

        PushMessage message = users.get(0).getTargets().get(0).getMessages().get(0);
        assertThat(message.getHandler()).isEqualTo(LegacyHandler.class.getName());
    }

    @Test
    void keepsExplicitHandlerWithoutRequiringLegacyEvent() {
        LegacyHandler handler = new LegacyHandler();
        JsonDataSource dataSource = dataSourceWith(handler);

        List<PushUser> users = dataSource.parse("""
                [{
                  "uid": 1,
                  "platform": "test",
                  "targets": [{
                    "platform": "test-sender",
                    "type": 0,
                    "num": 2,
                    "messages": [{"handler": "%s"}]
                  }]
                }]
                """.formatted(LegacyHandler.class.getName()));

        PushMessage message = users.get(0).getTargets().get(0).getMessages().get(0);
        assertThat(message.getHandler()).isEqualTo(LegacyHandler.class.getName());
    }

    @Test
    void parsesReadableFriendTargetType() {
        JsonDataSource dataSource = dataSourceWith(new LegacyHandler());

        List<PushUser> users = dataSource.parse("""
                [{
                  "uid": 1,
                  "platform": "test",
                  "targets": [{
                    "platform": "test-sender",
                    "type": "friend",
                    "num": 2870338968,
                    "messages": [{"handler": "%s"}]
                  }]
                }]
                """.formatted(LegacyHandler.class.getName()));

        assertThat(users.get(0).getTargets().get(0).getType()).isEqualTo(PushTargetType.FRIEND);
    }

    @Test
    void rejectsUnknownTargetTypeInsteadOfSilentlyRoutingIt() {
        JsonDataSource dataSource = dataSourceWith(new LegacyHandler());

        assertThatThrownBy(() -> dataSource.parse("""
                [{
                  "uid": 1,
                  "platform": "test",
                  "targets": [{
                    "platform": "test-sender",
                    "type": "channel",
                    "num": 2870338968,
                    "messages": []
                  }]
                }]
                """))
                .isInstanceOf(DataSourceException.class)
                .hasMessageContaining("无效的 type: channel")
                .hasMessageContaining("friend/0")
                .hasMessageContaining("group/1");
    }

    private JsonDataSource dataSourceWith(StarBotEventHandler handler) {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBeansOfType(StarBotEventHandler.class))
                .thenReturn(Map.of("legacyHandler", handler));

        StarBotEventHandlerService handlerService = new StarBotEventHandlerService(applicationContext);
        handlerService.onContextRefreshedEvent();
        return new JsonDataSource(
                mock(ApplicationEventPublisher.class),
                mock(DataSourceServiceRegistry.class),
                handlerService,
                new StarBotCoreProperties()
        );
    }

    @DefaultHandlerForEvent(event = "legacy-event")
    private static final class LegacyHandler implements StarBotEventHandler {
        @Override
        public void handle(StarBotExternalBaseEvent baseEvent, PushMessage pushMessage) {
        }

        @Override
        public Class<? extends StarBotExternalBaseEvent> getEventType() {
            return StarBotExternalBaseEvent.class;
        }

        @Override
        public JSONObject getDefaultParams() {
            return new JSONObject();
        }
    }
}
