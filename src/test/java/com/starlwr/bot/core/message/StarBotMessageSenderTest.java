package com.starlwr.bot.core.message;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.message.PushTargetType;
import com.starlwr.bot.core.message.Message;
import com.starlwr.bot.core.message.Sender;
import com.starlwr.bot.core.message.StarBotSenderService;
import com.starlwr.bot.core.support.http.HttpService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StarBotMessageSenderTest {
    @Test
    void drainsQueuedMessagesAndRejectsNewMessagesAfterClose() {
        HttpService http = mock(HttpService.class);
        StarBotSenderService senderService = mock(StarBotSenderService.class);
        Sender platform = new Sender("test", "http://127.0.0.1/send", 0);
        when(senderService.getSender("test")).thenReturn(Optional.of(platform));
        when(http.postJson(anyString(), anyMap(), any()))
                .thenReturn(new JSONObject().fluentPut("code", 0).fluentPut("id", "message-id"));

        StarBotMessageSender sender = new StarBotMessageSender(http, senderService);
        List<Message> messages = List.of("one", "two", "three").stream()
                .flatMap(content -> Message.create("test", PushTargetType.FRIEND, "2870338968", content).stream())
                .toList();
        messages.forEach(sender::send);

        sender.close();
        verify(http, times(3)).postJson(anyString(), anyMap(), any());

        sender.send(Message.create("test", PushTargetType.FRIEND, "2870338968", "late").get(0));
        verify(http, times(3)).postJson(anyString(), anyMap(), any());
    }
}
