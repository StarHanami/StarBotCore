package com.starlwr.bot.core.command;

import com.starlwr.bot.core.event.StarBotBaseEvent;
import lombok.Getter;

/**
 * 入站消息接收事件
 * <p>在各消息平台提交的消息进入命令闸门（提及/前缀过滤）之前发布，
 * 插件可监听此事件观察全部入站消息（含 {@link CommandMessage#getRawMessage()} 消息本体）。</p>
 * <p>监听器调用 {@link #stop(Object)} 后，该消息不再进入命令处理流程。</p>
 */
@Getter
public class StarBotMessageReceivedEvent extends StarBotBaseEvent {
    private final CommandMessage message;

    public StarBotMessageReceivedEvent(CommandMessage message) {
        this.message = message;
    }
}
