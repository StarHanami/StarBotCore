package com.starlwr.bot.core.command;

import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.config.StarBotCoreThreadPoolConfig;
import com.starlwr.bot.core.message.Message;
import com.starlwr.bot.core.message.PushTargetType;
import com.starlwr.bot.core.message.StarBotMessageSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 消息接收事件（统一入站渠道回调）行为测试
 */
class StarBotMessageReceivedEventTest {
    @TempDir
    Path directory;

    private StarBotCoreProperties properties;

    private StarBotCommandPolicy policy;

    private ApplicationContext applicationContext;

    private StarBotCommandRegistry registry;

    private ThreadPoolTaskExecutor executor;

    private StarBotCommandService service;

    private StarBotMessageSender sender;

    private List<Message> replies;

    private List<StarBotMessageReceivedEvent> receivedEvents;

    private java.util.function.Consumer<StarBotMessageReceivedEvent> listener;

    @BeforeEach
    void setup() {
        properties = new StarBotCoreProperties();
        properties.getCommand().setDisabledCommandPath(directory.resolve("disabled-command.json").toString());
        policy = new StarBotCommandPolicy(properties);
        policy.onApplicationReadyEvent();
        applicationContext = mock(ApplicationContext.class);
        registry = new StarBotCommandRegistry(applicationContext);
        executor = new StarBotCoreThreadPoolConfig(properties).commandThreadPool();
        sender = mock(StarBotMessageSender.class);
        replies = new CopyOnWriteArrayList<>();
        doAnswer(call -> {
            replies.add(call.getArgument(0));
            return null;
        }).when(sender).send(any());
    }

    private void buildService(java.util.function.Consumer<StarBotMessageReceivedEvent> listener) {
        receivedEvents = new CopyOnWriteArrayList<>();
        service = new StarBotCommandService(executor, properties, registry, policy, this::recordReceivedEvent);
        this.listener = listener;
    }

    private void recordReceivedEvent(Object event) {
        StarBotMessageReceivedEvent received = (StarBotMessageReceivedEvent) event;
        receivedEvents.add(received);
        if (listener != null) listener.accept(received);
    }

    private void register(StarBotCommand command) {
        when(applicationContext.getBeansOfType(StarBotCommand.class)).thenReturn(Map.of("testCommand", command));
        registry.onContextRefreshedEvent();
    }

    private StarBotCommand echoCommand() {
        return new StarBotCommand() {
            @Override public String getName() { return "echo"; }
            @Override public String getDescription() { return "echo test command"; }
            @Override public void execute(CommandContext context) { context.reply("echo done"); }
        };
    }

    private CommandMessage.CommandMessageBuilder messageBuilder(String content) {
        JSONObject raw = new JSONObject().fluentPut("raw_message", content).fluentPut("post_type", "message");
        return CommandMessage.builder().platform("bot-a").botId("1")
                .senderId("2").groupId("100").type(PushTargetType.GROUP).content(content)
                .rawMessage(raw).messageSender(sender);
    }

    @Test
    void nonCommandMessagesAreObservableBeforeGates() {
        buildService(event -> { });
        register(echoCommand());

        service.dispatch(() -> Optional.of(messageBuilder("随便聊聊天").build())).join();

        assertTrue(replies.isEmpty(), "非命令消息不应触发命令回复");
        assertEquals(1, receivedEvents.size(), "非命令消息也必须可被插件观察");
        StarBotMessageReceivedEvent event = receivedEvents.get(0);
        assertEquals("随便聊聊天", event.getMessage().getContent());
        assertEquals("随便聊聊天", event.getMessage().getRawMessage().getString("raw_message"), "消息本体必须可达");
        assertEquals("bot-a", event.getMessage().getPlatform());
    }

    @Test
    void stoppedEventSkipsCommandExecution() {
        buildService(event -> event.stop(event));
        register(echoCommand());

        service.dispatch(() -> Optional.of(messageBuilder("echo hi").build())).join();

        assertTrue(replies.isEmpty(), "被 stop 的消息不应进入命令处理");
        assertEquals(1, receivedEvents.size());
    }

    @Test
    void listenerFailureDoesNotBlockCommandDispatch() {
        buildService(event -> { throw new IllegalStateException("listener boom"); });
        register(echoCommand());

        service.dispatch(() -> Optional.of(messageBuilder("echo hi").build())).join();

        assertEquals(1, replies.size(), "监听器异常不得阻断命令处理");
        assertTrue(replies.get(0).getContent().contains("echo done"));
    }
}
