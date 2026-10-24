package com.starlwr.bot.core.push.datasource;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.StarBotExternalBaseEvent;
import com.starlwr.bot.core.push.handler.StarBotEventHandler;
import com.starlwr.bot.core.push.model.PushMessage;
import com.starlwr.bot.core.push.handler.StarBotEventHandlerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JsonDataSourceManagementTest {
    @TempDir
    Path tempDir;

    @Test
    void watcherWaitsForManagementTransactionAndThenSkipsItsOwnFileEvent() throws Exception {
        Path path = tempDir.resolve("datasource.json");
        Files.writeString(path, "[]");
        JsonDataSource source = source(path);
        var lockField = JsonDataSource.class.getDeclaredField("managementLock");
        lockField.setAccessible(true);
        Object lock = lockField.get(source);
        var reload = JsonDataSource.class.getDeclaredMethod("reloadFromDisk");
        reload.setAccessible(true);
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Thread watcher = new Thread(() -> {
            try { reload.invoke(source); } catch (Throwable e) { failure.set(e); }
        });
        try {
            synchronized (lock) {
                watcher.start();
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
                while (watcher.isAlive() && watcher.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline)
                    Thread.onSpinWait();
                assertThat(watcher.getState()).isEqualTo(Thread.State.BLOCKED);
                source.apply(JSON.parseArray("[{\"uid\":123,\"platform\":\"bilibili\",\"enabled\":false,\"targets\":[]}]"), source.snapshot().revision());
            }
            watcher.join(2_000);
            assertThat(watcher.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            var revisionField = JsonDataSource.class.getDeclaredField("lastAppliedRevision");
            revisionField.setAccessible(true);
            assertThat(revisionField.get(source)).isEqualTo(source.snapshot().revision());
            assertThat(source.snapshot().document().getJSONObject(0).getLongValue("uid")).isEqualTo(123L);
        } finally {
            source.close();
        }
    }

    @Test
    void appliesAtomicallyAndKeepsDisabledEntriesInDocument() throws Exception {
        Path path = tempDir.resolve("datasource.json");
        Files.writeString(path, "[]");
        JsonDataSource source = source(path);
        JsonDataSourceManagementService.Snapshot before = source.snapshot();

        var document = JSON.parseArray("""
                [{"uid":123,"platform":"bilibili","enabled":false,"targets":[]}]
                """);
        JsonDataSourceManagementService.ApplyResult result = source.apply(document, before.revision());

        assertThat(result.changed()).isTrue();
        assertThat(source.snapshot().document().getJSONObject(0).getBooleanValue("enabled")).isFalse();
        assertThat(source.getAllUsers()).isEmpty();
    }

    @Test
    void rejectsStaleRevisionWithoutChangingFile() throws Exception {
        Path path = tempDir.resolve("datasource.json");
        Files.writeString(path, "[]");
        JsonDataSource source = source(path);
        var document = JSON.parseArray("[{\"uid\":123,\"platform\":\"bilibili\",\"enabled\":false}]");

        assertThatThrownBy(() -> source.apply(document, "stale"))
                .isInstanceOf(JsonDataSourceRevisionConflictException.class);
        assertThat(JSON.parseArray(Files.readString(path))).isEmpty();
    }

    @Test
    void rejectsUnknownExplicitHandlerDuringValidation() throws Exception {
        Path path = tempDir.resolve("datasource.json");
        Files.writeString(path, "[]");
        JsonDataSource source = source(path);
        var document = JSON.parseArray("""
                [{"uid":1,"platform":"bilibili","enabled":false,"targets":[{
                  "platform":"qq","type":"friend","num":2,
                  "messages":[{"handler":"missing.Handler"}]
                }]}]
                """);

        assertThat(source.validate(document).valid()).isFalse();
        assertThat(source.validate(document).errors()).anyMatch(message -> message.contains("missing.Handler"));
    }

    private JsonDataSource source(Path path) {
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(StarBotEventHandler.class)).thenReturn(Map.of("handler", new Handler()));
        StarBotEventHandlerService handlers = new StarBotEventHandlerService(context);
        handlers.onContextRefreshedEvent();
        StarBotCoreProperties properties = new StarBotCoreProperties();
        properties.getDatasource().setJsonPath(path.toString());
        return new JsonDataSource(mock(ApplicationEventPublisher.class), mock(DataSourceServiceRegistry.class), handlers, properties);
    }

    private static final class Handler implements StarBotEventHandler {
        public void handle(StarBotExternalBaseEvent event, PushMessage message) {}
        public Class<? extends StarBotExternalBaseEvent> getEventType() { return StarBotExternalBaseEvent.class; }
        public JSONObject getDefaultParams() { return new JSONObject(); }
    }
}
