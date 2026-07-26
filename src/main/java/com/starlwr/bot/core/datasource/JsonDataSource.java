package com.starlwr.bot.core.datasource;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.enums.PushTargetType;
import com.starlwr.bot.core.event.datasource.other.StarBotDataSourceLoadCompleteEvent;
import com.starlwr.bot.core.exception.DataSourceException;
import com.starlwr.bot.core.model.PushMessage;
import com.starlwr.bot.core.model.PushTarget;
import com.starlwr.bot.core.model.PushUser;
import com.starlwr.bot.core.handler.StarBotEventHandler;
import com.starlwr.bot.core.service.StarBotEventHandlerService;
import com.starlwr.bot.core.util.CollectionUtil;
import com.starlwr.bot.core.util.StringUtil;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JSON 数据源
 */
@Profile("json")
@Slf4j
@Service
@DataSource(name = "json")
public class JsonDataSource extends AbstractDataSource implements JsonDataSourceManagementService {
    private final StarBotCoreProperties properties;

    private final StarBotEventHandlerService handlerService;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile WatchService watchService;

    private ScheduledFuture<?> pendingTask;

    private final AtomicLong lastTriggeredTime = new AtomicLong(0);

    private final long debounceDelayMillis = 1000L;

    private final Object managementLock = new Object();

    private volatile String lastAppliedRevision;

    @Autowired
    public JsonDataSource(ApplicationEventPublisher eventPublisher, DataSourceServiceRegistry dataSourceServiceRegistry, StarBotEventHandlerService handlerService, StarBotCoreProperties properties) {
        super(eventPublisher, dataSourceServiceRegistry, handlerService);
        this.properties = properties;
        this.handlerService = handlerService;
    }

    /**
     * 加载数据源，读取完毕后需调用 add 方法将推送用户添加至数据源中
     * PushUser 仅须填充 uid, platform, enabled, targets 字段
     * PushTarget 仅须填充 user, platform, type, num, enabled, messages 字段
     * PushMessage 仅须填充 target, event, handler, params, enabled 字段
     */
    @Override
    public void load() {
        log.info("已选用 JSON 作为数据源");
        log.info("开始从 JSON 中初始化推送配置");

        String path = properties.getDatasource().getJsonPath();
        try {
            String content = Files.readString(Path.of(path));
            List<PushUser> users = parse(content);
            add(users);
            lastAppliedRevision = revision(content);
        } catch (NoSuchFileException e) {
            throw new DataSourceException("数据源 JSON 文件不存在, 请检查配置的路径是否正确: " + path);
        } catch (Exception e) {
            throw new DataSourceException("读取数据源 JSON 文件异常", e);
        }

        log.info("成功从 JSON 中导入了 {} 个主播", this.users.size());

        eventPublisher.publishEvent(new StarBotDataSourceLoadCompleteEvent(new ArrayList<>(this.users)));

        if (properties.getDatasource().isJsonAutoReload()) {
            watchFileUpdate();
        }
    }

    /**
     * 监听 JSON 文件更新
     */
    private void watchFileUpdate() {
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            this.watchService = watchService;
            Path jsonPath = Paths.get(properties.getDatasource().getJsonPath()).toAbsolutePath();
            Path parentPath = jsonPath.getParent();
            parentPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE);

            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path changed = (Path) event.context();
                            if (changed != null && changed.getFileName().equals(jsonPath.getFileName())) {
                                if (pendingTask != null && !pendingTask.isDone()) {
                                    pendingTask.cancel(false);
                                }

                                pendingTask = scheduler.schedule(() -> {
                                    Thread.currentThread().setName("json-watcher");
                                    long now = System.currentTimeMillis();
                                    long last = lastTriggeredTime.getAndSet(now);
                                    if (now - last >= debounceDelayMillis) {
                                        reloadFromDisk();
                                    }
                                }, debounceDelayMillis, TimeUnit.MILLISECONDS);
                            }
                        }

                        if (!key.reset()) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("监听数据源 JSON 文件更新异常", e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("监听数据源 JSON 文件更新异常", e);
        }
    }

    @PreDestroy
    public void close() {
        if (pendingTask != null) {
            pendingTask.cancel(true);
        }
        scheduler.shutdownNow();
        executor.shutdownNow();
        WatchService current = watchService;
        watchService = null;
        if (current != null) {
            try {
                current.close();
            } catch (Exception e) {
                log.debug("关闭 JSON 数据源文件监听器异常", e);
            }
        }
    }

    /**
     * 重载数据源
     */
    private void reloadFromDisk() {
        String path = properties.getDatasource().getJsonPath();
        try {
            String content = Files.readString(Path.of(path), StandardCharsets.UTF_8);
            String currentRevision = revision(content);
            if (Objects.equals(currentRevision, lastAppliedRevision)) {
                log.debug("忽略由当前进程原子应用触发的数据源文件事件: revision={}", currentRevision);
                return;
            }
            log.info("检测到数据源 JSON 文件已更新, 开始从 JSON 中重载推送配置");
            List<PushUser> addUsers = new ArrayList<>();
            List<PushUser> removeUsers = new ArrayList<>();
            List<PushUser> updateUsers = new ArrayList<>();

            List<PushUser> users = parse(content);
            if (new HashSet<>(users).size() != users.size()) {
                throw new DataSourceException("推送用户列表中存在重复的用户");
            }

            CollectionUtil.compareCollectionDiff(this.users, users, addUsers, removeUsers, updateUsers);

            add(addUsers);
            for (PushUser user : removeUsers) {
                remove(user);
            }
            update(updateUsers);
            lastAppliedRevision = currentRevision;
        } catch (Exception e) {
            log.error("重载数据源 JSON 文件异常", e);
        }
    }

    /**
     * 解析 JSON 数据
     * @param json JSON 数据
     * @return 解析出的 PushUser 列表
     */
    @Override
    public Snapshot snapshot() {
        synchronized (managementLock) {
            try {
                String content = Files.readString(datasourcePath(), StandardCharsets.UTF_8);
                JSONArray document = JSON.parseArray(content);
                if (document == null) {
                    throw new DataSourceException("JSON 数据源根节点必须是数组");
                }
                return new Snapshot(revision(content), document);
            } catch (Exception e) {
                throw new DataSourceException("读取 JSON 数据源快照失败", e);
            }
        }
    }

    @Override
    public Validation validate(JSONArray document) {
        List<String> errors = new ArrayList<>();
        String canonical = "";
        try {
            if (document == null) {
                throw new DataSourceException("JSON 数据源根节点必须是数组");
            }
            canonical = JSON.toJSONString(document, JSONWriter.Feature.PrettyFormat);
            List<PushUser> parsed = parse(canonical);
            if (new HashSet<>(parsed).size() != parsed.size()) {
                throw new DataSourceException("推送用户列表中存在重复的平台和 UID");
            }
            validateConfiguredHandlers(document);
        } catch (Exception e) {
            errors.add(e.getMessage() == null ? e.toString() : e.getMessage());
        }
        return new Validation(errors.isEmpty(), canonical, List.copyOf(errors));
    }

    @Override
    public ApplyResult apply(JSONArray document, String expectedRevision) {
        synchronized (managementLock) {
            Validation validation = validate(document);
            if (!validation.valid()) {
                throw new DataSourceException("JSON 数据源校验失败: " + String.join("; ", validation.errors()));
            }
            Path path = datasourcePath();
            try {
                String previous = Files.readString(path, StandardCharsets.UTF_8);
                String previousRevision = revision(previous);
                if (expectedRevision != null && !expectedRevision.isBlank() && !expectedRevision.equals(previousRevision)) {
                    throw new JsonDataSourceRevisionConflictException(expectedRevision, previousRevision);
                }
                String canonical = validation.canonicalJson();
                String nextRevision = revision(canonical);
                if (nextRevision.equals(previousRevision)) {
                    return new ApplyResult(previousRevision, false, Instant.now(), List.of());
                }

                writeAtomically(path, canonical);
                try {
                    applyRuntime(canonical);
                    lastAppliedRevision = nextRevision;
                } catch (Exception runtimeFailure) {
                    writeAtomically(path, previous);
                    try {
                        applyRuntime(previous);
                        lastAppliedRevision = previousRevision;
                    } catch (Exception rollbackFailure) {
                        runtimeFailure.addSuppressed(rollbackFailure);
                    }
                    throw runtimeFailure;
                }
                return new ApplyResult(nextRevision, true, Instant.now(), List.of());
            } catch (JsonDataSourceRevisionConflictException e) {
                throw e;
            } catch (Exception e) {
                throw new DataSourceException("应用 JSON 数据源失败", e);
            }
        }
    }

    private void applyRuntime(String content) {
        List<PushUser> addUsers = new ArrayList<>();
        List<PushUser> removeUsers = new ArrayList<>();
        List<PushUser> updateUsers = new ArrayList<>();
        List<PushUser> parsed = parse(content);
        if (new HashSet<>(parsed).size() != parsed.size()) {
            throw new DataSourceException("推送用户列表中存在重复的平台和 UID");
        }
        CollectionUtil.compareCollectionDiff(this.users, parsed, addUsers, removeUsers, updateUsers);
        add(addUsers);
        for (PushUser user : removeUsers) {
            remove(user);
        }
        update(updateUsers);
    }

    private void validateConfiguredHandlers(JSONArray document) {
        for (int userIndex = 0; userIndex < document.size(); userIndex++) {
            JSONObject user = document.getJSONObject(userIndex);
            JSONArray targets = user == null ? null : user.getJSONArray("targets");
            if (targets == null) continue;
            for (int targetIndex = 0; targetIndex < targets.size(); targetIndex++) {
                JSONObject target = targets.getJSONObject(targetIndex);
                JSONArray messages = target == null ? null : target.getJSONArray("messages");
                if (messages == null) continue;
                for (int messageIndex = 0; messageIndex < messages.size(); messageIndex++) {
                    JSONObject message = messages.getJSONObject(messageIndex);
                    String handler = message == null ? null : message.getString("handler");
                    if (StringUtil.isNotBlank(handler) && handlerService.getHandler(handler).isEmpty()) {
                        throw new DataSourceException("不存在的消息处理器: " + handler);
                    }
                }
            }
        }
    }

    private Path datasourcePath() {
        return Paths.get(properties.getDatasource().getJsonPath()).toAbsolutePath().normalize();
    }

    private void writeAtomically(Path path, String content) throws Exception {
        Files.createDirectories(path.getParent());
        Path temp = Files.createTempFile(path.getParent(), path.getFileName().toString(), ".tmp");
        try {
            Files.writeString(temp, content, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private String revision(String content) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("无法计算 JSON 数据源 revision", e);
        }
    }

    protected List<PushUser> parse(String json) {
        List<PushUser> users = new ArrayList<>();

        List<String> userRequiredFields = List.of("uid", "platform");
        List<String> targetRequiredFields = List.of("platform", "type", "num");
        for (JSONObject userObject : JSON.parseArray(json).toList(JSONObject.class)) {
            for (String field : userRequiredFields) {
                if (!userObject.containsKey(field)) {
                    throw new DataSourceException("数据源 JSON 文件格式错误, " + field + " 字段缺失");
                }
            }

            PushUser user = new PushUser();
            user.setUid(userObject.getLong("uid"));
            user.setPlatform(userObject.getString("platform"));
            if (!userObject.containsKey("enabled")) {
                user.setEnabled(true);
            } else {
                user.setEnabled(userObject.getBoolean("enabled"));
            }
            user.setTargets(new ArrayList<>());

            if (userObject.containsKey("targets")) {
                for (JSONObject targetObject : userObject.getJSONArray("targets").toList(JSONObject.class)) {
                    for (String field : targetRequiredFields) {
                        if (!targetObject.containsKey(field)) {
                            throw new DataSourceException("数据源 JSON 文件格式错误, 缺失 " + field + " 字段");
                        }
                    }

                    PushTarget target = new PushTarget();
                    target.setUser(user);
                    target.setPlatform(targetObject.getString("platform"));
                    Object configuredType = targetObject.get("type");
                    PushTargetType targetType = PushTargetType.fromConfigValue(configuredType);
                    if (targetType == PushTargetType.UNKNOWN) {
                        throw new DataSourceException("数据源 JSON 文件格式错误, UID " + user.getUid()
                                + " 的目标 " + targetObject.get("num") + " 使用了无效的 type: " + configuredType
                                + "（好友使用 friend/0，群使用 group/1）");
                    }
                    target.setType(targetType);
                    target.setNum(targetObject.getLong("num"));
                    if (!targetObject.containsKey("enabled")) {
                        target.setEnabled(true);
                    } else {
                        target.setEnabled(targetObject.getBoolean("enabled"));
                    }
                    target.setMessages(new ArrayList<>());
                    user.getTargets().add(target);

                    if (targetObject.containsKey("messages")) {
                        int messageIndex = 0;
                        for (JSONObject messageObject : targetObject.getJSONArray("messages").toList(JSONObject.class)) {
                            int currentMessageNumber = messageIndex + 1;
                            String handlerClass = messageObject.getString("handler");
                            if (StringUtil.isBlank(handlerClass)) {
                                String eventClass = messageObject.getString("event");
                                if (StringUtil.isBlank(eventClass)) {
                                    throw new DataSourceException("数据源 JSON 文件格式错误, UID " + user.getUid()
                                            + " 的目标 " + target.getNum() + " 第 " + currentMessageNumber
                                            + " 条消息缺少 handler 字段（兼容旧格式时可提供 event 字段）");
                                }

                                StarBotEventHandler defaultHandler = handlerService.getHandler(eventClass, null)
                                        .orElseThrow(() -> new DataSourceException("数据源 JSON 旧格式迁移失败, UID "
                                                + user.getUid() + " 的目标 " + target.getNum() + " 第 "
                                                + currentMessageNumber + " 条消息事件 " + eventClass
                                                + " 没有可用的默认处理器"));
                                handlerClass = defaultHandler.getClass().getName();
                                log.warn("数据源 JSON 使用旧版 event 格式: {}, 已在内存中迁移为 handler: {}",
                                        eventClass, handlerClass);
                            }

                            PushMessage message = new PushMessage();
                            message.setTarget(target);
                            message.setHandler(handlerClass);
                            JSONObject params = messageObject.getJSONObject("params");
                            if (params != null) {
                                message.setParams(params.toJSONString());
                            }
                            if (!messageObject.containsKey("enabled")) {
                                message.setEnabled(true);
                            } else {
                                message.setEnabled(messageObject.getBoolean("enabled"));
                            }
                            target.getMessages().add(message);
                            messageIndex++;
                        }
                    }
                }
            }

            users.add(user);
        }

        return users;
    }
}
