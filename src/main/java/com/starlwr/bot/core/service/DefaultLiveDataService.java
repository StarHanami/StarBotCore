package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import com.starlwr.bot.core.event.live.base.StarBotLiveUserEvent;
import com.starlwr.bot.core.event.live.common.*;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * 默认直播数据服务实现
 */
@Slf4j
@Service
public class DefaultLiveDataService implements LiveDataService {
    private final StarBotCoreProperties properties;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock(true);

    private final Lock cacheReadLock = cacheLock.readLock();

    private final Lock cacheWriteLock = cacheLock.writeLock();

    private final Object saveMonitor = new Object();

    private final AtomicBoolean closing = new AtomicBoolean();

    private JSONObject cache = new JSONObject();

    private static final List<String> EVENT_RECORD_KEYS = List.of(
            "Danmu", "Emoji", "FreeGift", "PaidGift", "RandomGift", "SuperChat",
            "Membership", "EnterRoom", "Follow", "Like", "Share"
    );

    @Autowired
    public DefaultLiveDataService(StarBotCoreProperties properties) {
        this.properties = properties;
    }

    /**
     * 加载直播数据
     */
    @Order(-10000)
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReadyEvent() {
        if (properties.getLive().isSaveLiveData()) {
            String liveDataPath = properties.getLive().getLiveDataPath();
            log.info("开始从 {} 中加载直播数据", liveDataPath);
            try {
                JSONObject loaded = normalizeCache(JSONObject.parseObject(
                        Files.readString(Path.of(liveDataPath), StandardCharsets.UTF_8)));
                writeCache(() -> cache = loaded);
            } catch (NoSuchFileException e) {
                log.warn("直播数据文件 {} 不存在, 建立新文件", liveDataPath);
            } catch (Exception e) {
                log.error("读取直播数据 {} 异常", liveDataPath, e);
            }
            log.info("直播数据加载完成");
            autoSave();
        }
    }

    /**
     * 保存直播数据
     */
    @Order(0)
    @EventListener(ContextClosedEvent.class)
    public void onContextClosedEvent() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();

        if (properties.getLive().isSaveLiveData()) {
            String liveDataPath = properties.getLive().getLiveDataPath();
            log.info("开始保存直播数据至 {}", liveDataPath);
            try {
                if (!saveCache(Path.of(liveDataPath), true, true)) {
                    log.info("直播数据为空，跳过保存至 {}", liveDataPath);
                    return;
                }
            } catch (Exception e) {
                log.error("保存直播数据至 {} 异常", liveDataPath, e);
                return;
            }
            log.info("直播数据已保存至 {}", liveDataPath);
        }
    }

    static JSONObject normalizeCache(JSONObject loaded) {
        JSONObject normalized = loaded == null ? new JSONObject() : loaded;
        List<String> prefixes = List.of("LiveStatus:", "LiveStartTime:", "LiveEndTime:");
        for (String key : new ArrayList<>(normalized.keySet())) {
            String prefix = prefixes.stream().filter(key::startsWith).findFirst().orElse(null);
            if (prefix == null) continue;
            String platform = key.substring(prefix.length());
            Object value = normalized.get(key);
            if (!(value instanceof JSONObject legacy) || platform.isBlank()) continue;
            JSONObject platformCache = (JSONObject) normalized.computeIfAbsent(platform, ignored -> new JSONObject());
            String section = prefix.substring(0, prefix.length() - 1);
            JSONObject target = (JSONObject) platformCache.computeIfAbsent(section, ignored -> new JSONObject());
            legacy.forEach(target::putIfAbsent);
            normalized.remove(key);
        }
        return normalized;
    }

    public void autoSave() {
        int interval = properties.getLive().getAutoSaveLiveDataInterval();
        Path path = Path.of(properties.getLive().getLiveDataPath());

        scheduler.scheduleWithFixedDelay(() -> {
            Thread.currentThread().setName("auto-save-data");

            try {
                saveCache(path, false, false);
            } catch (Exception e) {
                log.error("自动保存直播数据异常", e);
            }

        }, interval, interval, TimeUnit.SECONDS);
    }

    private boolean saveCache(Path path, boolean closingSave, boolean skipIfEmpty) throws Exception {
        synchronized (saveMonitor) {
            if (closing.get() && !closingSave) return false;
            String snapshot = readCache(() -> skipIfEmpty && cache.isEmpty() ? null : cache.toJSONString());
            if (snapshot == null) return false;
            writeAtomically(path, snapshot);
            return true;
        }
    }

    private void writeAtomically(Path path, String content) throws Exception {
        Path target = path.toAbsolutePath().normalize();
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);
        Path temp = Files.createTempFile(parent, ".starbot-live-data-", ".tmp");
        try {
            Files.writeString(temp, content, StandardCharsets.UTF_8);
            replaceWithRetry(temp, target);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private void replaceWithRetry(Path temp, Path target) throws Exception {
        IOException lastError = null;
        for (int attempt = 0; attempt < 6; attempt++) {
            try {
                try {
                    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException ignored) {
                    Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            } catch (IOException e) {
                lastError = e;
                if (attempt == 5) break;
                try {
                    Thread.sleep(10L << attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                }
            }
        }
        throw Objects.requireNonNull(lastError);
    }

    private <T> T readCache(Supplier<T> action) {
        cacheReadLock.lock();
        try {
            return action.get();
        } finally {
            cacheReadLock.unlock();
        }
    }

    private boolean writeCache(Runnable action) {
        cacheWriteLock.lock();
        try {
            if (closing.get()) return false;
            action.run();
            return true;
        } finally {
            cacheWriteLock.unlock();
        }
    }

    private <T> T writeCache(Supplier<T> action, T closedValue) {
        cacheWriteLock.lock();
        try {
            if (closing.get()) return closedValue;
            return action.get();
        } finally {
            cacheWriteLock.unlock();
        }
    }

    // ================ 直播间状态 ================

    /**
     * 设置直播间状态
     *
     * @param platform 直播平台
     * @param uid      UID
     * @param status   直播间状态
     */
    @Override
    public void setLiveStatus(@NonNull String platform, @NonNull Long uid, boolean status) {
        writeCache(() -> {
            JSONObject platformCache = (JSONObject) cache.computeIfAbsent(platform, k -> new JSONObject());
            JSONObject statusCache = (JSONObject) platformCache.computeIfAbsent("LiveStatus", k -> new JSONObject());
            statusCache.put(String.valueOf(uid), status);
        });
    }

    /**
     * 获取直播间状态
     *
     * @param platform 直播平台
     * @param uid      UID
     * @return 直播间状态
     */
    @Override
    public Optional<Boolean> getLiveStatus(@NonNull String platform, @NonNull Long uid) {
        return readCache(() -> Optional.ofNullable(cache.getJSONObject(platform))
                .map(platformCache -> platformCache.getJSONObject("LiveStatus"))
                .map(statusCache -> statusCache.getBoolean(String.valueOf(uid))));
    }

    // ================ 直播开始时间 ================

    /**
     * 设置最近一场直播开始时间戳
     *
     * @param platform  直播平台
     * @param uid       UID
     * @param startTime 最近一场直播开始时间戳
     */
    @Override
    public void setLiveStartTime(@NonNull String platform, @NonNull Long uid, long startTime) {
        writeCache(() -> {
            JSONObject platformCache = (JSONObject) cache.computeIfAbsent(platform, k -> new JSONObject());
            JSONObject timeCache = (JSONObject) platformCache.computeIfAbsent("LiveStartTime", k -> new JSONObject());
            timeCache.put(String.valueOf(uid), startTime);
        });
    }

    /**
     * 获取最近一场直播开始时间戳
     *
     * @param platform 直播平台
     * @param uid      UID
     * @return 最近一场直播开始时间戳
     */
    @Override
    public Optional<Long> getLiveStartTime(@NonNull String platform, @NonNull Long uid) {
        return readCache(() -> Optional.ofNullable(cache.getJSONObject(platform))
                .map(platformCache -> platformCache.getJSONObject("LiveStartTime"))
                .map(timeCache -> timeCache.getLong(String.valueOf(uid))));
    }

    // ================ 直播结束时间 ================

    /**
     * 设置最近一场直播结束时间戳
     *
     * @param platform 直播平台
     * @param uid      UID
     * @param endTime  最近一场直播结束时间戳
     */
    @Override
    public void setLiveEndTime(@NonNull String platform, @NonNull Long uid, long endTime) {
        writeCache(() -> {
            JSONObject platformCache = (JSONObject) cache.computeIfAbsent(platform, k -> new JSONObject());
            JSONObject timeCache = (JSONObject) platformCache.computeIfAbsent("LiveEndTime", k -> new JSONObject());
            timeCache.put(String.valueOf(uid), endTime);
        });
    }

    /**
     * 获取最近一场直播结束时间戳
     *
     * @param platform 直播平台
     * @param uid      UID
     * @return 最近一场直播结束时间戳
     */
    @Override
    public Optional<Long> getLiveEndTime(@NonNull String platform, @NonNull Long uid) {
        return readCache(() -> Optional.ofNullable(cache.getJSONObject(platform))
                .map(platformCache -> platformCache.getJSONObject("LiveEndTime"))
                .map(timeCache -> timeCache.getLong(String.valueOf(uid))));
    }

    /**
     * 删除最近一场直播结束时间戳
     *
     * @param platform 直播平台
     * @param uid      UID
     */
    @Override
    public void deleteLiveEndTime(@NonNull String platform, @NonNull Long uid) {
        writeCache(() -> Optional.ofNullable(cache.getJSONObject(platform))
                .map(platformCache -> platformCache.getJSONObject("LiveEndTime"))
                .ifPresent(endTimeCache -> endTimeCache.remove(String.valueOf(uid))));
    }

    // ================ 其他操作 ================

    /**
     * 重置最近一场直播数据
     *
     * @param platform 直播平台
     * @param uid      UID
     */
    @Override
    public void resetLiveData(@NonNull String platform, @NonNull Long uid) {
        writeCache(() -> {
            JSONObject platformCache = cache.getJSONObject(platform);
            if (platformCache == null) return;

            String uidString = String.valueOf(uid);
            for (String key : EVENT_RECORD_KEYS) {
                JSONObject recordCache = platformCache.getJSONObject(key);
                if (recordCache != null) recordCache.remove(uidString);
            }
        });
    }

    /**
     * 设置自定义对象
     *
     * @param value 对象
     * @param keys  多级键
     */
    @Override
    public void setCustomObject(@NonNull Object value, @NonNull String... keys) {
        Objects.requireNonNull(value, "自定义对象不能为空");
        validateKeys(keys);
        Object detached = JSON.parse(JSON.toJSONString(value));
        writeCache(() -> {
            JSONObject parent = locateParent(keys, true);
            if (parent == null) throw new IllegalStateException("无法定位自定义键路径: " + String.join(".", keys));
            parent.put(keys[keys.length - 1], detached);
        });
    }

    /**
     * 获取自定义对象
     *
     * @param type 目标类型
     * @param keys 多级键
     * @return 自定义对象
     */
    @Override
    public <T> Optional<T> getCustomObject(Class<T> type, @NonNull String... keys) {
        Objects.requireNonNull(type, "目标类型不能为空");
        validateKeys(keys);
        Optional<String> json = readCache(() -> {
            JSONObject parent = locateParent(keys, false);
            if (parent == null) return Optional.empty();
            Object raw = parent.get(keys[keys.length - 1]);
            return raw == null ? Optional.empty() : Optional.of(JSON.toJSONString(raw));
        });
        return json.map(value -> JSON.parseObject(value, type));
    }

    /**
     * 删除自定义对象
     *
     * @param keys 多级键
     * @return 是否删除成功
     */
    @Override
    public boolean deleteCustomObject(@NonNull String... keys) {
        validateKeys(keys);
        return writeCache(() -> {
            JSONObject parent = locateParent(keys, false);
            if (parent == null) return false;
            String key = keys[keys.length - 1];
            if (!parent.containsKey(key)) return false;
            parent.remove(key);
            return true;
        }, false);
    }

    /**
     * 定位多级键的父级 JSON 对象
     *
     * @param keys   多级键
     * @param create 中间层级不存在时是否创建
     * @return 父级 JSON 对象，中间层级不存在且 create 为 false 时返回 null
     */
    private JSONObject locateParent(@NonNull String[] keys, boolean create) {
        JSONObject current = cache;
        for (int i = 0; i < keys.length - 1; i++) {
            String key = keys[i];
            Object child = current.get(key);
            if (child instanceof JSONObject) {
                current = (JSONObject) child;
            } else if (create) {
                JSONObject created = new JSONObject();
                current.put(key, created);
                current = created;
            } else {
                return null;
            }
        }
        return current;
    }

    private void validateKeys(String[] keys) {
        if (keys == null || keys.length == 0) throw new IllegalArgumentException("自定义键路径不能为空");
        for (String key : keys) {
            if (key == null || key.isBlank()) throw new IllegalArgumentException("自定义键路径不能包含空键");
        }
    }

    // ================ 直播数据 ================

    /**
     * 添加事件记录到缓存
     *
     * @param event   事件
     * @param dataKey 事件类型键
     */
    private void addEventToCache(@NonNull StarBotLiveUserEvent event, @NonNull String dataKey) {
        String platform = event.getPlatform();
        String sourceUid = String.valueOf(event.getSource().getUid());
        String senderUid = String.valueOf(event.getSender().getUid());

        JSONObject eventJson = JSONObject.from(event);
        eventJson.remove("platform");
        eventJson.remove("source");
        eventJson.remove("stopped");

        writeCache(() -> {
            JSONObject platformCache = (JSONObject) cache.computeIfAbsent(platform, k -> new JSONObject());
            JSONObject dataCache = (JSONObject) platformCache.computeIfAbsent(dataKey, k -> new JSONObject());
            JSONObject sourceCache = (JSONObject) dataCache.computeIfAbsent(sourceUid, k -> new JSONObject());
            JSONArray senderCache = (JSONArray) sourceCache.computeIfAbsent(senderUid, k -> new JSONArray());
            senderCache.add(eventJson);
        });
    }

    /**
     * 获取事件记录列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param dataKey  事件类型键
     * @param type     目标类型
     * @return 事件记录列表
     */
    private <T> List<T> getEvents(@NonNull String platform, @NonNull Long uid, @NonNull String dataKey, @NonNull Class<T> type) {
        Objects.requireNonNull(type, "目标类型不能为空");

        List<String> snapshots = readCache(() -> {
            List<JSONObject> merged = new ArrayList<>();
            Optional.ofNullable(cache.getJSONObject(platform))
                    .map(platformCache -> platformCache.getJSONObject(dataKey))
                    .map(dataCache -> dataCache.getJSONObject(String.valueOf(uid)))
                    .ifPresent(sourceCache -> {
                        for (Map.Entry<String, Object> entry : sourceCache.entrySet()) {
                            if (entry.getValue() instanceof JSONArray array) {
                                for (int i = 0; i < array.size(); i++) {
                                    JSONObject event = detachedEvent(array.getJSONObject(i), entry.getKey());
                                    merged.add(event);
                                }
                            }
                        }
                    });
            merged.sort(Comparator.comparingLong(json -> json.getLongValue("timestamp")));
            return merged.stream().map(event -> event.toJSONString()).toList();
        });
        return snapshots.stream().map(json -> JSON.parseObject(json, type)).toList();
    }

    private Long parseSenderUid(String sender) {
        try {
            return Long.valueOf(sender);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /**
     * 获取指定观众的事件记录列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param dataKey   事件类型键
     * @param type      目标类型
     * @return 事件记录列表
     */
    private <T> List<T> getUserEvents(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull String dataKey, @NonNull Class<T> type) {
        Objects.requireNonNull(type, "目标类型不能为空");

        List<String> snapshots = readCache(() -> {
            List<String> result = new ArrayList<>();
            Optional.ofNullable(cache.getJSONObject(platform))
                    .map(platformCache -> platformCache.getJSONObject(dataKey))
                    .map(dataCache -> dataCache.getJSONObject(String.valueOf(uid)))
                    .map(sourceCache -> sourceCache.getJSONArray(String.valueOf(senderUid)))
                    .ifPresent(array -> {
                        for (int i = 0; i < array.size(); i++) {
                            result.add(detachedEvent(array.getJSONObject(i), String.valueOf(senderUid)).toJSONString());
                        }
                    });
            return result;
        });
        return snapshots.stream().map(json -> JSON.parseObject(json, type)).toList();
    }

    private JSONObject detachedEvent(JSONObject stored, String senderUid) {
        JSONObject event = stored == null ? new JSONObject() : JSONObject.parseObject(stored.toJSONString());
        if (!(event.get("sender") instanceof JSONObject)) {
            event.put("sender", new JSONObject().fluentPut("uid", parseSenderUid(senderUid)));
        }
        return event;
    }

    /**
     * 添加弹幕记录
     *
     * @param event 弹幕事件
     */
    @Override
    public void addDanmu(@NonNull DanmuEvent event) {
        addEventToCache(event, "Danmu");
    }

    /**
     * 获取最近一场直播弹幕列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播弹幕列表
     */
    @Override
    public <T> List<T> getDanmu(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Danmu", type);
    }

    /**
     * 获取最近一场直播指定观众的弹幕列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的弹幕列表
     */
    @Override
    public <T> List<T> getUserDanmu(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Danmu", type);
    }

    /**
     * 添加表情弹幕记录
     *
     * @param event 表情事件
     */
    @Override
    public void addEmoji(@NonNull EmojiEvent event) {
        addEventToCache(event, "Emoji");
    }

    /**
     * 获取最近一场直播表情弹幕列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播表情弹幕列表
     */
    @Override
    public <T> List<T> getEmoji(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Emoji", type);
    }

    /**
     * 获取最近一场直播指定观众的表情弹幕列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的表情弹幕列表
     */
    @Override
    public <T> List<T> getUserEmoji(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Emoji", type);
    }

    /**
     * 添加免费礼物记录
     *
     * @param event 免费礼物事件
     */
    @Override
    public void addFreeGift(@NonNull FreeGiftEvent event) {
        addEventToCache(event, "FreeGift");
    }

    /**
     * 获取最近一场直播免费礼物列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播免费礼物列表
     */
    @Override
    public <T> List<T> getFreeGift(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "FreeGift", type);
    }

    /**
     * 获取最近一场直播指定观众的免费礼物列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的免费礼物列表
     */
    @Override
    public <T> List<T> getUserFreeGift(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "FreeGift", type);
    }

    /**
     * 添加付费礼物记录
     *
     * @param event 付费礼物事件
     */
    @Override
    public void addPaidGift(@NonNull PaidGiftEvent event) {
        addEventToCache(event, "PaidGift");
    }

    /**
     * 获取最近一场直播付费礼物列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播付费礼物列表
     */
    @Override
    public <T> List<T> getPaidGift(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "PaidGift", type);
    }

    /**
     * 获取最近一场直播指定观众的付费礼物列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的付费礼物列表
     */
    @Override
    public <T> List<T> getUserPaidGift(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "PaidGift", type);
    }

    /**
     * 添加随机礼物记录
     *
     * @param event 随机礼物事件
     */
    @Override
    public void addRandomGift(@NonNull RandomGiftEvent event) {
        addEventToCache(event, "RandomGift");
    }

    /**
     * 获取最近一场直播随机礼物列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播随机礼物列表
     */
    @Override
    public <T> List<T> getRandomGift(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "RandomGift", type);
    }

    /**
     * 获取最近一场直播指定观众的随机礼物列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的随机礼物列表
     */
    @Override
    public <T> List<T> getUserRandomGift(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "RandomGift", type);
    }

    /**
     * 添加醒目留言记录
     *
     * @param event 醒目留言事件
     */
    @Override
    public void addSuperChat(@NonNull SuperChatEvent event) {
        addEventToCache(event, "SuperChat");
    }

    /**
     * 获取最近一场直播醒目留言列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播醒目留言列表
     */
    @Override
    public <T> List<T> getSuperChat(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "SuperChat", type);
    }

    /**
     * 获取最近一场直播指定观众的醒目留言列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的醒目留言列表
     */
    @Override
    public <T> List<T> getUserSuperChat(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "SuperChat", type);
    }

    /**
     * 添加开通会员记录
     *
     * @param event 开通会员事件
     */
    @Override
    public void addMemberShip(@NonNull MembershipEvent event) {
        addEventToCache(event, "Membership");
    }

    /**
     * 获取最近一场直播开通会员列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播开通会员列表
     */
    @Override
    public <T> List<T> getMemberShip(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Membership", type);
    }

    /**
     * 获取最近一场直播指定观众的开通会员列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的开通会员列表
     */
    @Override
    public <T> List<T> getUserMemberShip(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Membership", type);
    }

    /**
     * 添加进入房间记录
     *
     * @param event 进入房间事件
     */
    @Override
    public void addEnterRoom(@NonNull EnterRoomEvent event) {
        addEventToCache(event, "EnterRoom");
    }

    /**
     * 获取最近一场直播进入房间记录列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播进入房间记录列表
     */
    @Override
    public <T> List<T> getEnterRoom(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "EnterRoom", type);
    }

    /**
     * 获取最近一场直播指定观众的进入房间记录列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的进入房间记录列表
     */
    @Override
    public <T> List<T> getUserEnterRoom(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "EnterRoom", type);
    }

    /**
     * 添加关注记录
     *
     * @param event 关注事件
     */
    @Override
    public void addFollow(@NonNull FollowEvent event) {
        addEventToCache(event, "Follow");
    }

    /**
     * 获取最近一场直播关注记录列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播关注记录列表
     */
    @Override
    public <T> List<T> getFollow(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Follow", type);
    }

    /**
     * 获取最近一场直播指定观众的关注记录列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的关注记录列表
     */
    @Override
    public <T> List<T> getUserFollow(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Follow", type);
    }

    /**
     * 添加点赞记录
     *
     * @param event 点赞事件
     */
    @Override
    public void addLike(@NonNull LikeEvent event) {
        addEventToCache(event, "Like");
    }

    /**
     * 获取最近一场直播点赞记录列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播点赞记录列表
     */
    @Override
    public <T> List<T> getLike(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Like", type);
    }

    /**
     * 获取最近一场直播指定观众的点赞记录列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的点赞记录列表
     */
    @Override
    public <T> List<T> getUserLike(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Like", type);
    }

    /**
     * 添加分享记录
     *
     * @param event 分享事件
     */
    @Override
    public void addShare(@NonNull ShareEvent event) {
        addEventToCache(event, "Share");
    }

    /**
     * 获取最近一场直播分享记录列表
     *
     * @param platform 直播平台
     * @param uid      主播 UID
     * @param type     目标类型
     * @return 最近一场直播分享记录列表
     */
    @Override
    public <T> List<T> getShare(@NonNull String platform, @NonNull Long uid, @NonNull Class<T> type) {
        return getEvents(platform, uid, "Share", type);
    }

    /**
     * 获取最近一场直播指定观众的分享记录列表
     *
     * @param platform  直播平台
     * @param uid       主播 UID
     * @param senderUid 观众 UID
     * @param type      目标类型
     * @return 最近一场直播指定观众的分享记录列表
     */
    @Override
    public <T> List<T> getUserShare(@NonNull String platform, @NonNull Long uid, @NonNull Long senderUid, @NonNull Class<T> type) {
        return getUserEvents(platform, uid, senderUid, "Share", type);
    }
}
