package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSONObject;

import java.util.Optional;
import java.util.function.Consumer;

/** Stable latest-session events and platform metadata for one report. Close after all reads. */
public interface LiveDataReadSession extends AutoCloseable {
    void forEachEvent(String dataKey, Consumer<JSONObject> consumer);

    /** Metadata keys are relative to the platform, e.g. LiveStartTime, source UID. */
    <T> Optional<T> getCustomObject(Class<T> type, String... keys);

    @Override void close();
}
