package com.starlwr.bot.core.datasource;

import com.alibaba.fastjson2.JSONArray;

import java.time.Instant;
import java.util.List;

public interface JsonDataSourceManagementService {
    record Snapshot(String revision, JSONArray document) {}
    record Validation(boolean valid, String canonicalJson, List<String> errors) {}
    record ApplyResult(String revision, boolean changed, Instant appliedAt, List<String> warnings) {}

    Snapshot snapshot();
    Validation validate(JSONArray document);
    ApplyResult apply(JSONArray document, String expectedRevision);
}
