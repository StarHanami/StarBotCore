package com.starlwr.bot.core.plugin.web;

import java.util.Set;

public final class PluginWebPermissions {
    public static final String DATASOURCE_JSON_READ = "core.datasource.json.read";
    public static final String DATASOURCE_JSON_VALIDATE = "core.datasource.json.validate";
    public static final String DATASOURCE_JSON_APPLY = "core.datasource.json.apply";
    public static final String EVENT_HANDLERS_DESCRIBE = "core.event-handlers.describe";

    public static final Set<String> KNOWN = Set.of(
            DATASOURCE_JSON_READ,
            DATASOURCE_JSON_VALIDATE,
            DATASOURCE_JSON_APPLY,
            EVENT_HANDLERS_DESCRIBE
    );

    private PluginWebPermissions() {
    }
}
