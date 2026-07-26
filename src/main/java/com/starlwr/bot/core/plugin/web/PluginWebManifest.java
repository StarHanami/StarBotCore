package com.starlwr.bot.core.plugin.web;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class PluginWebManifest {
    private String alias;

    private String webRoot = "web";

    private List<String> permissions = new ArrayList<>();
}
