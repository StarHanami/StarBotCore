package com.starlwr.bot.core.plugin.web;

import com.starlwr.bot.core.plugin.StarBotPlugin;
import com.starlwr.bot.core.plugin.StarBotPluginLoader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class PluginRequestMappingHandlerMapping extends RequestMappingHandlerMapping {
    private final StarBotPluginLoader pluginLoader;

    public PluginRequestMappingHandlerMapping(StarBotPluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    @Override
    protected RequestMappingInfo getMappingForMethod(Method method, Class<?> handlerType) {
        RequestMappingInfo mapping = super.getMappingForMethod(method, handlerType);
        StarBotPluginWebApi annotation = AnnotatedElementUtils.findMergedAnnotation(handlerType, StarBotPluginWebApi.class);
        if (mapping == null || annotation == null) {
            return mapping;
        }

        StarBotPlugin plugin = pluginLoader.findPluginByComponent(handlerType).orElse(null);
        if (plugin == null || plugin.getWebManifest() == null) {
            log.error("插件 Web API 未注册，因为无法解析有效的 starbot-web.json: {}", handlerType.getName());
            return null;
        }

        Set<String> declared = new HashSet<>(plugin.getWebManifest().getPermissions());
        if (!declared.containsAll(Arrays.asList(annotation.permissions()))) {
            log.error("插件 Web API 未注册，因为权限声明不完整: plugin={}, controller={}, required={}, declared={}",
                    plugin.getId(), handlerType.getName(), Arrays.toString(annotation.permissions()), declared);
            return null;
        }

        RequestMappingInfo prefix = RequestMappingInfo.paths("/plugins/" + plugin.getWebManifest().getAlias())
                .options(getBuilderConfiguration())
                .build();
        return prefix.combine(mapping);
    }
}
