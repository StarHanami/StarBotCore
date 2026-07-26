package com.starlwr.bot.core.plugin.web;

import com.starlwr.bot.core.plugin.StarBotPluginLoader;
import org.springframework.boot.webmvc.autoconfigure.WebMvcRegistrations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@Configuration
public class PluginWebMvcConfig {
    @Bean
    public WebMvcRegistrations pluginWebMvcRegistrations(StarBotPluginLoader pluginLoader) {
        return new WebMvcRegistrations() {
            @Override
            public RequestMappingHandlerMapping getRequestMappingHandlerMapping() {
                return new PluginRequestMappingHandlerMapping(pluginLoader);
            }
        };
    }
}
