package com.starlwr.bot.core.service;

import com.starlwr.bot.core.handler.DefaultHandlerForEvent;
import com.starlwr.bot.core.handler.StarBotEventHandler;
import com.starlwr.bot.core.util.StringUtil;
import jakarta.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;

/**
 * StarBot 事件处理器服务
 */
@Slf4j
@Service
public class StarBotEventHandlerService {
    private final ApplicationContext applicationContext;

    private final Map<String, StarBotEventHandler> cache = new HashMap<>();

    private final Map<String, StarBotEventHandler> defaultHandlers = new HashMap<>();

    @Autowired
    public StarBotEventHandlerService(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * 加载事件处理器
     */
    @Order(0)
    @EventListener(ContextRefreshedEvent.class)
    public void onContextRefreshedEvent() {
        cache.clear();
        defaultHandlers.clear();
        for (StarBotEventHandler handler : applicationContext.getBeansOfType(StarBotEventHandler.class).values()) {
            cache.put(handler.getClass().getName(), handler);

            DefaultHandlerForEvent annotation = handler.getClass().getAnnotation(DefaultHandlerForEvent.class);
            if (annotation != null) {
                defaultHandlers.putIfAbsent(annotation.event(), handler);
            }
        }
    }

    /**
     * 获取事件处理器
     * @param handlerClass 处理器全类名
     * @return 事件处理器
     */
    public Optional<StarBotEventHandler> getHandler(@NonNull String handlerClass) {
        return Optional.ofNullable(cache.get(handlerClass));
    }

    /**
     * Resolves a configured handler, falling back to the legacy event default.
     *
     * @param eventClass legacy event class name
     * @param handlerClass configured handler class name
     * @return resolved handler
     */
    public Optional<StarBotEventHandler> getHandler(@NonNull String eventClass, @Nullable String handlerClass) {
        if (StringUtil.isNotBlank(handlerClass)) {
            return getHandler(handlerClass);
        }
        return Optional.ofNullable(defaultHandlers.get(eventClass));
    }

    public List<StarBotEventHandler> getHandlers() {
        return List.copyOf(cache.values());
    }
}
