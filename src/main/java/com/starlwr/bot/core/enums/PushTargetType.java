package com.starlwr.bot.core.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Locale;

/**
 * 推送目标类型
 */
@Getter
@AllArgsConstructor
public enum PushTargetType {
    FRIEND(0, "好友"),
    GROUP(1, "群"),
    UNKNOWN(-1, "未知");

    private final int code;
    private final String str;

    public static PushTargetType of(int code) {
        for (PushTargetType pushTargetType : PushTargetType.values()) {
            if (pushTargetType.code == code) {
                return pushTargetType;
            }
        }

        return UNKNOWN;
    }

    /**
     * Parses the JSON data-source representation while retaining numeric compatibility.
     */
    public static PushTargetType fromConfigValue(Object value) {
        if (value instanceof Number number) {
            return of(number.intValue());
        }
        if (!(value instanceof String string)) {
            return UNKNOWN;
        }

        return switch (string.trim().toLowerCase(Locale.ROOT)) {
            case "0", "friend", "private", "user", "好友", "私聊" -> FRIEND;
            case "1", "group", "群", "群聊" -> GROUP;
            default -> UNKNOWN;
        };
    }
}
