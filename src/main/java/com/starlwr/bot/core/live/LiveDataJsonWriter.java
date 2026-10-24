package com.starlwr.bot.core.live;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/** Writes containers incrementally, without a document-sized JSONWriter buffer or String. */
final class LiveDataJsonWriter {
    private final Writer output;

    LiveDataJsonWriter(Writer output) { this.output = output; }

    void raw(char value) throws IOException { output.write(value); }

    void string(String value) throws IOException {
        output.write('"');
        int start = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            String escaped = switch (c) {
                case '"' -> "\\\"";
                case '\\' -> "\\\\";
                case '\b' -> "\\b";
                case '\f' -> "\\f";
                case '\n' -> "\\n";
                case '\r' -> "\\r";
                case '\t' -> "\\t";
                default -> null;
            };
            if (escaped == null && c >= 0x20) continue;
            output.write(value, start, i - start);
            if (escaped != null) output.write(escaped);
            else {
                output.write("\\u00");
                output.write(Character.forDigit(c >> 4, 16));
                output.write(Character.forDigit(c & 15, 16));
            }
            start = i + 1;
        }
        output.write(value, start, value.length() - start);
        output.write('"');
    }

    void value(Object value) throws IOException {
        if (value == null) output.write("null");
        else if (value instanceof String text) string(text);
        else if (value instanceof Map<?, ?> map) {
            raw('{');
            boolean comma = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (comma) raw(',');
                string(String.valueOf(entry.getKey())); raw(':'); value(entry.getValue());
                comma = true;
            }
            raw('}');
        } else if (value instanceof Collection<?> values) {
            raw('[');
            boolean comma = false;
            for (Object item : values) {
                if (comma) raw(',');
                value(item); comma = true;
            }
            raw(']');
        } else if (value instanceof Boolean || value instanceof Number) {
            if (value instanceof Double d && !Double.isFinite(d)
                    || value instanceof Float f && !Float.isFinite(f)) output.write("null");
            else output.write(value.toString());
        } else output.write(JSON.toJSONString(value));
    }

    /** Cache values have already been normalized to JSON; copy containers, share immutable scalars. */
    static Object detached(Object value) {
        if (value instanceof Map<?, ?> map) {
            JSONObject copy = new JSONObject(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) copy.put(String.valueOf(entry.getKey()), detached(entry.getValue()));
            return copy;
        }
        if (value instanceof Collection<?> values) {
            List<Object> copy = new ArrayList<>(values.size());
            for (Object item : values) copy.add(detached(item));
            return copy;
        }
        return value;
    }
}
