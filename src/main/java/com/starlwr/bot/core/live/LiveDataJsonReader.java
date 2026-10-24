package com.starlwr.bot.core.live;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import tools.jackson.core.*;
import tools.jackson.core.json.JsonFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/** Streams the legacy file into compact chunks, materializing at most one event at a time. */
final class LiveDataJsonReader {
    private static final Set<String> EVENT_KEYS = Set.of("Danmu", "Emoji", "FreeGift", "PaidGift", "RandomGift",
            "SuperChat", "Membership", "EnterRoom", "Follow", "Like", "Share");
    private static final JsonFactory FACTORY = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build()).build();

    static JSONObject read(InputStream input, CompactLiveEventStore events) throws IOException {
        try (JsonParser parser = FACTORY.createParser(input)) {
            JsonToken token = parser.nextToken();
            JSONObject metadata = new JSONObject();
            if (token == null) {
                // A blank live-data file is tolerated as empty data (aligned with upstream c1bb49b).
                return metadata;
            }
            if (token != JsonToken.VALUE_NULL) {
                expect(token, JsonToken.START_OBJECT);
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    expect(parser.currentToken(), JsonToken.PROPERTY_NAME);
                    String key = parser.currentName();
                    if (parser.nextToken() == JsonToken.START_OBJECT) metadata.put(key, platform(parser, key, events));
                    else metadata.put(key, value(parser));
                }
            }
            if (parser.nextToken() != null) throw new IOException("Trailing content after live data object");
            return metadata;
        }
    }

    private static JSONObject platform(JsonParser parser, String platform, CompactLiveEventStore events) throws IOException {
        JSONObject metadata = new JSONObject();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            expect(parser.currentToken(), JsonToken.PROPERTY_NAME);
            String key = parser.currentName(); parser.nextToken();
            if (EVENT_KEYS.contains(key)) eventSources(parser, platform, key, events);
            else metadata.put(key, value(parser));
        }
        return metadata;
    }

    private static void eventSources(JsonParser parser, String platform, String dataKey, CompactLiveEventStore events) throws IOException {
        if (parser.currentToken() == JsonToken.VALUE_NULL) return;
        expect(parser.currentToken(), JsonToken.START_OBJECT);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            expect(parser.currentToken(), JsonToken.PROPERTY_NAME);
            String uid = parser.currentName(); parser.nextToken();
            if (parser.currentToken() != JsonToken.START_OBJECT) { parser.skipChildren(); continue; }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                expect(parser.currentToken(), JsonToken.PROPERTY_NAME);
                String sender = parser.currentName(); parser.nextToken();
                if (parser.currentToken() != JsonToken.START_ARRAY) { parser.skipChildren(); continue; }
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == null) throw new IOException("Unfinished event array");
                    if (parser.currentToken() != JsonToken.START_OBJECT) { parser.skipChildren(); continue; }
                    JSONObject event = (JSONObject) value(parser);
                    if (event.get("sender") == null) event.put("sender", new JSONObject().fluentPut("uid", sender));
                    events.add(platform, uid, dataKey, event);
                }
            }
        }
    }

    private static Object value(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == null) throw new IOException("Unfinished live data value");
        return switch (token) {
            case START_OBJECT -> {
                JSONObject object = new JSONObject();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    expect(parser.currentToken(), JsonToken.PROPERTY_NAME);
                    String key = parser.currentName(); parser.nextToken(); object.put(key, value(parser));
                }
                yield object;
            }
            case START_ARRAY -> {
                JSONArray array = new JSONArray();
                while (parser.nextToken() != JsonToken.END_ARRAY) array.add(value(parser));
                yield array;
            }
            case VALUE_STRING -> parser.getString();
            case VALUE_NUMBER_INT -> parser.getNumberValue();
            case VALUE_NUMBER_FLOAT -> parser.getDecimalValue();
            case VALUE_TRUE -> true;
            case VALUE_FALSE -> false;
            case VALUE_NULL -> null;
            default -> throw new IOException("Unexpected live data token: " + token);
        };
    }
    private static void expect(JsonToken actual, JsonToken expected) throws IOException {
        if (actual != expected) throw new IOException("Expected " + expected + " in live data, got " + actual);
    }
}
