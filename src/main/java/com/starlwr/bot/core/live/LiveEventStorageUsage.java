package com.starlwr.bot.core.live;

/** Retained allocations; indexBytes is a conservative estimate, not a JVM heap measurement. */
public record LiveEventStorageUsage(long heapBytes, long nativeBytes, long indexBytes,
                                   long payloadBytes, long eventCount, long sessionCount, int sourceCount) {
    public long estimatedBytes() { return heapBytes + nativeBytes + indexBytes; }

    public static LiveEventStorageUsage empty() {
        return new LiveEventStorageUsage(0, 0, 0, 0, 0, 0, 0);
    }
}
