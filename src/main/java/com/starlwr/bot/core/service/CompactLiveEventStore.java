package com.starlwr.bot.core.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.starlwr.bot.core.config.StarBotCoreProperties;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/** Latest-session storage. Readers pin a stable record index until they finish. */
final class CompactLiveEventStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CompactLiveEventStore.class);
    private static final int CHUNK_BYTES = 1 << 20;
    private static final List<String> EVENT_KEYS = List.of(
            "Danmu", "Emoji", "FreeGift", "PaidGift", "RandomGift", "SuperChat",
            "Membership", "EnterRoom", "Follow", "Like", "Share");
    private final Map<String, SourceStore> stores = new ConcurrentHashMap<>();
    private final Set<Session> retired = ConcurrentHashMap.newKeySet();
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final ChunkBackendFactory backendFactory;
    private final ColdSegmentCodec codec;
    private final long budgetBytes;
    private final AtomicBoolean budgetWarningLogged = new AtomicBoolean();
    private final AtomicLong heapBytes = new AtomicLong(), nativeBytes = new AtomicLong(), indexBytes = new AtomicLong();
    private final AtomicLong payloadBytes = new AtomicLong(), eventCount = new AtomicLong(), sessionCount = new AtomicLong();
    private boolean closed;

    CompactLiveEventStore(StarBotCoreProperties.Live properties) {
        backendFactory = ChunkBackendFactory.create(properties.getMemoryBackend());
        codec = ColdSegmentCodec.create(properties.getColdCompression(), properties.getColdCompressionLevel(),
                Math.max(0L, properties.getColdCompressionThresholdBytes()));
        budgetBytes = Math.max(0L, properties.getMemoryBudgetBytes());
        log.info("直播事件紧凑存储已启用: backend={}, coldCompression={}, budgetBytes={}, retention=latest-session",
                backendFactory.name(), codec.name(), budgetBytes);
    }

    void add(@NonNull String platform, @NonNull Long uid, @NonNull String dataKey, JSONObject event) {
        if (!EVENT_KEYS.contains(dataKey)) return;
        lifecycle.readLock().lock();
        try {
            if (closed) return;
            source(platform, uid).add(dataKey, event);
            long used = estimatedBytes();
            if (budgetBytes > 0 && used > budgetBytes) {
                if (budgetWarningLogged.compareAndSet(false, true))
                    log.warn("直播事件存储超过预算（当前场次不会被丢弃）: estimatedBytes={}, budgetBytes={}, stores={}", used, budgetBytes, stores.size());
            } else budgetWarningLogged.set(false);
        } finally { lifecycle.readLock().unlock(); }
    }

    private SourceStore source(String platform, long uid) {
        return stores.computeIfAbsent(key(platform, uid), ignored -> new SourceStore(platform, uid));
    }

    void importLegacy(JSONObject loaded) {
        if (loaded == null) return;
        for (Map.Entry<String, Object> platformEntry : loaded.entrySet()) {
            if (!(platformEntry.getValue() instanceof JSONObject platform)) continue;
            for (String dataKey : EVENT_KEYS) {
                JSONObject data = platform.getJSONObject(dataKey);
                if (data == null) continue;
                for (Map.Entry<String, Object> sourceEntry : data.entrySet()) {
                    Long uid = parseLong(sourceEntry.getKey());
                    if (uid == null || !(sourceEntry.getValue() instanceof JSONObject bySender)) continue;
                    for (Map.Entry<String, Object> senderEntry : bySender.entrySet()) {
                        if (!(senderEntry.getValue() instanceof JSONArray events)) continue;
                        for (Object event : events) {
                            if (!(event instanceof JSONObject json)) continue;
                            if (json.get("sender") == null) {
                                json = new JSONObject(json);
                                json.put("sender", new JSONObject().fluentPut("uid", parseLong(senderEntry.getKey())));
                            }
                            add(platformEntry.getKey(), uid, dataKey, json);
                        }
                    }
                }
            }
            EVENT_KEYS.forEach(platform::remove);
        }
    }

    List<JSONObject> read(@NonNull String platform, @NonNull Long uid, @NonNull String dataKey) {
        List<JSONObject> result = new ArrayList<>();
        forEach(platform, uid, dataKey, result::add);
        return result;
    }

    void forEach(@NonNull String platform, @NonNull Long uid, @NonNull String dataKey,
                 @NonNull Consumer<JSONObject> consumer) {
        ReadView view = openView(platform, uid, dataKey);
        if (view == null) return;
        try (ReadView reading = view) { reading.forEachEvent(dataKey, consumer); }
    }

    ReadView openView(String platform, long uid, String dataKey) {
        lifecycle.readLock().lock();
        try {
            SourceStore source = stores.get(key(platform, uid));
            return !closed && source != null ? source.openView(dataKey) : null;
        } finally { lifecycle.readLock().unlock(); }
    }

    void reset(@NonNull String platform, @NonNull Long uid) {
        lifecycle.readLock().lock();
        try { if (!closed) source(platform, uid).reset(); }
        finally { lifecycle.readLock().unlock(); }
    }

    void deactivate(@NonNull String platform, @NonNull Long uid, String reason) {
        lifecycle.readLock().lock();
        try {
            SourceStore source = stores.get(key(platform, uid));
            if (!closed && source != null) source.deactivate();
        } finally { lifecycle.readLock().unlock(); }
    }

    Snapshot snapshot() {
        Snapshot snapshot = new Snapshot();
        lifecycle.readLock().lock();
        try {
            if (closed) return snapshot;
            for (SourceStore source : stores.values()) {
                ReadView view = source.openView(null);
                if (view != null) snapshot.sources.add(new SourceView(source.platform, source.uid, view));
            }
            return snapshot;
        } catch (Throwable failure) {
            try { snapshot.close(); } catch (Throwable cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        } finally { lifecycle.readLock().unlock(); }
    }

    long estimatedBytes() { return heapBytes.get() + nativeBytes.get() + indexBytes.get(); }
    int storeCount() { return stores.size(); }
    LiveEventStorageUsage usage() {
        return new LiveEventStorageUsage(heapBytes.get(), nativeBytes.get(), indexBytes.get(),
                payloadBytes.get(), eventCount.get(), sessionCount.get(), stores.size());
    }
    private static String key(String platform, long uid) { return platform + '\u0000' + uid; }
    private static Long parseLong(String value) {
        try { return Long.valueOf(value); } catch (NumberFormatException ignored) { return null; }
    }

    @Override public void close() {
        lifecycle.writeLock().lock();
        try {
            RuntimeException failure = null;
            if (!closed) {
                closed = true;
                for (SourceStore source : stores.values()) try { source.close(); }
                catch (RuntimeException error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
                indexBytes.addAndGet(-128L * stores.size());
                stores.clear();
            }
            for (Session session : retired.toArray(new Session[0])) try { session.retryClose(); }
            catch (RuntimeException error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
            if (failure != null) throw failure;
        } finally { lifecycle.writeLock().unlock(); }
    }

    private final class SourceStore {
        private final String platform;
        private final long uid;
        private Session latest;
        SourceStore(String platform, long uid) {
            this.platform = platform; this.uid = uid; indexBytes.addAndGet(128);
        }
        synchronized void add(String dataKey, JSONObject event) {
            if (latest == null || latest.sealed) reset();
            latest.add(dataKey, event);
        }
        synchronized void reset() {
            Session next = new Session(backendFactory.create());
            Session previous = latest;
            latest = next;
            if (previous != null) previous.retire();
        }
        synchronized void deactivate() { if (latest != null) latest.seal(); }
        synchronized ReadView openView(String dataKey) { return latest == null ? null : latest.openView(dataKey); }
        synchronized void close() {
            try { if (latest != null) latest.retire(); }
            finally { latest = null; }
        }
    }

    private final class Session {
        private ChunkBackend backend;
        private final Map<String, Series> series = new LinkedHashMap<>();
        private final ArrayList<Record> records = new ArrayList<>();
        private final Map<String, Integer> senderIds = new HashMap<>();
        private final ArrayList<byte[]> senders = new ArrayList<>();
        private long payloadSize, metadataSize = 256;
        private long countedHeap, countedNative, countedMetadata, countedPayload, countedEvents;
        private int readers;
        private boolean sealed, retiring, disposed;
        Session(ChunkBackend backend) {
            this.backend = backend; sessionCount.incrementAndGet(); account();
        }
        synchronized void add(String dataKey, JSONObject original) {
            if (sealed || retiring || disposed) throw new IllegalStateException("event session is closed");
            try {
                JSONObject event = new JSONObject(original);
                int senderId = senderId(event.get("sender"));
                if (senderId >= 0) event.put("sender", Integer.toString(senderId));
                byte[] payload = JSON.toJSONBytes(event);
                ChunkSlice slice = backend.append(payload);
                Record record = new Record(event.getLongValue("timestamp"), senderId, slice);
                records.add(record);
                Series target = series.get(dataKey);
                if (target == null) { target = new Series(); series.put(dataKey, target); metadataSize += 96; }
                target.records.add(record);
                payloadSize += payload.length; metadataSize += 72;
            } finally { account(); }
        }
        private int senderId(Object sender) {
            if (sender == null) return -1;
            // JSONObject.from(event) may retain a typed UserInfo rather than a nested JSONObject.
            String fingerprint = JSON.toJSONString(sender);
            if (fingerprint.isEmpty() || fingerprint.charAt(0) != '{') return -1;
            Integer existing = senderIds.get(fingerprint);
            if (existing != null) return existing;
            int id = senders.size();
            byte[] bytes = fingerprint.getBytes(StandardCharsets.UTF_8);
            senderIds.put(fingerprint, id); senders.add(bytes);
            metadataSize += 128L + bytes.length + 2L * fingerprint.length();
            return id;
        }
        synchronized ReadView openView(String dataKey) {
            if (disposed) return null;
            Map<String, List<Record>> copy = new LinkedHashMap<>();
            for (Map.Entry<String, Series> entry : series.entrySet()) {
                if (dataKey == null || entry.getKey().equals(dataKey))
                    copy.put(entry.getKey(), new ArrayList<>(entry.getValue().records));
            }
            ReadView view = new ReadView(this, copy, new ArrayList<>(senders));
            readers++;
            return view;
        }
        synchronized void seal() {
            if (disposed || sealed) return;
            sealed = true;
            if (readers == 0) archive();
        }
        private void archive() {
            if (backend instanceof ColdChunkBackend || disposed) return;
            // Build a replacement before releasing the arena; slices keep their original chunk offsets.
            ChunkBackend replacement = new ColdChunkBackend(backend, codec, payloadSize >= codec.threshold());
            backend.close(); backend = replacement; account();
        }
        synchronized void retire() {
            if (disposed) return;
            retiring = true; retired.add(this); retryClose();
        }
        synchronized void retryClose() {
            if (!retiring || readers != 0 || disposed) return;
            backend.close();
            disposed = true; payloadSize = 0; metadataSize = 0;
            records.clear(); records.trimToSize(); series.clear(); senderIds.clear(); senders.clear(); senders.trimToSize();
            account(); sessionCount.decrementAndGet(); retired.remove(this);
        }
        synchronized void release() {
            readers--;
            if (retiring) retryClose();
            else if (sealed && readers == 0) archive();
        }
        private void account() {
            long heap = disposed ? 0 : backend.heapBytes(), nativeMemory = disposed ? 0 : backend.nativeBytes();
            long events = disposed ? 0 : records.size();
            heapBytes.addAndGet(heap - countedHeap); nativeBytes.addAndGet(nativeMemory - countedNative);
            long indexes = disposed ? 0 : metadataSize + 64L * backend.chunkCount();
            indexBytes.addAndGet(indexes - countedMetadata); payloadBytes.addAndGet(payloadSize - countedPayload);
            eventCount.addAndGet(events - countedEvents);
            countedHeap = heap; countedNative = nativeMemory; countedMetadata = indexes;
            countedPayload = payloadSize; countedEvents = events;
        }
        synchronized byte[] read(ChunkSlice slice, ReadView reader) {
            if (backend instanceof ColdChunkBackend cold) {
                if (reader.cachedChunk != slice.chunk) {
                    reader.cachedBytes = cold.copyChunk(slice.chunk); reader.cachedChunk = slice.chunk;
                }
                return Arrays.copyOfRange(reader.cachedBytes, slice.offset, slice.offset + slice.length);
            }
            return backend.read(slice);
        }
    }

    final class ReadView implements AutoCloseable {
        private final Session owner;
        private final Map<String, List<Record>> series;
        private final List<byte[]> senders;
        private byte[] cachedBytes;
        private int cachedChunk = -1;
        private boolean released;
        ReadView(Session owner, Map<String, List<Record>> series, List<byte[]> senders) {
            this.owner = owner; this.series = series; this.senders = senders;
        }
        JSONObject materialize(Record record) {
            JSONObject event = JSON.parseObject(owner.read(record.slice, this));
            if (record.senderId >= 0) event.put("sender", JSON.parseObject(senders.get(record.senderId)));
            return event;
        }
        void forEachEvent(String dataKey, Consumer<JSONObject> consumer) {
            if (released) throw new IllegalStateException("event read view is closed");
            List<Record> values = series.get(dataKey);
            if (values == null) return;
            values.sort(Comparator.comparingLong(record -> record.timestamp));
            for (Record record : values) consumer.accept(materialize(record));
        }
        void writeBySender(LiveDataJsonWriter writer, String dataKey) throws IOException {
            Map<String, List<Record>> bySender = new LinkedHashMap<>();
            Map<Integer, String> senderKeys = new HashMap<>();
            for (Record record : series.get(dataKey)) {
                String key = senderKeys.computeIfAbsent(record.senderId, id -> {
                    if (id < 0) return "0";
                    String uid = JSON.parseObject(senders.get(id)).getString("uid");
                    return uid == null ? "0" : uid;
                });
                bySender.computeIfAbsent(key, ignored -> new ArrayList<>()).add(record);
            }
            writer.raw('{'); boolean comma = false;
            for (Map.Entry<String, List<Record>> entry : bySender.entrySet()) {
                if (comma) writer.raw(',');
                writer.string(entry.getKey()); writer.raw(':'); writer.raw('[');
                boolean eventComma = false;
                for (Record record : entry.getValue()) {
                    if (eventComma) writer.raw(',');
                    writer.value(materialize(record)); eventComma = true;
                }
                writer.raw(']'); comma = true;
            }
            writer.raw('}');
        }
        @Override public void close() {
            if (released) return;
            released = true;
            series.clear(); senders.clear(); cachedBytes = null;
            owner.release();
        }
    }

    private record SourceView(String platform, long uid, ReadView view) { }
    static final class Snapshot implements AutoCloseable {
        private final List<SourceView> sources = new ArrayList<>();
        boolean isEmpty() {
            for (SourceView source : sources) for (List<Record> values : source.view.series.values())
                if (!values.isEmpty()) return false;
            return true;
        }
        void write(Writer output, JSONObject metadata) throws IOException {
            LiveDataJsonWriter writer = new LiveDataJsonWriter(output);
            Map<String, List<SourceView>> platforms = new LinkedHashMap<>();
            for (SourceView source : sources) platforms.computeIfAbsent(source.platform, ignored -> new ArrayList<>()).add(source);
            Set<String> keys = new LinkedHashSet<>(metadata.keySet()); keys.addAll(platforms.keySet());
            writer.raw('{'); boolean comma = false;
            for (String key : keys) {
                if (comma) writer.raw(','); writer.string(key); writer.raw(':');
                if (!platforms.containsKey(key)) writer.value(metadata.get(key));
                else writePlatform(writer, metadata.get(key), platforms.get(key));
                comma = true;
            }
            writer.raw('}');
        }
        private void writePlatform(LiveDataJsonWriter writer, Object metadata, List<SourceView> sources) throws IOException {
            if (metadata != null && !(metadata instanceof Map<?, ?>)) throw new IOException("Live platform metadata must be an object");
            writer.raw('{'); boolean comma = false;
            if (metadata instanceof Map<?, ?> map) for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = String.valueOf(entry.getKey()); if (EVENT_KEYS.contains(key)) continue;
                if (comma) writer.raw(','); writer.string(key); writer.raw(':'); writer.value(entry.getValue()); comma = true;
            }
            for (String key : EVENT_KEYS) {
                boolean found = false;
                for (SourceView source : sources) if (source.view.series.containsKey(key)) { found = true; break; }
                if (!found) continue;
                if (comma) writer.raw(','); writer.string(key); writer.raw(':'); writer.raw('{');
                boolean sourceComma = false;
                for (SourceView source : sources) {
                    if (!source.view.series.containsKey(key)) continue;
                    if (sourceComma) writer.raw(','); writer.string(Long.toString(source.uid)); writer.raw(':');
                    source.view.writeBySender(writer, key); sourceComma = true;
                }
                writer.raw('}'); comma = true;
            }
            writer.raw('}');
        }
        @Override public void close() {
            Throwable failure = null;
            for (SourceView source : sources) try { source.view.close(); }
            catch (Throwable error) { if (failure == null) failure = error; else failure.addSuppressed(error); }
            sources.clear();
            if (failure instanceof RuntimeException error) throw error;
            if (failure instanceof Error error) throw error;
        }
    }

    private static final class Series { private final List<Record> records = new ArrayList<>(); }
    private record Record(long timestamp, int senderId, ChunkSlice slice) { }
    private record ChunkSlice(int chunk, int offset, int length) { }
    private interface ChunkBackend extends AutoCloseable {
        ChunkSlice append(byte[] bytes);
        byte[] read(ChunkSlice slice);
        int chunkCount();
        byte[] copyChunk(int index);
        long heapBytes();
        long nativeBytes();
        @Override void close();
    }

    private static final class ColdChunkBackend implements ChunkBackend {
        private final List<byte[]> chunks = new ArrayList<>();
        private final List<Integer> rawLengths = new ArrayList<>();
        private final List<Boolean> compressed = new ArrayList<>();
        private final ColdSegmentCodec codec;
        private long allocated;
        ColdChunkBackend(ChunkBackend original, ColdSegmentCodec codec, boolean compress) {
            this.codec = codec;
            for (int i = 0; i < original.chunkCount(); i++) {
                byte[] raw = original.copyChunk(i);
                byte[] encoded = compress && codec.enabled() ? codec.compress(raw) : raw;
                boolean smaller = encoded.length < raw.length;
                byte[] stored = smaller ? encoded : raw;
                chunks.add(stored); rawLengths.add(raw.length); compressed.add(smaller); allocated += stored.length;
            }
        }
        public ChunkSlice append(byte[] bytes) { throw new IllegalStateException("event session is sealed"); }
        public byte[] read(ChunkSlice slice) { return Arrays.copyOfRange(copyChunk(slice.chunk), slice.offset, slice.offset + slice.length); }
        public int chunkCount() { return chunks.size(); }
        public byte[] copyChunk(int index) {
            return compressed.get(index) ? codec.decode(chunks.get(index), rawLengths.get(index)) : chunks.get(index);
        }
        public long heapBytes() { return allocated; }
        public long nativeBytes() { return 0; }
        public void close() { chunks.clear(); rawLengths.clear(); compressed.clear(); allocated = 0; }
    }

    private interface ChunkBackendFactory {
        ChunkBackend create();
        String name();

        static ChunkBackendFactory create(String requested) {
            String mode = requested == null ? "auto" : requested.trim().toLowerCase(Locale.ROOT);
            if (!"heap".equals(mode)) {
                try {
                    ReflectiveMemorySegmentBackend.probe();
                    return new ChunkBackendFactory() {
                        public ChunkBackend create() { return new ReflectiveMemorySegmentBackend(); }
                        public String name() { return "memory-segment"; }
                    };
                } catch (RuntimeException | LinkageError error) {
                    if ("memory-segment".equals(mode)) log.warn("MemorySegment 不可用，回退堆内分块: {}", error.toString());
                }
            }
            return new ChunkBackendFactory() {
                public ChunkBackend create() { return new HeapChunkBackend(); }
                public String name() { return "heap"; }
            };
        }
    }

    private static final class HeapChunkBackend implements ChunkBackend {
        private final List<byte[]> chunks = new ArrayList<>();
        private final List<ChunkBounds> bounds = new ArrayList<>();
        private int position;
        private long bytes;
        private long allocated;

        public ChunkSlice append(byte[] value) {
            if (chunks.isEmpty() || (long) position + value.length > chunks.get(chunks.size() - 1).length) {
                int capacity = Math.max(CHUNK_BYTES, value.length);
                chunks.add(new byte[capacity]); bounds.add(new ChunkBounds(capacity)); position = 0;
                allocated += capacity;
            }
            int chunk = chunks.size() - 1;
            int offset = position;
            System.arraycopy(value, 0, chunks.get(chunk), position, value.length);
            position += value.length; bytes += value.length;
            bounds.get(chunk).used = position;
            return new ChunkSlice(chunk, offset, value.length);
        }

        public byte[] read(ChunkSlice slice) { return Arrays.copyOfRange(chunks.get(slice.chunk), slice.offset, slice.offset + slice.length); }
        public int chunkCount() { return chunks.size(); }
        public byte[] copyChunk(int index) { return Arrays.copyOf(chunks.get(index), bounds.get(index).used); }
        public long heapBytes() { return allocated; }
        public long nativeBytes() { return 0; }
        public void close() { chunks.clear(); bounds.clear(); position = 0; bytes = 0; allocated = 0; }
    }

    private static final class ReflectiveMemorySegmentBackend implements ChunkBackend {
        private static final Method OF_SHARED;
        private static final Method ALLOCATE;
        private static final Method AS_BYTE_BUFFER;
        private static final Method CLOSE;
        static {
            try {
                Class<?> arena = Class.forName("java.lang.foreign.Arena");
                Class<?> segment = Class.forName("java.lang.foreign.MemorySegment");
                OF_SHARED = arena.getMethod("ofShared");
                ALLOCATE = arena.getMethod("allocate", long.class, long.class);
                AS_BYTE_BUFFER = segment.getMethod("asByteBuffer");
                CLOSE = arena.getMethod("close");
            } catch (Exception error) {
                throw new ExceptionInInitializerError(error);
            }
        }

        private final Object arena;
        private final List<Object> chunks = new ArrayList<>();
        private final List<ChunkBounds> bounds = new ArrayList<>();
        private int position;
        private long bytes;
        private long allocated;
        private boolean closed;

        static void probe() {
            ReflectiveMemorySegmentBackend backend = new ReflectiveMemorySegmentBackend();
            backend.close();
        }
        private ReflectiveMemorySegmentBackend() {
            try { arena = OF_SHARED.invoke(null); } catch (Exception error) { throw new IllegalStateException(error); }
        }
        public ChunkSlice append(byte[] value) {
            try {
                if (closed) throw new IllegalStateException("memory arena is closed");
                if (chunks.isEmpty() || (long) position + value.length > bounds.get(bounds.size() - 1).capacity) {
                    int capacity = Math.max(CHUNK_BYTES, value.length);
                    chunks.add(ALLOCATE.invoke(arena, (long) capacity, 1L)); bounds.add(new ChunkBounds(capacity)); position = 0;
                    allocated += capacity;
                }
                ByteBuffer buffer = ((ByteBuffer) AS_BYTE_BUFFER.invoke(chunks.get(chunks.size() - 1))).duplicate();
                int offset = position; buffer.position(position); buffer.put(value); position += value.length; bytes += value.length;
                bounds.get(chunks.size() - 1).used = position;
                return new ChunkSlice(chunks.size() - 1, offset, value.length);
            } catch (RuntimeException error) { throw error; } catch (Exception error) { throw new IllegalStateException(error); }
        }
        public byte[] read(ChunkSlice slice) {
            try {
                ByteBuffer buffer = ((ByteBuffer) AS_BYTE_BUFFER.invoke(chunks.get(slice.chunk))).duplicate();
                byte[] value = new byte[slice.length]; buffer.position(slice.offset); buffer.get(value); return value;
            } catch (Exception error) { throw new IllegalStateException(error); }
        }
        public int chunkCount() { return chunks.size(); }
        public byte[] copyChunk(int index) {
            return read(new ChunkSlice(index, 0, bounds.get(index).used));
        }
        public long heapBytes() { return 0; }
        public long nativeBytes() { return allocated; }
        public void close() {
            if (closed) return;
            try { CLOSE.invoke(arena); }
            catch (Exception error) { throw new IllegalStateException("Unable to release event memory arena", error); }
            closed = true; chunks.clear(); bounds.clear(); position = 0; bytes = 0; allocated = 0;
        }
    }

    private static final class ChunkBounds {
        private final int capacity;
        private int used;
        private ChunkBounds(int capacity) { this.capacity = capacity; }
    }

    private static final class ColdSegmentCodec {
        private final boolean enabled;
        private final int level;
        private final long threshold;
        private final Method compress;
        private final Method decompress;

        private ColdSegmentCodec(boolean enabled, int level, long threshold) {
            this(enabled, level, threshold, null, null);
        }
        private ColdSegmentCodec(boolean enabled, int level, long threshold, Method compress, Method decompress) {
            this.enabled = enabled; this.level = level; this.threshold = threshold; this.compress = compress; this.decompress = decompress;
        }
        static ColdSegmentCodec create(String requested, int level, long threshold) {
            String mode = requested == null ? "auto" : requested.trim().toLowerCase(Locale.ROOT);
            if ("none".equals(mode)) return new ColdSegmentCodec(false, level, Long.MAX_VALUE);
            try {
                Class<?> zstd = Class.forName("com.github.luben.zstd.Zstd");
                Method compress = zstd.getMethod("compress", byte[].class, int.class);
                Method decompress = zstd.getMethod("decompress", byte[].class, int.class);
                return new ColdSegmentCodec(true, Math.max(1, level), threshold, compress, decompress);
            } catch (Exception error) {
                if ("zstd".equals(mode)) log.warn("Zstd 不可用，冷段保持未压缩: {}", error.toString());
                return new ColdSegmentCodec(false, level, Long.MAX_VALUE);
            }
        }
        boolean enabled() { return enabled; }
        long threshold() { return threshold; }
        String name() { return enabled ? "zstd" : "none"; }
        byte[] compress(byte[] raw) {
            if (!enabled) return raw;
            try { return (byte[]) compress.invoke(null, raw, level); } catch (Exception error) { return raw; }
        }
        byte[] decode(byte[] compressed, int rawLength) {
            if (!enabled) return compressed;
            try { return (byte[]) decompress.invoke(null, compressed, rawLength); } catch (Exception error) { throw new IllegalStateException("无法解压冷事件段", error); }
        }
    }
}
