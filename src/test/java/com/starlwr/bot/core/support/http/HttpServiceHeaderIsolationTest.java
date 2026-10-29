package com.starlwr.bot.core.support.http;

import com.starlwr.bot.core.config.StarBotCoreProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Deterministic queued requests, with a mocked transport and no external network. */
class HttpServiceHeaderIsolationTest {
    private static final String URL = "https://example.invalid/resource";

    record RequestCase(String name, HeaderRequest request) {
        @Override public String toString() { return name; }
    }

    @FunctionalInterface
    interface HeaderRequest {
        CompletableFuture<?> submit(HttpService service, Map<String, String> headers);
    }

    static Stream<RequestCase> requests() {
        return Stream.of(
                new RequestCase("get", (s, h) -> s.asyncGet(URL, h)),
                new RequestCase("getForEntity", (s, h) -> s.asyncGetForEntity(URL, h)),
                new RequestCase("getJson", (s, h) -> s.asyncGetJson(URL, h)),
                new RequestCase("getBytes", (s, h) -> s.asyncGetBytes(URL, h)),
                new RequestCase("getBufferedImage", (s, h) -> s.asyncGetBufferedImage(URL, h)),
                new RequestCase("postWithHeaders", (s, h) -> s.asyncPostWithHeaders(URL, h)),
                new RequestCase("post", (s, h) -> s.asyncPost(URL, h, Map.of())),
                new RequestCase("postForEntity", (s, h) -> s.asyncPostForEntity(URL, h, Map.of())),
                new RequestCase("postJsonWithHeaders", (s, h) -> s.asyncPostJsonWithHeaders(URL, h)),
                new RequestCase("postJson", (s, h) -> s.asyncPostJson(URL, h, Map.of())),
                new RequestCase("postWithHeadersAsForm", (s, h) -> s.asyncPostWithHeadersAsForm(URL, h)),
                new RequestCase("postAsForm", (s, h) -> s.asyncPostAsForm(URL, h, Map.of())),
                new RequestCase("postJsonWithHeadersAsForm", (s, h) -> s.asyncPostJsonWithHeadersAsForm(URL, h)),
                new RequestCase("postJsonAsForm", (s, h) -> s.asyncPostJsonAsForm(URL, h, Map.of()))
        );
    }

    @ParameterizedTest(name = "{0} retains headers from submission time")
    @MethodSource("requests")
    void queuedRequestsRetainIndependentHeaderSnapshots(RequestCase requestCase) {
        QueuedExecutor executor = new QueuedExecutor();
        RestTemplate transport = mock(RestTemplate.class);
        List<Map<String, String>> received = new ArrayList<>();
        stubTransport(transport, received);
        HttpService service = new HttpService(executor, transport, new StarBotCoreProperties());
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Cookie", "identity=first-test-value");
        headers.put("User-Agent", "first-test-agent");
        headers.put("X-Removed", "retained-in-first-request");

        CompletableFuture<?> first = requestCase.request.submit(service, headers);
        headers.put("Cookie", "identity=second-test-value");
        headers.put("User-Agent", "second-test-agent");
        headers.remove("X-Removed");
        CompletableFuture<?> second = requestCase.request.submit(service, headers);
        headers.clear();
        headers.put("Cookie", "identity=late-mutation");
        verifyNoInteractions(transport);
        assertFalse(first.isDone());
        assertFalse(second.isDone());

        executor.runAll();
        first.join();
        second.join();
        assertEquals(2, received.size());
        assertEquals("identity=first-test-value", received.get(0).get("Cookie"));
        assertEquals("first-test-agent", received.get(0).get("User-Agent"));
        assertEquals("retained-in-first-request", received.get(0).get("X-Removed"));
        assertEquals("identity=second-test-value", received.get(1).get("Cookie"));
        assertEquals("second-test-agent", received.get(1).get("User-Agent"));
        assertNull(received.get(1).get("X-Removed"));
        assertEquals(Map.of("Cookie", "identity=late-mutation"), headers);
    }

    @ParameterizedTest(name = "{0} retains null-header async behavior")
    @MethodSource("requests")
    void nullHeadersRetainExistingAsyncBehavior(RequestCase requestCase) {
        QueuedExecutor executor = new QueuedExecutor();
        RestTemplate transport = mock(RestTemplate.class);
        HttpService service = new HttpService(executor, transport, new StarBotCoreProperties());

        CompletableFuture<?> request = assertDoesNotThrow(() -> requestCase.request.submit(service, null));
        assertFalse(request.isDone());
        executor.runAll();

        if (requestCase.name.equals("getBufferedImage")) {
            // This API historically catches request errors and returns an empty image.
            assertEquals(Optional.empty(), request.join());
        } else {
            CompletionException failure = assertThrows(CompletionException.class, request::join);
            assertInstanceOf(NullPointerException.class, failure.getCause());
        }
        verifyNoInteractions(transport);
    }

    @Test
    void immutableCallerHeadersAreAcceptedAndNeverModified() {
        QueuedExecutor executor = new QueuedExecutor();
        RestTemplate transport = mock(RestTemplate.class);
        List<Map<String, String>> received = new ArrayList<>();
        stubTransport(transport, received);
        HttpService service = new HttpService(executor, transport, new StarBotCoreProperties());
        Map<String, String> headers = Map.of("Cookie", "identity=immutable-test-value");
        CompletableFuture<?> request = service.asyncGet(URL, headers);
        executor.runAll();
        request.join();
        assertEquals(headers, received.get(0));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void stubTransport(RestTemplate transport, List<Map<String, String>> received) {
        when(transport.exchange(eq(URL), any(HttpMethod.class), any(HttpEntity.class), any(Class.class)))
                .thenAnswer(invocation -> {
                    HttpEntity<?> entity = invocation.getArgument(2);
                    received.add(new LinkedHashMap<>(entity.getHeaders().toSingleValueMap()));
                    Class<?> responseType = invocation.getArgument(3);
                    return ResponseEntity.ok(responseType == byte[].class ? new byte[0] : "{}");
                });
    }

    static class QueuedExecutor extends ThreadPoolTaskExecutor {
        private final ArrayDeque<Runnable> pending = new ArrayDeque<>();
        @Override public void execute(Runnable command) { pending.addLast(command); }
        void runAll() { while (!pending.isEmpty()) pending.removeFirst().run(); }
    }
}
