package dev.langchain4j.http.client.jdk;

import static dev.langchain4j.http.client.HttpMethod.GET;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import dev.langchain4j.exception.AsyncNotSupportedException;
import dev.langchain4j.http.client.HttpRequest;
import dev.langchain4j.http.client.sse.DefaultServerSentEventParser;
import dev.langchain4j.http.client.sse.HttpStreamingEvent;
import dev.langchain4j.http.client.sse.ServerSentEvent;
import dev.langchain4j.http.client.sse.ServerSentEventParser;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdkHttpClientParserFailureTest {

    private ServerSocket server;
    private ExecutorService serverExecutor;
    private final AtomicReference<Socket> connection = new AtomicReference<>();
    private final CountDownLatch clientDisconnected = new CountDownLatch(1);
    private final CompletableFuture<Void> responseSent = new CompletableFuture<>();

    @BeforeEach
    void startServer() throws Exception {
        server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try (Socket socket = server.accept()) {
                connection.set(socket);
                var input = socket.getInputStream();
                StringBuilder headers = new StringBuilder();
                int value;
                while ((value = input.read()) != -1) {
                    headers.append((char) value);
                    if (headers.toString().endsWith("\r\n\r\n")) {
                        break;
                    }
                }
                var output = socket.getOutputStream();
                output.write(("HTTP/1.1 200 OK\r\n"
                                + "Content-Type: text/event-stream\r\n"
                                + "Connection: close\r\n\r\n"
                                + "data: hello\n\n")
                        .getBytes(UTF_8));
                output.flush();
                responseSent.complete(null);

                // Keep the response open. Only the client can end it before test cleanup.
                try {
                    if (input.read() == -1) {
                        clientDisconnected.countDown();
                    }
                } catch (SocketException resetByClient) {
                    clientDisconnected.countDown();
                }
            } catch (IOException error) {
                responseSent.completeExceptionally(error);
            }
        });
    }

    @AfterEach
    void stopServer() throws Exception {
        server.close();
        Socket socket = connection.get();
        if (socket != null) {
            socket.close();
        }
        serverExecutor.shutdownNow();
        assertThat(serverExecutor.awaitTermination(5, SECONDS)).isTrue();
    }

    @Test
    void unsupported_incremental_parser_should_release_the_http_connection() throws Exception {
        ServerSentEventParser blockingOnlyParser = (body, listener) -> {};

        Throwable error = streamUntilError(blockingOnlyParser);

        assertThat(error).isInstanceOf(AsyncNotSupportedException.class);
        assertClientDisconnected();
    }

    @Test
    void failing_parser_initialization_should_release_the_http_connection() throws Exception {
        RuntimeException failure = new IllegalStateException("parser initialization failed");
        ServerSentEventParser parser = new DefaultServerSentEventParser() {
            @Override
            public Incremental incremental() {
                throw failure;
            }
        };

        assertThat(streamUntilError(parser)).isSameAs(failure);
        assertClientDisconnected();
    }

    @Test
    void failing_parser_feed_should_release_the_http_connection() throws Exception {
        RuntimeException failure = new IllegalStateException("parser feed failed");
        ServerSentEventParser parser = new DefaultServerSentEventParser() {
            @Override
            public Incremental incremental() {
                return new Incremental() {
                    @Override
                    public List<ServerSentEvent> feed(ByteBuffer bytes) {
                        throw failure;
                    }

                    @Override
                    public List<ServerSentEvent> flush() {
                        return List.of();
                    }
                };
            }
        };

        assertThat(streamUntilError(parser)).isSameAs(failure);
        assertClientDisconnected();
    }

    @Test
    void downstream_cancellation_should_release_the_http_connection() throws Exception {
        CompletableFuture<Void> cancelled = new CompletableFuture<>();
        JdkHttpClient.builder().build().stream(request(), new DefaultServerSentEventParser())
                .subscribe(new Flow.Subscriber<>() {
                    private Flow.Subscription subscription;

                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        this.subscription = subscription;
                        subscription.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(HttpStreamingEvent event) {
                        if (event instanceof ServerSentEvent) {
                            subscription.cancel();
                            cancelled.complete(null);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        cancelled.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        cancelled.completeExceptionally(new AssertionError("Expected cancellation"));
                    }
                });

        responseSent.get(5, SECONDS);
        cancelled.get(5, SECONDS);
        assertClientDisconnected();
    }

    private Throwable streamUntilError(ServerSentEventParser parser) throws Exception {
        CompletableFuture<Throwable> error = new CompletableFuture<>();

        JdkHttpClient.builder().build().stream(request(), parser).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(HttpStreamingEvent event) {}

            @Override
            public void onError(Throwable throwable) {
                error.complete(throwable);
            }

            @Override
            public void onComplete() {
                error.completeExceptionally(new AssertionError("Expected a parsing error"));
            }
        });

        responseSent.get(5, SECONDS);
        return error.get(5, SECONDS);
    }

    private HttpRequest request() {
        return HttpRequest.builder()
                .method(GET)
                .url("http://localhost:" + server.getLocalPort() + "/stream")
                .build();
    }

    private void assertClientDisconnected() throws Exception {
        assertThat(clientDisconnected.await(5, SECONDS))
                .as("a terminal parser failure must release the open HTTP connection")
                .isTrue();
    }
}
