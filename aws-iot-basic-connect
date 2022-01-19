import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.io.ClientBootstrap;
import software.amazon.awssdk.crt.io.EventLoopGroup;
import software.amazon.awssdk.crt.io.HostResolver;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MyPubSub {
    static int port = 8883;
    static String certPath = "C:\\Users\\Administrator\\Desktop\\test.cert.pem";
    static String keyPath = "C:\\Users\\Administrator\\Desktop\\test.private.key";
    static int messagesToPublish = 3;
    static String topic = "$aws/things/test/shadow/get/accepted";
    static String message = "{\n" + "  \"message\": \"来自 Amazon IoT Java SDK的问候\"\n" + "}";
    static boolean isCI = false;

    static void onApplicationFailure(Throwable cause) {
        if (isCI) {
            throw new RuntimeException("BasicPubSub execution failure", cause);
        } else if (cause != null) {
            System.out.println("Exception encountered: " + cause.toString());
        }
    }

    public static void main(String[] args) {
        MqttClientConnectionEvents callbacks = new MqttClientConnectionEvents() {
            @Override
            public void onConnectionInterrupted(int errorCode) {
                if (errorCode != 0) {
                    System.out.println("Connection interrupted: " + errorCode + ": " + CRT.awsErrorString(errorCode));
                }
            }

            @Override
            public void onConnectionResumed(boolean sessionPresent) {
                System.out.println("Connection resumed: " + (sessionPresent ? "existing session" : "clean session"));
            }
        };

        try (EventLoopGroup eventLoopGroup = new EventLoopGroup(1);
             HostResolver resolver = new HostResolver(eventLoopGroup);
             ClientBootstrap clientBootstrap = new ClientBootstrap(eventLoopGroup, resolver);
             AwsIotMqttConnectionBuilder builder = AwsIotMqttConnectionBuilder.newMtlsBuilderFromPath(certPath, keyPath)) {


            builder.withBootstrap(clientBootstrap)
//                    .withConnectionEventCallbacks(callbacks)
                    .withClientId("test-" + UUID.randomUUID().toString())
                    .withEndpoint("a1vomo9z7jjbz.ats.iot.cn-north-1.amazonaws.com.cn")
//                    .withPort((short) port)
                    .withCleanSession(true)
                    .withProtocolOperationTimeoutMs(60000);


            try (MqttClientConnection connection = builder.build()) {
                CompletableFuture<Boolean> connected = connection.connect();
                try {
                    boolean sessionPresent = connected.get();
                    System.out.println("Connected to " + (!sessionPresent ? "new" : "existing") + " session!");
                } catch (Exception ex) {
                    throw new RuntimeException("Exception occurred during connect", ex);
                }

                CountDownLatch countDownLatch = new CountDownLatch(messagesToPublish);

                CompletableFuture<Integer> subscribed = connection.subscribe(topic, QualityOfService.AT_LEAST_ONCE, (message) -> {
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    System.out.println("MESSAGE: " + payload);
                    countDownLatch.countDown();
                });

                subscribed.get();

                int count = 0;
                while (count++ < messagesToPublish) {
                    CompletableFuture<Integer> published = connection.publish(new MqttMessage(topic, message.getBytes(), QualityOfService.AT_LEAST_ONCE, false));
                    published.get();
                    Thread.sleep(1000);
                }
                countDownLatch.await();

                CompletableFuture<Void> disconnected = connection.disconnect();
                disconnected.get();
            }
        } catch (CrtRuntimeException | InterruptedException | ExecutionException ex) {
            onApplicationFailure(ex);
        }
        CrtResource.waitForNoResources();
        System.out.println("Complete!");
    }

}
