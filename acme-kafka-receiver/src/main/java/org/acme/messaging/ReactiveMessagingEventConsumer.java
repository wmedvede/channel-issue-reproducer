package org.acme.messaging;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ReactiveMessagingEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMessagingEventConsumer.class);

    private static final String KAFKA_TOPIC = "acme-events";

    @Inject
    @ConfigProperty(name = "sleepTime", defaultValue = "PT30S")
    Duration sleepTime;

    @Inject
    @Channel("NOT_CONFIGURED_EMITTER")
    Emitter<String> notConfiguredEmitter;

    @Incoming(KAFKA_TOPIC)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> onEventEvent(Message<String> message) {
        LOGGER.info("Receive message: {}", message.getPayload());
        if (message.getPayload() != null && message.getPayload().contains("nack")) {
            LOGGER.debug("A nack message was received");
            return message.nack(new Exception("A nack message was received", new Exception("The internal cause!!")));
        } else {
            return CompletableFuture.runAsync(() -> handleEvent(message.getPayload()))
                    .thenRun(message::ack);
        }
    }

    private void handleEvent(String payload) {
        try {
            //do what you want;
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
