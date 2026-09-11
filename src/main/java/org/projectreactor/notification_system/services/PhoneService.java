package org.projectreactor.notification_system.services;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.notification_system.model.NotificationEvent;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class PhoneService implements NotificationService {
    @Override
    public Mono<Boolean> sendNotification(NotificationEvent event) {
        return Mono.fromCallable(() -> {
            Thread.sleep(1000);

            //Simulate error
            if(ThreadLocalRandom.current().nextInt(100) < 20) {
                throw new RuntimeException("Error on send message in Phone call.");
            }
            log.info("Message in Phone Call success: {}", event);
            return true;
        });
    }
}
