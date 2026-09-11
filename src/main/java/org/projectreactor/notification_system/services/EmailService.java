package org.projectreactor.notification_system.services;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.notification_system.model.NotificationEvent;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class EmailService implements NotificationService{
    @Override
    public Mono<Boolean> sendNotification(NotificationEvent event) {
        return Mono.fromCallable(() -> {
            Thread.sleep(300);

            //Simulate error
            if(ThreadLocalRandom.current().nextInt(100) < 15) {
                throw new RuntimeException("Error on send message in Email.");
            }
            log.info("Message in Email success: {}", event);
            return true;
        });
    }
}
