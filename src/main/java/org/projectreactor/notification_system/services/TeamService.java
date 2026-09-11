package org.projectreactor.notification_system.services;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.notification_system.model.NotificationEvent;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class TeamService implements NotificationService {

    @Override
    public Mono<Boolean> sendNotification(NotificationEvent event) {
        return Mono.fromCallable(() -> {
            Thread.sleep(150);

            //Simulate error
            if(ThreadLocalRandom.current().nextInt(10) == 0) {
                throw new RuntimeException("Error on send message in Teams.");
            }
            log.info("Message in Teams success: {}", event);
            return true;
        });
    }
}
