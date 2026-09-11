package org.projectreactor.notification_system.services;

import org.projectreactor.notification_system.model.NotificationEvent;
import reactor.core.publisher.Mono;

public interface NotificationService {

    Mono<Boolean> sendNotification(NotificationEvent event);

}
