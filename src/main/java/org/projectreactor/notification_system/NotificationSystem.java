package org.projectreactor.notification_system;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.projectreactor.notification_system.model.NotificationEvent;
import org.projectreactor.notification_system.model.Priority;
import org.projectreactor.notification_system.model.Status;
import org.projectreactor.notification_system.services.EmailService;
import org.projectreactor.notification_system.services.NotificationService;
import org.projectreactor.notification_system.services.PhoneService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class NotificationSystem {
    private static final String TEAMS_CHANNEL = "teams_channel";
    private static final String EMAIL_CHANNEL = "email_channel";
    private static final String PHONE_CHANNEL = "phone_channel";

    private final Sinks.Many<NotificationEvent> mainEventSink;

    @Getter
    private final Sinks.Many<NotificationEvent> historySink;

    private final NotificationService teamsService;
    private final NotificationService emailService;
    private final NotificationService phoneService;


    private final Sinks.One<NotificationEvent> teamsSink;
    private final Sinks.One<NotificationEvent> emailSink;
    private final Sinks.One<NotificationEvent> phoneSink;

    private final ConcurrentMap<String, NotificationEvent> notificationCache;

    public NotificationSystem(
            NotificationService teamsService,
            NotificationService emailService,
            NotificationService phoneService) {
        this.mainEventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.historySink = Sinks.many().replay().limit(50);

        this.teamsSink = Sinks.one();
        this.emailSink = Sinks.one();
        this.phoneSink = Sinks.one();

        this.teamsService = teamsService;
        this.emailService = emailService;
        this.phoneService = phoneService;

        this.notificationCache = new ConcurrentHashMap<>();

        setupProcessingFlows();
    }

    public NotificationSystem() {
        this.mainEventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.historySink  = Sinks.many().replay().limit(50);
        this.teamsSink = Sinks.one();
        this.emailSink = Sinks.one();
        this.phoneSink = Sinks.one();

        this.teamsService = new PhoneService();
        this.emailService = new EmailService();
        this.phoneService = new PhoneService();

        this.notificationCache = new ConcurrentHashMap<>();

        this.setupProcessingFlows();
    }

    private void setupProcessingFlows() {
        mainEventSink
                .asFlux()
                .doOnNext(event -> log.info("Received new event: {}", event))
                .doOnNext(this::updateEventStatus)
                .doOnNext(this.historySink::tryEmitNext)
                .subscribe(this::routeEventByPriority);

        setupTeamsProcessor();
        setupEmailProcessor();
        setupPhoneProcessor();
    }

    private void setupTeamsProcessor() {
        teamsSink
                .asMono()
                .flatMap(event ->
                        teamsService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> updateDeliveredStatus(event, TEAMS_CHANNEL))
                                .doOnError(error -> updatedErrorStatus(event, TEAMS_CHANNEL, error))
                                .onErrorResume(error -> Mono.just(false))
                )
                .subscribe();
    }

    private void setupEmailProcessor() {
        emailSink
                .asMono()
                .flatMap(event ->
                        emailService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> updateDeliveredStatus(event, EMAIL_CHANNEL))
                                .doOnError(error -> updatedErrorStatus(event, EMAIL_CHANNEL, error))
                                .onErrorResume(error -> Mono.just(false))
                        )
                .subscribe();
    }

    private void setupPhoneProcessor() {
        phoneSink
                .asMono()
                .flatMap(event ->
                        phoneService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> updateDeliveredStatus(event, PHONE_CHANNEL))
                                .doOnError(error -> updatedErrorStatus(event, PHONE_CHANNEL, error))
                                .retry(3)
                                .onErrorResume(error -> Mono.just(false))
                )
                .subscribe();
    }

    private void updateEventStatus(NotificationEvent event) {
        if (Objects.isNull(event.getId())) {
            event.setId(UUID.randomUUID().toString());
        }
        if (Objects.isNull(event.getStatus())) {
            event.setStatus(Status.PENDING);
        }
        this.notificationCache.put(event.getId(), event);
    }

    private void updatedErrorStatus(NotificationEvent event, String channel, Throwable error) {
        log.error("Error to send notification by {}, for event {}, error {}", channel, event.getId(), error);
        NotificationEvent cacheEvent = notificationCache.get(event.getId());
        if (Objects.nonNull(cacheEvent)) {
            cacheEvent.setStatus(Status.FAILED);
            historySink.tryEmitNext(cacheEvent);
        }
    }

    private void updateDeliveredStatus(NotificationEvent event, String channel) {
        log.info("Success event by {}, event {}", channel, event.getId());
        NotificationEvent cacheEvent = notificationCache.get(event.getId());
        if (Objects.nonNull(cacheEvent)) {
            cacheEvent.setStatus(Status.DELIVERED);
            historySink.tryEmitNext(cacheEvent);
        }
    }

    private void routeEventByPriority(NotificationEvent event) {
        this.teamsSink.tryEmitValue(event);
        if (event.getPriority() == Priority.HIGH || event.getPriority() == Priority.MEDIUM) {
            emailSink.tryEmitValue(event);
        }
        if (event.getPriority() == Priority.HIGH) {
            phoneSink.tryEmitValue(event);
        }
    }

    public void publishEvent(NotificationEvent event) {
        mainEventSink.tryEmitNext(event);
    }

    public Flux<NotificationEvent> getNotificationHistory() {
        return historySink
                .asFlux();
    }

    public Mono<NotificationEvent> getNotificationById(String id) {
        return Mono.justOrEmpty(notificationCache.get(id));
    }

    public Flux<NotificationEvent> retryFailedNotification() {
        return Flux.fromIterable(notificationCache.values())
                .filter(event -> event.getStatus() == Status.FAILED)
                .doOnNext(this::publishEvent);
    }

}
