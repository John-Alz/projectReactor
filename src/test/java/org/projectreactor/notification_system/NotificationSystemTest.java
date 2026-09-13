package org.projectreactor.notification_system;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.projectreactor.notification_system.model.NotificationEvent;
import org.projectreactor.notification_system.model.Priority;
import org.projectreactor.notification_system.model.Status;
import org.projectreactor.notification_system.services.EmailService;
import org.projectreactor.notification_system.services.NotificationService;
import org.projectreactor.notification_system.services.PhoneService;
import org.projectreactor.notification_system.services.TeamService;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;


class NotificationSystemTest {

    private NotificationService mockTeamsService;
    private NotificationService mockEmailService;
    private NotificationService mockPhoneService;
    private NotificationSystem notificationSystem;

    private AtomicInteger teamsCallCount;
    private AtomicInteger emailCallCount;
    private AtomicInteger phoneCallCount;

    @BeforeEach
    void setUp() {
        teamsCallCount = new AtomicInteger(0);
        emailCallCount = new AtomicInteger(0);
        phoneCallCount = new AtomicInteger(0);

        mockTeamsService = mock(TeamService.class);
        mockEmailService = mock(EmailService.class);
        mockPhoneService = mock(PhoneService.class);

        when(mockTeamsService.sendNotification(any(NotificationEvent.class))).thenAnswer(invocationOnMock -> {
            teamsCallCount.incrementAndGet();
            return Mono.just(true);
        });

        when(mockEmailService.sendNotification(any(NotificationEvent.class))).thenAnswer(invocationOnMock -> {
            emailCallCount.incrementAndGet();
            return Mono.just(true);
        });

        when(mockPhoneService.sendNotification(any(NotificationEvent.class))).thenAnswer(invocationOnMock -> {
            phoneCallCount.incrementAndGet();
            return Mono.just(true);
        });

        notificationSystem = new NotificationSystem(mockTeamsService, mockEmailService, mockPhoneService);

    }



    @Test
    @DisplayName("Should send events with LOW priority")
    void testLowPriority() {
        NotificationEvent event = createTestEvent(Priority.LOW);
        notificationSystem.publishEvent(event);

        verify(mockTeamsService, times(1)).sendNotification(any(NotificationEvent.class));
        verify(mockEmailService, never()).sendNotification(any(NotificationEvent.class));
        verify(mockPhoneService, never()).sendNotification(any(NotificationEvent.class));

        assert teamsCallCount.get() == 1;
        assert emailCallCount.get() == 0;
        assert phoneCallCount.get() == 0;
    }

    @Test
    @DisplayName("Should send events with MEDIUM priority")
    void testMediumPriority() {
        NotificationEvent event = createTestEvent(Priority.MEDIUM);
        notificationSystem.publishEvent(event);

        verify(mockTeamsService, times(1)).sendNotification(any(NotificationEvent.class));
        verify(mockEmailService, times(1)).sendNotification(any(NotificationEvent.class));
        verify(mockPhoneService, never()).sendNotification(any(NotificationEvent.class));

        assert teamsCallCount.get() == 1;
        assert emailCallCount.get() == 1;
        assert phoneCallCount.get() == 0;
    }


    @Test
    @DisplayName("Should send events with HIGH priority")
    void highPriorityEventsShouldGoToAllChannels() {
        NotificationEvent testEvent = createTestEvent(Priority.HIGH);

        notificationSystem.publishEvent(testEvent);

        verify(mockTeamsService, times(1)).sendNotification(any());
        verify(mockEmailService, times(1)).sendNotification(any());
        verify(mockPhoneService, times(1)).sendNotification(any());

        assert teamsCallCount.get() == 1;
        assert emailCallCount.get() == 1;
        assert phoneCallCount.get() == 1;
    }


    @Test
    void shouldHistoryKeep3Events() {
        NotificationEvent event1 = createTestEvent(Priority.LOW);
        NotificationEvent event2 = createTestEvent(Priority.MEDIUM);
        NotificationEvent event3 = createTestEvent(Priority.HIGH);

        notificationSystem.publishEvent(event1);
        notificationSystem.publishEvent(event2);
        notificationSystem.publishEvent(event3);

        StepVerifier.create(notificationSystem.getNotificationHistory().take(3))
                .expectNextCount(3)
                .verifyComplete();

    }


    private NotificationEvent createTestEvent(Priority priority) {
        return NotificationEvent.builder()
                .id(UUID.randomUUID().toString())
                .source("TEST")
                .message("Test msg with priority: " + priority.toString())
                .priority(priority)
                .timestamp(LocalDateTime.now())
                .status(Status.PENDING)
                .build();
    }

    private void sleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}