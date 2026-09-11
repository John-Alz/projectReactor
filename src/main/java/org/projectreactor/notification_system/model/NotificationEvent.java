package org.projectreactor.notification_system.model;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class NotificationEvent {

    private String id;
    private String source;
    private String message;
    private Priority priority;
    private LocalDateTime timestamp;
    private Status status;
}
