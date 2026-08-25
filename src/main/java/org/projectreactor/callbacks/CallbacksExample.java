package org.projectreactor.callbacks;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.database.Database;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class CallbacksExample {

    public static Flux<Videogame> callbacks() {
        return Database.getVideogamesFlux()
//                .delayElements(Duration.ofMillis(500))
//                .timeout(Duration.ofMillis(300))
                .doOnSubscribe(s -> log.info("[doOnSubscribe]"))
                .doOnRequest(n -> log.info("[doOnRequest]: {}", n))
                .doOnNext(videogame -> log.info("[doOnNext]: {}", videogame.getName()))
                .doOnCancel(() -> log.warn("[doOnCancel]"))
                .doOnError(error -> log.error("[doOnError]: {}", error.getMessage()))
                .doOnComplete(() -> log.info("[doOnComplete]: success"))
                .doOnTerminate(() -> log.info("[doOnTerminate]: siempre se ejecuta"))
                .doFinally(s -> log.warn("[doFinally]: {}", s));
    }

}
