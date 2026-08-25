package org.projectreactor.errorhandler;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.database.Database;
import org.projectreactor.models.Console;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Flux;

@Slf4j
public class FallbackService {


    public static Flux<Videogame> callFallback() {
        return Database.getVideogamesFlux()
                .handle((vg, sink) -> {
                    if (Console.DISABLED == vg.getConsole()) {
                        sink.error(new RuntimeException("VideoGame is disabled."));
                        return;
                    }
                    sink.next(vg);
                })
                .retry(5)
                .onErrorResume(error -> {
                    log.error("Error: {}", error.getMessage());
                    return Database.fluxFallback;
                })
                .repeat(1)
                .cast(Videogame.class);
    }

}
