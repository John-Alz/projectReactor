package org.projectreactor.monoflux;

import lombok.extern.java.Log;
import reactor.core.publisher.Flux;

@Log
public class FluxExample {

    public static void main(String[] args) {

        // Publisher
        Flux<String> stringFlux = Flux.just("Java", "Spring", "Reactor")
                .doOnNext(value -> log.info("[onNext]: " + value))
                .doOnComplete(() -> log.info("[onComplete]: success"));

        // Consumer
        stringFlux.subscribe(
                data -> log.info("Recibiendo: " + data),
                err -> log.info("Error: " + err.getMessage()),
                () -> log.info("Completed success")
        );

    }

}
