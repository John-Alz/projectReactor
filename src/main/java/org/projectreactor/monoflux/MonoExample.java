package org.projectreactor.monoflux;

import lombok.extern.java.Log;
import reactor.core.publisher.Mono;


@Log
public class MonoExample {

    public static void main(String[] args) {
        // Publisher
        Mono<String> mono = Mono.just("Hola mundo")
                .doOnNext(s -> log.info("[onNext]: " + s))
                .doOnSuccess(value -> log.info("[onSuccess]: " + value))
                .doOnError(error -> log.info("[onError]: " + error.getMessage()));

        // Consumer
        mono.subscribe(
                data -> log.info("Recibiendo datos: " + data),
                err -> log.info("Error: " + err.getMessage()),
                () -> log.info("Complete success")
        );

    }

}
