package org.projectreactor;

import lombok.extern.slf4j.Slf4j;
import org.projectreactor.callbacks.CallbacksExample;
import org.projectreactor.database.Database;
import org.projectreactor.errorhandler.FallbackService;
import org.projectreactor.errorhandler.HandleDisabledVideoGame;
import org.projectreactor.models.Console;
import org.projectreactor.models.Videogame;
import org.projectreactor.pipelines.PipelineAllComments;
import org.projectreactor.pipelines.PipelineSumAllPricesInDiscount;
import org.projectreactor.pipelines.PipelineTopSalling;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;

@Slf4j
public class Main {
    public static void main(String[] args) {


//        PipelineTopSalling.getTopSellingVideogames()
//                .subscribe(System.out::println);

//        PipelineSumAllPricesInDiscount.getSumAllPricesInDiscount()
//                .subscribe();

//        PipelineAllComments.getAllReviewsComments()
//                .subscribe(log::info);

//        Flux<String> fluxA = Flux.just("A1", "A2", "A3").delayElements(Duration.ofMillis(100));
//        Flux<String> fluxB = Flux.just("B1", "B2", "B3").delayElements(Duration.ofMillis(50));
//
//        Flux<String> fluxMerge = Flux.concat(fluxA, fluxB);
//        Flux<String> fluxMerge = Flux.merge(fluxA, fluxB);
//
//        fluxMerge
//                .doOnNext(System.out::println)
//                .blockLast();


//        Flux<String> fluxShipments = Flux.just("Shipment1", "Shipment2", "Shipment3", "Shipment4").delayElements(Duration.ofMillis(120));
//        Flux<String> fluxWarehouse = Flux.just("Stock1", "Stock2", "Stock3", "Stock4").delayElements(Duration.ofMillis(50));
//        Flux<String> fluxPayments = Flux.just("Pay1", "Pay2", "Pay3", "Pay4").delayElements(Duration.ofMillis(150));
//        Flux<String> fluxConfirm = Flux.just("Confirm1", "Confirm2", "Confirm3", "Confirm4").delayElements(Duration.ofMillis(20));
//
////        Flux<String> reportFlux = Flux.zip(fluxShipments, fluxWarehouse, (shipment, stock) -> shipment + " " + stock); // zip de dos elementos
//        Flux<String> reportFlux = Flux.zip(fluxShipments, fluxWarehouse, fluxPayments, fluxConfirm) // Zip de mas de dos elementos
//                        .map(tuple -> tuple.getT1() + " -> " + tuple.getT2() + " -> " + tuple.getT3() + " -> " + tuple.getT4());
//
//        reportFlux.doOnNext(System.out::println).blockLast();

//        HandleDisabledVideoGame.handleDisabledVideoGamesDefault()
//                .subscribe(v -> log.info(v.toString()));

//        FallbackService.callFallback()
//                .subscribe(v -> log.info(v.toString()));

//        CallbacksExample.callbacks()
//                .subscribe(data -> log.debug(data.getName()));

        Database.getVideogamesFlux()
                .filterWhen(videogame -> Mono.deferContextual(ctx -> {
                    var userId = ctx.getOrDefault("userId", "0");
                    if (userId.startsWith("1")) {
                        return Mono.just(videoGameForConsole(videogame, Console.XBOX));
                    } else if (userId.startsWith("2")) {
                        return Mono.just(videoGameForConsole(videogame, Console.PLAYSTATION));
                    } else {
                        return Mono.just(false);
                    }
                }))
                .contextWrite(Context.of("userId", "30020192"))
                .subscribe(vg -> log.info("Recomendacion name {} console {}", vg.getName(), vg.getConsole()));


    }

    private static boolean videoGameForConsole(Videogame videogame, Console console) {
        return videogame.getConsole().equals(console) || videogame.getConsole().equals(Console.ALL);
    }
}