package org.projectreactor;

import lombok.extern.java.Log;
import org.projectreactor.pipelines.PipelineAllComments;
import org.projectreactor.pipelines.PipelineSumAllPricesInDiscount;
import org.projectreactor.pipelines.PipelineTopSalling;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Log
public class Main {
    public static void main(String[] args) {


//        PipelineTopSalling.getTopSellingVideogames()
//                .subscribe(System.out::println);

//        PipelineSumAllPricesInDiscount.getSumAllPricesInDiscount()
//                .subscribe();

//        PipelineAllComments.getAllReviewsComments()
//                .subscribe(log::info);

        Flux<String> fluxA = Flux.just("A1", "A2", "A3").delayElements(Duration.ofMillis(100));
        Flux<String> fluxB = Flux.just("B1", "B2", "B3").delayElements(Duration.ofMillis(50));

        Flux<String> fluxMerge = Flux.concat(fluxA, fluxB);

        fluxMerge
                .doOnNext(System.out::println)
                .blockLast();

    }
}