package org.projectreactor.errorhandler;

import org.projectreactor.database.Database;
import org.projectreactor.models.Console;
import org.projectreactor.models.Review;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Flux;

import java.util.List;

public class HandleDisabledVideoGame {

    private static final Videogame DEFAULT_VIDEOGAME = Videogame.builder()
            .name("GTA 6")
            .price(100.00)
            .console(Console.ALL)
            .reviews(List.of(

            ))
            .officialWebsite("https://www.rockstargames.com/VI")
            .isDiscount(true)
            .totalSold(140)
            .build();

    public static Flux<Videogame> handleDisabledVideoGames() {
        return Database.getVideogamesFlux()
                .handle((vg, sink) -> {
                    if (Console.DISABLED == vg.getConsole()) {
                        sink.error(new RuntimeException("VideoGame is disabled."));
                        return;
                    }
                    sink.next(vg);
                })
                .onErrorResume(error -> {
                    System.out.println("Error: " + error.getMessage());
                    return Database.getVideogamesFlux().mergeWith(Database.fluxAssassinsDefault);
                })
                .cast(Videogame.class)
                .distinct(Videogame::getName);
    }

    public static Flux<Videogame> handleDisabledVideoGamesDefault() {
        return Database.getVideogamesFlux()
                .handle((vg, sink) -> {
                    if (Console.DISABLED == vg.getConsole()) {
                        sink.error(new RuntimeException("VideoGame is disabled."));
                        return;
                    }
                    sink.next(vg);
                })
                .onErrorReturn(DEFAULT_VIDEOGAME)
                .cast(Videogame.class);
    }

}
