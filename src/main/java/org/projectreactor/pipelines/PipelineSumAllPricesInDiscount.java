package org.projectreactor.pipelines;

import lombok.extern.java.Log;
import org.projectreactor.database.Database;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Mono;


@Log
public class PipelineSumAllPricesInDiscount {

    // Suma todos los precios de cada videojuego con descuento.
    public static Mono<Double> getSumAllPricesInDiscount() {
        return Database.getVideogamesFlux()
                .filter(Videogame::getIsDiscount)
                .map(Videogame::getPrice)
                .reduce(0.0, Double::sum)
                .doOnSuccess(value -> log.info("El valor total de la sumatoria de los juegos en descuento es: $" + value));
    }
}
