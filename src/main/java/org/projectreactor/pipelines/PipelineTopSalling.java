package org.projectreactor.pipelines;

import org.projectreactor.database.Database;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Flux;

public class PipelineTopSalling {

    // Debe retornar los nombres de los videojuegos con ventas mayores a 80
    public static Flux<String> getTopSellingVideogames() {
        return Database.getVideogamesFlux()
                .filter(game -> game.getTotalSold() > 80)
                .map(Videogame::getName);
    }

}
