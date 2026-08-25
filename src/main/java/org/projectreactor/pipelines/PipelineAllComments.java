package org.projectreactor.pipelines;

import org.projectreactor.database.Database;
import org.projectreactor.models.Review;
import org.projectreactor.models.Videogame;
import reactor.core.publisher.Flux;

public class PipelineAllComments {

    public static Flux<String> getAllReviewsComments() {
        return Database.getVideogamesFlux()
                .flatMap(videogame -> Flux.fromIterable(videogame.getReviews()))
                .map(Review::getComment);
    }

}
