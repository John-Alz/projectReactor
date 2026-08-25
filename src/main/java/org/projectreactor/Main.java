package org.projectreactor;

import lombok.extern.java.Log;
import org.projectreactor.pipelines.PipelineSumAllPricesInDiscount;
import org.projectreactor.pipelines.PipelineTopSalling;

@Log
public class Main {
    public static void main(String[] args) {


//        PipelineTopSalling.getTopSellingVideogames()
//                .subscribe(System.out::println);

        PipelineSumAllPricesInDiscount.getSumAllPricesInDiscount()
                .subscribe();

    }
}