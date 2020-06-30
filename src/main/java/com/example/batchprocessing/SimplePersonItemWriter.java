package com.example.batchprocessing;

import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;

import java.util.Iterator;
import java.util.List;

public class SimplePersonItemWriter implements ItemWriter<Person> {

    /**
     * Process the supplied data element. Will not be called with any null items
     * in normal operation.
     *
     * @param items items to be written
     * @throws Exception if there are errors. The framework will catch the
     *                   exception and convert or rethrow it as appropriate.
     */
    @Override
    public void write(List<? extends Person> items) throws Exception {
        System.out.println(items);
    }
}
