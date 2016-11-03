/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sqlitetest;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class FixedSizeGroupCollector<T> implements Collector<T, List<T>, Stream<List<T>>> {

    public FixedSizeGroupCollector(final Integer size) {
        this.maxSize = size;
    }

    private final Integer maxSize;
    private final Stream.Builder<List<T>> builder = Stream.builder();

    @Override
    public Supplier<List<T>> supplier() {
        return () -> new ArrayList<>(maxSize);
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return (buffer, item) -> {
            buffer.add(item);
            if (buffer.size() >= maxSize) {
                builder.accept(new ArrayList<>(buffer));
                buffer.clear();
            }
        };
    }

    @Override
    public BinaryOperator<List<T>> combiner() {
        return (a, b) -> {
            throw new UnsupportedOperationException("Not supported.");
        };
    }

    @Override
    public Function<List<T>, Stream<List<T>>> finisher() {
        return (List<T> buffer) -> {
            if (buffer.isEmpty()) {
                return builder.build();
            } else {
                return Stream.concat(builder.build(), Stream.of(buffer));
            }
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.noneOf(Characteristics.class);
    }

}
