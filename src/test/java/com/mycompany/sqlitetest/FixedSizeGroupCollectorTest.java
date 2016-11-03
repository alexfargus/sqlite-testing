/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sqlitetest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 *
 * @author alex
 */
public class FixedSizeGroupCollectorTest extends Assertions {

    @Test
    public void collect_withSize1_returnsSingletonLists() {
        final List<List<Integer>> expected = makeStream()
                .map(i -> Arrays.asList(i))
                .collect(Collectors.toList());

        final List<List<Integer>> result = makeStream()
                .collect(new FixedSizeGroupCollector<>(1))
                .collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }
    
    @Test
    public void collect_withSize2_returnsTupleStream() {
        final List<List<Integer>> expected = makeStream()
                .filter(i -> i % 2 == 0)
                .map(i -> Arrays.asList(i, i+ 1))
                .collect(Collectors.toList());

        final List<List<Integer>> result = makeStream()
                .collect(new FixedSizeGroupCollector<>(2))
                .collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }
    
    @Test
    public void collect_withSize3_returnsTupleStream() {
        final List<List<Integer>> expected = Arrays.asList(
                Arrays.asList(0, 1, 2),
                Arrays.asList(3, 4, 5),
                Arrays.asList(6, 7, 8),
                Arrays.asList(9)
        );

        final List<List<Integer>> result = makeStream()
                .collect(new FixedSizeGroupCollector<>(3))
                .collect(Collectors.toList());

        assertThat(result).isEqualTo(expected);
    }    
    
    
    private Stream<Integer> makeStream() {
        return IntStream.range(0, 10).boxed();
    }
}
