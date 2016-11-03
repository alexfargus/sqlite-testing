/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sqlitetest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author alex
 */
public class SqliteChannelTest extends Assertions {

    @ClassRule
    public static final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getData_forAllSamples_ReturnsAllSamples() throws Exception {
        doTest(5L, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void getData_forMiddleSubsetOfSamples_ReturnsSpecifiedSubset() throws Exception {
        doTest(15L, 5L, 10L);
    }

    @Test
    public void getData_forHeadOfSamples_ReturnsSpecifiedSubset() throws Exception {
        doTest(15L, -1L, 10L);
    }

    @Test
    public void getData_forTailOfSamples_ReturnsSpecifiedSubset() throws Exception {
        doTest(15L, 10L, 20L);
    }

    @Test
    public void putData_forLargeSamples_StoresAllSamples() throws Exception {
        final Path root = folder.newFolder().toPath();
        final String channelId = UUID.randomUUID().toString();     
        final Long count = 10_000_000L;
        
        try (final SqliteChannel channel = new SqliteChannel(root, channelId)) {
            LongStream.range(0L, count).boxed()
                    .collect(new FixedSizeGroupCollector<>(10_000))
                    .map(chunk -> makeMap(chunk))
                    .forEach(chunk -> {
                        try {
                            channel.putData(chunk);
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
        }
        
        try (final SqliteChannel channel = new SqliteChannel(root, channelId)) {
            final NavigableMap<Long, Object> data = channel.getData(Long.MIN_VALUE, Long.MAX_VALUE);
            assertThat(data.size()).isEqualTo(count.intValue());
            data.forEach(this::checkEntry);
        }
    }
    
    //@Test
    public void putData_forLargeSamplesInParallel_StoresAllSamples() throws Exception {
        final Path root = folder.newFolder().toPath();
        final String channelId = UUID.randomUUID().toString();     
        final Long count = 1_000_000L;
        
        try (final SqliteChannel channel = new SqliteChannel(root, channelId)) {
            LongStream.range(0L, count).boxed()
                    .collect(new FixedSizeGroupCollector<>(10_000))
                    .parallel()
                    .map(chunk -> makeMap(chunk))
                    .forEach(chunk -> {
                        try {
                            channel.putData(chunk);
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
        }
        
        try (final SqliteChannel channel = new SqliteChannel(root, channelId)) {
            final NavigableMap<Long, Object> data = channel.getData(Long.MIN_VALUE, Long.MAX_VALUE);
            assertThat(data.size()).isEqualTo(count.intValue());
            data.forEach(this::checkEntry);
        }
    }    

    private void doTest(Long nSamples, Long from, Long to) throws IOException, SQLException {
        final NavigableMap<Long, Object> data = makeData(nSamples);
        final NavigableMap<Long, Object> result;

        try (final SqliteChannel channel = makeChannel()) {
            channel.putData(data);
            result = channel.getData(from, to);
        }

        assertThat(result).isEqualTo(data.subMap(from, true, to, true));
    }

    private SqliteChannel makeChannel() throws SQLException, IOException {
        final File root = folder.newFolder();
        return new SqliteChannel(root.toPath(), UUID.randomUUID().toString());
    }

    private void checkEntry(Long index, Object value) {
        assertThat(index.doubleValue()).isEqualTo(((Number) value).doubleValue());
    }
    
    private Collector<Long, ?, NavigableMap<Long, Object>> makeDataCollector() {
        return Collectors.toMap(
                Function.identity(),
                Number::doubleValue,
                (a, b) -> a,
                TreeMap::new);
    }

    private NavigableMap<Long, Object> makeData(Long nSamples) {
        return LongStream.range(0, nSamples)
                .boxed()
                .collect(makeDataCollector());
    }

    private NavigableMap<Long, Object> makeMap(List<Long> chunk) {
        return chunk.stream().collect(makeDataCollector());
    }

}
