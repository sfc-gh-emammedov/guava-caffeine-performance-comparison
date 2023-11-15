package com.example.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CaffeineVsGuavaTest {
    private final int THREAD_COUNT = 10;

    private final int UNIQUE_ENTITIES_COUNT = 1000;

    private final long TOTAL_OPERATIONS_PER_THREAD = 100L;

    private final double READ_RATIO = 0.9; // Adjust this ratio to shift between reads and writes

    private final GlobalCache caffeineCache = new CaffeineGlobalCache(true);

    private final GlobalCache caffeineWithoutExpirationCache = new CaffeineGlobalCache(false);

    private final GlobalCache guavaCache = new GuavaGlobalCache();

    @Test
    public void testConcurrentCacheOperations() throws InterruptedException {
        System.out.println("Guava");
        testConcurrentCacheOperationsHelper(guavaCache);
        System.out.println();
        System.out.println("Caffeine");
        testConcurrentCacheOperationsHelper(caffeineCache);
        System.out.println();
        System.out.println("Caffeine without expiration");
        testConcurrentCacheOperationsHelper(caffeineWithoutExpirationCache);
    }

    public void testConcurrentCacheOperationsHelper(GlobalCache cache)
            throws InterruptedException {

        int numberOfIterations = 100;
        long totalTimeNs = 0;
        long totalAvgReadDur = 0;
        long totalAvgWriteDur = 0;
        for (int i = 0; i < numberOfIterations; i++) {
            long startNs = System.nanoTime();
            var avgDurs = concurrentCacheOperationsBenchmarkRunner(cache);
            totalAvgReadDur += avgDurs.getLeft();
            totalAvgWriteDur += avgDurs.getRight();
            totalTimeNs += (System.nanoTime() - startNs);
        }
        var caffeineAvgDur = (totalTimeNs / numberOfIterations);
        System.out.println("Avg benchmark dur:" + caffeineAvgDur);
        System.out.println("Avg read dur:" + totalAvgReadDur / numberOfIterations);
        System.out.println("Avg write dur:" + totalAvgWriteDur / numberOfIterations);
    }

    public Pair<Long, Long> concurrentCacheOperationsBenchmarkRunner(GlobalCache cache)
            throws InterruptedException {
        long totalAvgReadDur = 0;
        long totalAvgWriteDur = 0;

        // Prepare values and keys for the test
        var dummyEntry = "hello";
        List<Long> keys = new ArrayList<>();
        for (long i = 0; i < UNIQUE_ENTITIES_COUNT; i++) {
            keys.add(i);
        }

        // Init the executor
        final int readersCount = (int) (THREAD_COUNT * READ_RATIO);
        final int writersCount = THREAD_COUNT - readersCount;
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CacheTester> readerThreads = new ArrayList<>();
        List<CacheTester> writerThreads = new ArrayList<>();

        // Prepare the tasks
        int numKeysPerReaderThread = keys.size() / readersCount;
        int curKeyIndex = 0;
        for (int i = 0; i < readersCount; i++) {
            List<Long> curReaderThreadKeys = new ArrayList<>();
            for (int j = 0; j < numKeysPerReaderThread; j++) {
                curReaderThreadKeys.add(keys.get(curKeyIndex));
                curKeyIndex++;
            }
            readerThreads.add(
                    new CacheTester(
                            cache, true, TOTAL_OPERATIONS_PER_THREAD, curReaderThreadKeys, dummyEntry));
        }

        int numKeysPerWriterThread = writersCount > 0 ? keys.size() / writersCount : 0;
        curKeyIndex = 0;
        for (int i = 0; i < writersCount; i++) {
            List<Long> curWriterThreadKeys = new ArrayList<>();
            for (int j = 0; j < numKeysPerWriterThread; j++) {
                curWriterThreadKeys.add(keys.get(curKeyIndex));
                curKeyIndex++;
            }
            writerThreads.add(
                    new CacheTester(
                            cache, false, TOTAL_OPERATIONS_PER_THREAD, curWriterThreadKeys, dummyEntry));
        }

        // Submit the tasks and wait
        ArrayList<CacheTester> allThreads = new ArrayList<>();
        allThreads.addAll(readerThreads);
        allThreads.addAll(writerThreads);
        try {
            List<Future<Long>> futures = executor.invokeAll(allThreads);

            for (Future<Long> future : futures) {
                long avgDur = future.get();
                if (avgDur > 0) {
                    totalAvgReadDur += avgDur;
                } else {
                    totalAvgWriteDur += -avgDur;
                }
            }
        } catch (ExecutionException e) {
            Assertions.fail(e);
        }

        // Shutdown the executor
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        return Pair.of(
                totalAvgReadDur / readersCount, writersCount > 0 ? totalAvgWriteDur / writersCount : 0);
    }

    private static class CacheTester implements Callable<Long> {
        private final GlobalCache cache;

        private final boolean isReader; // if true, it is reader; if false, it is writer

        private final long opsNum;

        private final List<Long> keys;

        private final String value;

        public CacheTester(
                GlobalCache cache,
                boolean isReader,
                long opsNum,
                List<Long> keys,
                String value) {
            this.cache = cache;
            this.isReader = isReader;
            this.opsNum = opsNum;
            this.keys = keys;
            this.value = value;
        }

        @Override
        public Long call() {
            long startNs = System.nanoTime();

            for (long j = 0; j < opsNum; j++) {
                for (Long key : keys) {
                    if (isReader) {
                        cache.getIfPresent(key);
                    } else {
                        cache.putOrMerge(key, value);
                    }
                }
            }

            if (isReader) {
                return (System.nanoTime() - startNs) / (opsNum * keys.size());
            } else {
                return (startNs - System.nanoTime()) / (opsNum * keys.size());
            }
        }
    }
}

