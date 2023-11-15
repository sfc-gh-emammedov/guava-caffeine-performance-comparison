package com.example.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

import java.util.concurrent.TimeUnit;

public class CaffeineGlobalCache implements GlobalCache {

    Cache<Long, String> cache;

    CaffeineGlobalCache(boolean withExpiration) {
        var caffeineBuilder = Caffeine.newBuilder()
                .maximumWeight(200L * 1024L * 1024L)
                .recordStats()
                .weigher(
                        (Weigher<Long, String>)
                                (cacheKey, entry) -> 80)
                .removalListener(
                        (key, entry, cause) -> {
                            // dummy listener
                        })
                // .scheduler(Scheduler.systemScheduler())
                // .executor(Executors.newFixedThreadPool(5))
                ;

        if (withExpiration) {
            caffeineBuilder.expireAfterWrite(60L * 60L * 2L, TimeUnit.SECONDS)
                    .expireAfterAccess(60L * 60L * 2L, TimeUnit.SECONDS);
        }

        cache = caffeineBuilder.build();
    }

    @Override
    public String getIfPresent(Long key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void putOrMerge(Long key, String value) {
        cache
                .asMap()
                .merge(
                        key,
                        value,
                        // dummy remapping function
                        (existingEntry, newEntry) -> newEntry);
    }
}
