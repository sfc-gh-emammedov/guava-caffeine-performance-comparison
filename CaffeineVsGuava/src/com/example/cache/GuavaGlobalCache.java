package com.example.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.concurrent.TimeUnit;

public class GuavaGlobalCache implements GlobalCache {

    Cache<Long, String> cache;

    GuavaGlobalCache() {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(200L * 1024L * 1024L)
                .recordStats()
                .weigher(
                        (Weigher<Long, String>)
                                (cacheKey, entry) -> 80)
                .removalListener(
                        notification -> {
                            // dummy listener
                        })
                .expireAfterWrite(60L * 60L * 2L, TimeUnit.SECONDS)
                .expireAfterAccess(60L * 60L * 2L, TimeUnit.SECONDS)
                .build();
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
