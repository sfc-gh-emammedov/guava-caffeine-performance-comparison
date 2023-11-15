package com.example.cache;

public interface GlobalCache {
    String getIfPresent(Long key);

    void putOrMerge(Long key, String value);
}
