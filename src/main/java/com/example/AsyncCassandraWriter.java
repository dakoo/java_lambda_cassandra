package com.example;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Mapper.Option;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import com.example.annotations.VersionKey;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An AsyncCassandraWriter that:
 *  1) Uses DataStax's Mapper<Object> (3.x) for the entity.
 *  2) Finds one field annotated with @VersionKey, applying it as the TIMESTAMP
 *     so that only columns with an older timestamp get overwritten.
 *  3) Executes all writes asynchronously in parallel, returning a single future.
 */
@Slf4j
public class AsyncCassandraWriter {

    private final Mapper<Object> mapper;
    private final Field versionField;

    /**
     * @param manager    the MappingManager from the driver
     * @param modelClass the entity class discovered at runtime (e.g. Dish.class)
     */
    @SuppressWarnings("unchecked")
    public AsyncCassandraWriter(MappingManager manager, Class<?> modelClass) {
        // Cast modelClass to Class<Object> to avoid "raw type" warnings:
        Class<Object> castedClass = (Class<Object>) modelClass;

        // Build the mapper for that class
        this.mapper = manager.mapper(castedClass);

        // Reflect to find the single field annotated with @VersionKey
        this.versionField = findVersionField(castedClass);

        log.info("AsyncCassandraWriter initialized for model={}, versionField={}",
                castedClass.getSimpleName(), versionField.getName());
    }

    /**
     * Asynchronously writes all items, passing "Option.timestamp(...)" with the entity's versionKey.
     * If a column in Cassandra is newer, it remains; older columns get overwritten.
     */
    public void executeAsyncWrites(List<Object> items) {
        if (items == null || items.isEmpty()) {
            log.info("No items to write, returning completed future.");
            return;
        }

        log.info("executeAsyncWrites called with {} item(s).", items.size());
        List<CompletableFuture<Void>> futures = new ArrayList<>(items.size());

        for (Object entity : items) {
            CompletableFuture<Void> cf = saveEntityAsync(entity);
            futures.add(cf);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    }

    /**
     * Saves a single entity with a custom TIMESTAMP (the @VersionKey field).
     */
    private CompletableFuture<Void> saveEntityAsync(Object entity) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        try {
            // Grab the version (timestamp) from the @VersionKey field
            long versionVal = (long) versionField.get(entity);

            // mapper.saveAsync(...) returns a Guava ListenableFuture<Void>
            ListenableFuture<Void> lf = mapper.saveAsync(entity, Option.timestamp(versionVal));

            // Convert it to a Java CompletableFuture
            Futures.addCallback(lf, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    log.debug("Save succeeded for entity: {}", entity);
                    cf.complete(null);
                }
                @Override
                public void onFailure(Throwable t) {
                    log.error("Save failed for entity {}: {}", entity, t.getMessage(), t);
                    cf.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());

        } catch (IllegalAccessException e) {
            log.error("Failed to read @VersionKey field on entity {}", entity, e);
            cf.completeExceptionally(e);
        }
        return cf;
    }

    /**
     * Finds the single field with @VersionKey in the given class (or throws if missing/duplicate).
     */
    private Field findVersionField(Class<?> cls) {
        Field found = null;
        for (Field f : cls.getDeclaredFields()) {
            if (f.isAnnotationPresent(VersionKey.class)) {
                if (found != null) {
                    throw new RuntimeException("Multiple @VersionKey fields in " + cls.getName());
                }
                f.setAccessible(true);
                found = f;
            }
        }
        if (found == null) {
            throw new RuntimeException("No @VersionKey field found in " + cls.getName());
        }
        return found;
    }
}
