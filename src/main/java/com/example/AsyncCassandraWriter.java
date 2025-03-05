package com.example;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * AsyncCassandraWriter using the DataStax Java Driver 3.x with SLF4J logging.
 * - Reuses a single PreparedStatement for all inserts (reflection-based)
 * - Uses "USING TIMESTAMP <version>" for concurrency
 * - Writes each item asynchronously (one row per call) in parallel.
 */
public class AsyncCassandraWriter {

    private static final Logger logger = LoggerFactory.getLogger(AsyncCassandraWriter.class);

    private final Session session;         // 3.x driver uses Session (not CqlSession)
    private final EnvironmentConfig config;

    // Model reflection
    private final Class<?> modelClass;
    private final Field partitionKeyField;
    private final Field versionKeyField;
    private final List<Field> columnFields; // includes partitionKey, excludes versionKey

    // Pre-built prepared statement for reuse
    private final PreparedStatement preparedStatement;

    /**
     * @param session     DataStax 3.x Session
     * @param config      environment config (table name, etc.)
     * @param modelClass  the class to reflect for columns
     */
    public AsyncCassandraWriter(Session session, EnvironmentConfig config, Class<?> modelClass) {
        this.session = session;
        this.config = config;
        this.modelClass = modelClass;

        // 1) Reflect the model class
        ReflectionResult reflectionResult = reflectModelClass(modelClass);
        this.partitionKeyField = reflectionResult.partitionKey;
        this.versionKeyField = reflectionResult.versionKey;
        this.columnFields = reflectionResult.columnFields;

        // 2) Build the single prepared statement
        this.preparedStatement = createPreparedStatement();
    }

    /**
     * Asynchronously writes each item in parallel using session.executeAsync(...).
     * Returns a CompletableFuture<Void> that completes when all writes are done.
     */
    public void executeAsyncWrites(List<Object> items) {
        if (items == null || items.isEmpty()) {
            logger.info("No items to write. Returning completed future.");
            return;
        }

        logger.info("executeAsyncWrites called with {} item(s).", items.size());

        // We'll gather a list of futures (CompletableFutures) for each item
        List<CompletableFuture<ResultSet>> futures = new ArrayList<>(items.size());

        for (Object item : items) {
            BoundStatement boundStmt = buildBoundStatement(item);
            // session.executeAsync returns a Guava ListenableFuture<ResultSet>
            ListenableFuture<ResultSet> listenable = session.executeAsync(boundStmt);

            // Convert ListenableFuture to CompletableFuture
            CompletableFuture<ResultSet> completable = toCompletableFuture(listenable, item);

            futures.add(completable);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    }

    /**
     * Build the BoundStatement from the single prepared statement,
     * binding all column fields + version as the final TIMESTAMP.
     */
    private BoundStatement buildBoundStatement(Object item) {
        try {
            // Extract the version
            long versionValue = (long) versionKeyField.get(item);

            // Prepare an array of bind values: [colAVal, colBVal, partitionKeyVal, ..., versionVal]
            Object[] bindValues = new Object[columnFields.size() + 1];

            int i = 0;
            for (Field f : columnFields) {
                bindValues[i] = f.get(item);
                i++;
            }
            // The last slot is the version
            bindValues[columnFields.size()] = versionValue;

            return preparedStatement.bind(bindValues);

        } catch (IllegalAccessException e) {
            logger.error("Failed to build BoundStatement for item: {}", item, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Create the single PreparedStatement using reflection data:
     * "INSERT INTO <table> (col1, col2, ..., pk) VALUES (?, ?, ?, ...) USING TIMESTAMP ?"
     */
    private PreparedStatement createPreparedStatement() {
        String tableName = config.getCassandraTableName();

        // Gather column names from columnFields
        List<String> colNames = columnFields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        String joinedColumns = String.join(", ", colNames);

        // For each column, we have a "?"
        String questionMarks = colNames.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        // final "?" is for the version in USING TIMESTAMP ?
        String cql = String.format(
                "INSERT INTO %s (%s) VALUES (%s) USING TIMESTAMP ?",
                tableName, joinedColumns, questionMarks
        );

        logger.info("Preparing CQL statement: {}", cql);
        return session.prepare(cql);
    }

    /**
     * Convert a Guava ListenableFuture<ResultSet> to a Java 8 CompletableFuture<ResultSet>,
     * adding logging for success/failure.
     */
    private CompletableFuture<ResultSet> toCompletableFuture(
            ListenableFuture<ResultSet> listenableFuture,
            Object item
    ) {
        CompletableFuture<ResultSet> cf = new CompletableFuture<>();

        Futures.addCallback(
                listenableFuture,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        logger.debug("Write succeeded for item: {}", item);
                        cf.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.error("Write failed for item {}: {}", item, t.getMessage(), t);
                        cf.completeExceptionally(t);
                    }
                },
                MoreExecutors.directExecutor() // you can use a dedicated executor if preferred
        );

        return cf;
    }

    /**
     * Reflect the model class to find a single @PartitionKey, a single @VersionKey, and all other fields as columns.
     */
    private ReflectionResult reflectModelClass(Class<?> cls) {
        Field pk = null;
        Field vk = null;
        List<Field> cols = new ArrayList<>();

        for (Field f : cls.getDeclaredFields()) {
            f.setAccessible(true);
            if (f.isAnnotationPresent(PartitionKey.class)) {
                if (pk != null) {
                    throw new RuntimeException("Multiple @PartitionKey fields in " + cls.getName());
                }
                pk = f;
            } else if (f.isAnnotationPresent(VersionKey.class)) {
                if (vk != null) {
                    throw new RuntimeException("Multiple @VersionKey fields in " + cls.getName());
                }
                vk = f;
            } else {
                cols.add(f);
            }
        }

        if (pk == null) {
            throw new RuntimeException("No @PartitionKey field in " + cls.getName());
        }
        if (vk == null) {
            throw new RuntimeException("No @VersionKey field in " + cls.getName());
        }

        // The partition key is also a normal column
        cols.add(pk);

        // The version key is used only for the TIMESTAMP, not as a normal column
        return new ReflectionResult(pk, vk, cols);
    }

    // Helper class to hold reflection results
    private static class ReflectionResult {
        final Field partitionKey;
        final Field versionKey;
        final List<Field> columnFields;

        ReflectionResult(Field pk, Field vk, List<Field> cf) {
            this.partitionKey = pk;
            this.versionKey = vk;
            this.columnFields = cf;
        }
    }
}
