package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An AsyncCassandraWriter that:
 *  - Reflects a single model class at construction to identify columns
 *  - Builds and reuses ONE PreparedStatement for all inserts
 *  - Uses "USING TIMESTAMP <version>" for concurrency
 *  - Writes each item asynchronously (single-row inserts in parallel).
 */
@Slf4j
public class AsyncCassandraWriter {

    private final CqlSession session;
    private final EnvironmentConfig config;

    // Model reflection
    private final Class<?> modelClass;
    private final Field partitionKeyField;
    private final Field versionKeyField;
    private final List<Field> columnFields;      // includes partitionKeyField, excludes versionKeyField

    // Pre-built prepared statement for reuse
    private final PreparedStatement preparedStatement;

    /**
     * Constructs the writer, reflecting on the given modelClass to discover
     * all columns, partition key, and version key. Then builds a single
     * prepared statement for repeated use.
     *
     * @param session the Cassandra session
     * @param config  environment config (includes table name, etc.)
     * @param modelClass the class to reflect, which must contain exactly one @PartitionKey and one @VersionKey
     */
    public AsyncCassandraWriter(CqlSession session, EnvironmentConfig config, Class<?> modelClass) {
        this.session = session;
        this.config = config;
        this.modelClass = modelClass;

        // 1) Reflect the model class
        ReflectionResult reflectionResult = reflectModelClass(modelClass);
        this.partitionKeyField = reflectionResult.partitionKey;
        this.versionKeyField = reflectionResult.versionKey;
        this.columnFields = reflectionResult.columnFields;

        // 2) Build & store the single prepared statement
        this.preparedStatement = createPreparedStatement();
    }

    /**
     * Asynchronously writes a list of items to Cassandra using the single re-used prepared statement.
     * Returns a CompletableFuture that finishes when all inserts are done.
     */
    public void executeAsyncWrites(List<Object> items) {
        if (items == null || items.isEmpty()) {
            log.info("No items to write. Returning completed future.");
            return;
        }

        log.info("executeAsyncWrites called with {} item(s).", items.size());
        List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>(items.size());

        for (Object item : items) {
            BoundStatement bound = buildBoundStatement(item);
            CompletableFuture<AsyncResultSet> future =
                    session.executeAsync(bound)
                            .toCompletableFuture()
                            .thenApply(rs -> {
                                log.debug("Write succeeded for item: {}", item);
                                return rs;
                            })
                            .exceptionally(ex -> {
                                log.error("Write failed for item {}: {}", item, ex.getMessage(), ex);
                                return null;
                            });

            futures.add(future);
        }

        // Combine them all
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    /**
     * Builds the BoundStatement for a single item, using the re-used PreparedStatement.
     * The version is appended as the last bind parameter for 'USING TIMESTAMP ?'.
     */
    private BoundStatement buildBoundStatement(Object item) {
        try {
            // 1) Extract the version
            long versionValue = (long) versionKeyField.get(item);

            // 2) Extract column values in the same order we built them
            Object[] bindValues = new Object[columnFields.size() + 1];

            int i = 0;
            for (Field field : columnFields) {
                bindValues[i] = field.get(item);
                i++;
            }

            // The last slot is the version for USING TIMESTAMP
            bindValues[columnFields.size()] = versionValue;

            // 3) Bind & return
            return preparedStatement.bind(bindValues);

        } catch (IllegalAccessException e) {
            log.error("Failed to bind statement for item {}", item, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Reflects the model class to find @PartitionKey, @VersionKey, and normal columns.
     * Returns a ReflectionResult containing the fields. 
     */
    private ReflectionResult reflectModelClass(Class<?> modelCls) {
        Field partitionKey = null;
        Field versionKey = null;
        List<Field> allColumns = new ArrayList<>();

        for (Field f : modelCls.getDeclaredFields()) {
            f.setAccessible(true);

            if (f.isAnnotationPresent(PartitionKey.class)) {
                if (partitionKey != null) {
                    throw new RuntimeException("Multiple @PartitionKey fields found in " + modelCls.getName());
                }
                partitionKey = f;
            } else if (f.isAnnotationPresent(VersionKey.class)) {
                if (versionKey != null) {
                    throw new RuntimeException("Multiple @VersionKey fields found in " + modelCls.getName());
                }
                versionKey = f;
            } else {
                // A normal column
                allColumns.add(f);
            }
        }

        if (partitionKey == null) {
            throw new RuntimeException("No @PartitionKey found in " + modelCls.getName());
        }
        if (versionKey == null) {
            throw new RuntimeException("No @VersionKey found in " + modelCls.getName());
        }

        // Also add the partition key field as part of the columns to insert
        allColumns.add(partitionKey);

        // The version key is not added to 'allColumns' because we pass it in USING TIMESTAMP
        // (not as a normal column).

        return new ReflectionResult(partitionKey, versionKey, allColumns);
    }

    /**
     * Builds the PreparedStatement (INSERT ... USING TIMESTAMP ?) 
     * from the discovered columns in the constructor.
     */
    private PreparedStatement createPreparedStatement() {
        String tableName = config.getCassandraTableName();

        // Gather column names (for all fields except the version field).
        List<String> columnNames = columnFields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        // Prepare placeholders for all columns + 1 for the TIMESTAMP
        // Actually, the last placeholder is for the TIMESTAMP in "USING TIMESTAMP ?"
        // The columns themselves get placeholders in the VALUES portion
        String joinedColumns = String.join(", ", columnNames);
        String questionMarks = columnNames.stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        // e.g. "INSERT INTO tableName (colA, colB, id) VALUES (?, ?, ?) USING TIMESTAMP ?"
        String cql = String.format(
                "INSERT INTO %s (%s) VALUES (%s) USING TIMESTAMP ?",
                tableName, joinedColumns, questionMarks
        );

        log.info("Preparing statement: {}", cql);
        return session.prepare(cql);
    }

    /**
     * A small holder for reflection results.
     */
    private static class ReflectionResult {
        Field partitionKey;
        Field versionKey;
        List<Field> columnFields;

        public ReflectionResult(Field pk, Field vk, List<Field> cols) {
            this.partitionKey = pk;
            this.versionKey = vk;
            this.columnFields = cols;
        }
    }
}
