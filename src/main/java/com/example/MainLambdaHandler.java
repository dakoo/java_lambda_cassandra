package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MainLambdaHandler implements RequestHandler<KafkaEvent, String> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // ✅ Counters for tracking execution results
    private final AtomicInteger totalRecords = new AtomicInteger();
    private final AtomicInteger successfulWrites = new AtomicInteger();
    private final AtomicInteger conditionalCheckFailedCount = new AtomicInteger();
    private final AtomicInteger otherFailedWrites = new AtomicInteger();

    @Override
    public String handleRequest(KafkaEvent event, Context context) {
        // ✅ Reset counters for the new invocation
        totalRecords.set(0);
        successfulWrites.set(0);
        conditionalCheckFailedCount.set(0);
        otherFailedWrites.set(0);

        if (event == null) {
            String message = "No event data";
            log.warn(message);
            return message;
        }

        try {
            String jsonEvent = objectMapper.writeValueAsString(event);
            log.info("Received Lambda Event:\n" + jsonEvent + "\n");
        } catch (Exception e) {
            String message = "Failed to serialize event to JSON: " + e.getMessage();
            log.error(message);
            throw new RuntimeException(message);
        }

        // 1) Load config from environment
        EnvironmentConfig config = EnvironmentConfig.loadFromSystemEnv();
        log.info("Loaded environment: {}", config);

        // 2) Flatten the Kafka event
        List<KafkaEvent.KafkaEventRecord> records = KafkaEventFlattener.flatten(event);
        log.info("Flattened {} record(s).", records.size());

        // 3) Create the parser via factory
        ParserInterface<?> parser = ParserFactory.createParser(config.getParserName());
        log.info("Using parser: {}", config.getParserName());

        // 4) Create the DynamoDB async client + writer
        CassandraClientProvider cassandraClientProvider= new CassandraClientProvider();
        CqlSession session = cassandraClientProvider.getSession();
        AsyncCassandraWriter writer = new AsyncCassandraWriter(session, config, parser.getModelClass());
        List<Object> models = new ArrayList<>();
        // 5) For each record, parse + prepare writes
        for (KafkaEvent.KafkaEventRecord r : records) {
            try {
                // parse
                Object modelObj = parser.parseRecord(r);
                if (modelObj != null) {
                    log.debug("Parsed model: {}", modelObj);
                    totalRecords.incrementAndGet();
                    models.add(modelObj);
                }
            } catch (Exception e) {
                log.error("Error parsing record offset={} partition={}: {}",
                        r.getOffset(), r.getPartition(), e.getMessage(), e);
                otherFailedWrites.incrementAndGet();
            }
        }

        // 6) If not DRY_RUN, do asynchronous writes with concurrency checks
        if (!config.isDryRun()) {
            writer.executeAsyncWrites(models);
        } else {
            log.info("DRY_RUN=true, skipping DynamoDB writes.");
        }

        log.info("\nLambda Execution Summary:\n");
        log.info("  - Total Records Processed: " + totalRecords.get() + "\n");
        log.info("  - Successfully Written Records: " + successfulWrites.get() + "\n");
        log.info("  - ConditionalCheckFailedException Records: " + conditionalCheckFailedCount.get() + "\n");
        log.info("  - Other Failed Records: " + otherFailedWrites.get() + "\n");
        if (otherFailedWrites.get() > 0) {
            throw new RuntimeException("Failed to process some records");
        }
        return "completed";
    }
}
