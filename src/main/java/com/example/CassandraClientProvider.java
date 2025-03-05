package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple provider that initializes and returns a Cassandra CqlSession.
 * Customize as needed for your environment (multiple contact points, etc.).
 */
@Slf4j
public class CassandraClientProvider {

    private final CqlSession session;

    public CassandraClientProvider() {
        // Read environment variables
        String contactPointsStr = System.getenv("CASSANDRA_CONTACT_POINTS");
        String localDc = System.getenv("CASSANDRA_LOCAL_DC");

        if (contactPointsStr == null || contactPointsStr.isEmpty()) {
            // Provide a default for local testing or throw an exception if mandatory
            contactPointsStr = "localhost:9042";
            log.warn("CASSANDRA_CONTACT_POINTS not set. Defaulting to localhost:9042");
        }

        if (localDc == null || localDc.isEmpty()) {
            // Provide a default DC name or throw an exception if mandatory
            localDc = "datacenter1";
            log.warn("CASSANDRA_LOCAL_DC not set. Defaulting to datacenter1");
        }

        // Parse contact points. For example: "host1:9042,host2:9042"
        List<InetSocketAddress> contactPoints = parseContactPoints(contactPointsStr);

        log.info("Initializing CqlSession with contactPoints={} and localDc={}",
                contactPoints, localDc);

        // Build the session
        this.session = CqlSession.builder()
                .withLocalDatacenter(localDc)
                .addContactPoints(contactPoints)
                .build();

        log.info("CqlSession created successfully.");
    }

    /**
     * Returns the singleton CqlSession for your Lambda container.
     */
    public CqlSession getSession() {
        return session;
    }

    /**
     * Parses a comma-separated list of host:port strings into InetSocketAddress objects.
     */
    private List<InetSocketAddress> parseContactPoints(String contactPointsStr) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        String[] parts = contactPointsStr.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                String[] hostPort = trimmed.split(":");
                if (hostPort.length == 2) {
                    String host = hostPort[0];
                    int port = Integer.parseInt(hostPort[1]);
                    addresses.add(new InetSocketAddress(host, port));
                } else {
                    log.warn("Invalid contact point format: {} (expected host:port)", trimmed);
                }
            }
        }
        return addresses;
    }
}
