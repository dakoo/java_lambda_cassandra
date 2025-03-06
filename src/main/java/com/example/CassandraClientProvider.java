package com.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * CassandraClientProvider for driver 3.x + SLF4J.
 * Builds a Cluster, obtains a Session, reuses it as a singleton.
 */
public class CassandraClientProvider {

    private static final Logger logger = LoggerFactory.getLogger(CassandraClientProvider.class);

    private final MappingManager manager;

    public CassandraClientProvider() {
        // Read environment variables
        String contactPointsStr = System.getenv("CASSANDRA_CONTACT_POINTS"); // e.g. "host1,host2"
        String localDc = System.getenv("CASSANDRA_LOCAL_DC");               // e.g. "datacenter1"
        String portStr = System.getenv("CASSANDRA_PORT");                   // e.g. "9042"

        if (contactPointsStr == null || contactPointsStr.isEmpty()) {
            contactPointsStr = "localhost";
            logger.warn("CASSANDRA_CONTACT_POINTS not set. Defaulting to 'localhost'");
        }
        if (localDc == null || localDc.isEmpty()) {
            localDc = "datacenter1";
            logger.warn("CASSANDRA_LOCAL_DC not set. Defaulting to 'datacenter1'");
        }
        int port = (portStr != null && !portStr.isEmpty()) ? Integer.parseInt(portStr) : 9042;

        String[] hosts = contactPointsStr.split(",");
        for (int i = 0; i < hosts.length; i++) {
            hosts[i] = hosts[i].trim();
        }

        logger.info("Initializing Cassandra cluster with hosts={}, port={}, localDc={}",
                Arrays.toString(hosts), port, localDc);

        // Build cluster. If you have credentials, SSL, etc., do them here.
        Cluster.Builder builder = Cluster.builder();
        for (String host : hosts) {
            builder.addContactPoint(host);
        }
        builder.withPort(port);

        // local DC is typically used for load balancing policies in 3.x, e.g. DCAwareRoundRobinPolicy
        builder.withLoadBalancingPolicy(
                new com.datastax.driver.core.policies.DCAwareRoundRobinPolicy(localDc)
        );

        // If you have credentials:
        // builder.withCredentials("username", "password");

        Cluster cluster = builder.build();

        // Connect
        // If you have a keyspace already: cluster.connect("myKeyspace")
        Session session = cluster.connect();
        logger.info("Cassandra session established successfully.");
        // Create the MappingManager
        manager = new MappingManager(session);
    }

    public MappingManager getMapperManager() {
        return manager;
    }
}
