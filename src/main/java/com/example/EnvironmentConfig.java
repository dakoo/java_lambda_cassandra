package com.example;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnvironmentConfig {
    private String cassandraClusterName;
    private String cassandraKeyspaceName;
    private String cassandraTableName;
    private String parserName;
    private String vaultDbRoles;
    private String vaultRoleId;
    private String vaultSecretId;
    private String stageName;
    private String vdcName;
    private boolean dryRun;

    public static EnvironmentConfig loadFromSystemEnv() {
        String cluster = System.getenv("CASSANDRA_CLUSTER");
        String keyspace = System.getenv("CASSANDRA_KEYSPACE");
        String table = System.getenv("CASSANDRA_TABLE");
        String parser = System.getenv("PARSER");
        String dbRoles = System.getenv("VAULT_DB_ROLES");
        String roleId = System.getenv("VAULT_ROLE_ID");
        String secretId = System.getenv("VAULT_SECRET_ID");
        String stage = System.getenv("STAGE");
        String vdc = System.getenv("VDC");
        boolean dry = Boolean.parseBoolean(System.getenv("DRY_RUN"));

        return EnvironmentConfig.builder()
                .cassandraClusterName(cluster)
                .cassandraKeyspaceName(keyspace)
                .cassandraTableName(table)
                .parserName(parser)
                .vaultDbRoles(dbRoles)
                .vaultRoleId(roleId)
                .vaultSecretId(secretId)
                .stageName(stage)
                .vdcName(vdc)
                .dryRun(dry)
                .build();
    }
}
