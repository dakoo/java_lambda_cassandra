package com.example;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnvironmentConfig {
    private String cassandraTableName;
    private String parserName;
    private boolean dryRun;

    public static EnvironmentConfig loadFromSystemEnv() {
        String tableName = getenvOrDefault("CASSANDRA_TABLE_NAME", "DefaultTable");
        String parser = getenvOrDefault("PARSER_NAME", "DishParser");
        boolean dry = Boolean.parseBoolean(System.getenv("DRY_RUN"));

        return EnvironmentConfig.builder()
                .cassandraTableName(tableName)
                .parserName(parser)
                .dryRun(dry)
                .build();
    }

    private static String getenvOrDefault(String key, String defaultVal) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }
}
