package com.enterprise.invoice.config;


import jakarta.transaction.Transactional;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class BatchSchemaInitializer implements InitializingBean {

    private final DataSource dataSource;

    public BatchSchemaInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Transactional
    @Override
    public void afterPropertiesSet() throws Exception {
        try (Connection conn = dataSource.getConnection()) {

            // Check if a key Spring Batch table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(null, null, "batch_job_instance", new String[]{"TABLE"})) {
                if (!rs.next()) {
                    // Table does not exist → create all batch tables
                    System.out.println("Batch tables not found. Creating from schema...");

                    Resource resource = new ClassPathResource("db/batch/schema-postgresql.sql");
                    String sql = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

                    // Split statements on semicolon (;) carefully
                    for (String stmt : sql.split(";")) {
                        stmt = stmt.trim();
                        if (!stmt.isEmpty()) {
                            try (Statement statement = conn.createStatement()) {
                                statement.execute(stmt);
                            }
                        }
                    }

                    System.out.println("Batch tables created successfully!");
                } else {
                    System.out.println("Batch tables already exist. Skipping creation.");
                }
            }
        }
    }
}