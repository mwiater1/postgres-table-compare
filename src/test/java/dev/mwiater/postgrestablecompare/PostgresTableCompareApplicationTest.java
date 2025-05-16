package dev.mwiater.postgrestablecompare;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Testcontainers
@SpringBootTest
@TestPropertySource(properties = """
        spring.datasource.internal.url=jdbc:h2:mem:myDb
        spring.datasource.source.url=jdbc:tc:postgresql:17.2-alpine:///source?TC_INITSCRIPT=file:src/test/resources/init_source.sql
        spring.datasource.target.url=jdbc:tc:postgresql:17.2-alpine:///target?TC_INITSCRIPT=file:src/test/resources/init_target.sql
        compare.tables[0].sourceTable=users
        compare.tables[0].sourceSchema=test
        compare.tables[0].targetTable=users
        compare.tables[0].targetSchema=test
        
        compare.tables[1].sourceTable=users
        compare.tables[1].sourceSchema=test
        compare.tables[1].targetTable=users_missing_email
        compare.tables[1].targetSchema=test
        
        compare.tables[2].sourceTable=users_missing_email
        compare.tables[2].sourceSchema=test
        compare.tables[2].targetTable=users
        compare.tables[2].targetSchema=test
        
        compare.tables[3].sourceTable=users_missing_email
        compare.tables[3].sourceSchema=test
        compare.tables[3].targetTable=users_missing_last_name
        compare.tables[3].targetSchema=test
        """)
public class PostgresTableCompareApplicationTest {

    @Autowired
    private CompareService compareService;

    @Test
    public void contextLoads() throws ExecutionException, InterruptedException {
        compareService.compare();

        assertTrue(true);
    }

    @Test
    public void testNoSpring() {
        String url = "jdbc:h2:mem:db;INIT=CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name VARCHAR(255))";
        String sql = """
                MERGE INTO users AS T
                    USING (VALUES (1, 'John')) incoming(id, name)
                ON T.id = incoming.id
                WHEN NOT MATCHED THEN
                    INSERT (id, name) VALUES (incoming.id, incoming.name)
                WHEN MATCHED THEN
                    UPDATE SET T.name = incoming.name;
                """;

        assertDoesNotThrow(() -> {
            try(Connection con1 = DriverManager.getConnection(url); Connection con2 = DriverManager.getConnection(url)) {
                try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
                    Future<Boolean> f1 = executor.submit(() -> execute(con1, sql));
                    Future<Boolean> f2 = executor.submit(() -> execute(con2, sql));
                    f1.get();
                    f2.get();
                }
            }
        });
    }


    private boolean execute(String url, String sql) throws SQLException {
        try (Connection con = DriverManager.getConnection(url)) {
            return execute(con, sql);
        }
    }

    private boolean execute(Connection con, String sql) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            return stmt.execute(sql);
        }
    }
}