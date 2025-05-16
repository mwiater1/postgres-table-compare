package dev.mwiater.postgrestablecompare;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        // H2 In-Memory url with table creation script
//        String url = "jdbc:h2:mem:db";
//        String url = "jdbc:hsqldb:mem:db";
        String url = "jdbc:derby:derbyDB;create=true";
        // Merge SQL which should either insert if no record exists or update the name if it does
        String sql = """
                MERGE INTO users AS T
                    USING (VALUES (1, 'John')) incoming(id, name)
                ON T.id = incoming.id
                WHEN NOT MATCHED THEN
                    INSERT (id, name) VALUES (incoming.id, incoming.name)
                WHEN MATCHED THEN
                    UPDATE SET T.name = incoming.name;
                """;


        // Get two separate connections to the same h2 database
        try (Connection con1 = DriverManager.getConnection(url); Connection con2 = DriverManager.getConnection(url)) {
            execute(con1, "CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name VARCHAR(255))");
            // Create an executor with only two threads
            try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
                // Execute the merge on the first thread
                Future<Boolean> f1 = executor.submit(() -> execute(con1, sql));
                // Execute the merge on the second thread
                Future<Boolean> f2 = executor.submit(() -> execute(con2, sql));
                // Wait for the first merge to complete
                f1.get();
                // Wait for the second merge to complete
                f2.get();
            }
        }
    }

    // Execute SQL on a specific connection
    private static boolean execute(Connection con, String sql) throws SQLException {
        // Create the statement
        try (Statement stmt = con.createStatement()) {
            System.out.println(Thread.currentThread().getName() + " Exec: " + sql);
            // Execute the statement
            return stmt.execute(sql);
        }
    }
}
