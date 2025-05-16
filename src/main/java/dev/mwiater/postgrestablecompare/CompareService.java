package dev.mwiater.postgrestablecompare;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.SetUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CompareService {

    private final CompareConfig compareConfig;
    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final JdbcTemplate internalJdbcTemplate;

    public void compare() throws ExecutionException, InterruptedException {

        for (Compare compare : compareConfig.getTables()) {
            internalJdbcTemplate.update(createInternalTableSql(compare));

            Set<Column> sourceColumns = getColumns(sourceJdbcTemplate, compare.getSourceTable(), compare.getSourceSchema());
            Set<Column> targetColumns = getColumns(targetJdbcTemplate, compare.getTargetTable(), compare.getTargetSchema());

            Set<Column> intersection = SetUtils.intersection(sourceColumns, targetColumns);

            log.info("Compare: {}", compare);
            log.info("Exists Only In Source: {}", SetUtils.difference(sourceColumns, targetColumns));
            log.info("Exists Only In Target: {}", SetUtils.difference(targetColumns, sourceColumns));
            log.info("Intersection: {}", intersection);

            String sourceHashSql = createSourceHashSql(compare.getInternalTableName());
            String targetHashSql = createTargetHashSql(compare.getInternalTableName());

            try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
                Future<?> sourceFuture = executor.submit(() -> executeHash(sourceJdbcTemplate, intersection, compare.getSourceTable(), compare.getSourceSchema(), sourceHashSql));
                Future<?> targetFuture = executor.submit(() -> executeHash(targetJdbcTemplate, intersection, compare.getTargetTable(), compare.getTargetSchema(), targetHashSql));

                sourceFuture.get();
                targetFuture.get();
            }

            log.warn("DIFFERENCE: {}", internalJdbcTemplate.queryForList("SELECT * FROM " + compare.getInternalTableName()));
        }
    }

    private String createSourceHashSql(String table) {
        return String.format("""
                MERGE INTO %s AS T
                    USING (VALUES (?, ?)) incoming(id, hash)
                ON T.id = incoming.id
                WHEN NOT MATCHED THEN
                    INSERT (id, source_hash)
                    VALUES (incoming.id, incoming.hash)
                WHEN MATCHED AND (T.target_hash = incoming.hash) THEN
                    DELETE
                WHEN MATCHED AND (T.target_hash <> incoming.hash) THEN
                    UPDATE
                    SET T.source_hash = incoming.hash;
                """, table);
    }

    private String createTargetHashSql(String table) {
        return String.format("""
                MERGE INTO %s AS T
                    USING (VALUES (?, ?)) incoming(id, hash)
                ON T.id = incoming.id
                WHEN NOT MATCHED THEN
                    INSERT (id, target_hash)
                    VALUES (incoming.id, incoming.hash)
                WHEN MATCHED AND (T.source_hash = incoming.hash) THEN
                    DELETE
                WHEN MATCHED AND (T.source_hash <> incoming.hash) THEN
                    UPDATE
                    SET T.target_hash = incoming.hash;
                """, table);
    }

    private void executeHash(JdbcTemplate template, Set<Column> intersection, String table, String schema, String hashSql) {
        String sql = String.format("SELECT %s AS id, md5(cast((%s) AS TEXT)) AS hash FROM %s.%s", "id", intersection.stream().map(Column::name).collect(Collectors.joining(",")), schema, table);

        template.query(sql, rs -> {
            log.info("ID: {} Hash: {}", rs.getLong("id"), rs.getString("hash"));
            internalJdbcTemplate.update(hashSql, rs.getLong("id"), rs.getString("hash"));
        });
    }

    private String createInternalTableSql(Compare compare) {
        return String.format("""
                CREATE TABLE public.%s
                (
                    id            BIGINT PRIMARY KEY,
                    source_hash    VARCHAR(32),
                    target_hash     VARCHAR(32)
                );
                """, compare.getInternalTableName());
    }

    private Set<Column> getColumns(JdbcTemplate template, String table, String schema) {
        return new HashSet<>(template.query(getColumnsSql(table, schema), (rs, _) -> {
            Column col = new Column(rs.getString("column_name"), rs.getString("data_type"));
            log.info("{}", col);
            return col;
        }));
    }

    private String getColumnsSql(String table, String schema) {
        return String.format("""
                SELECT column_name, data_type
                  FROM information_schema.columns
                 WHERE table_schema = '%s'
                   AND table_name   = '%s'
                """, schema, table);
    }

    private record Column(String name, String type) {
    }
}
