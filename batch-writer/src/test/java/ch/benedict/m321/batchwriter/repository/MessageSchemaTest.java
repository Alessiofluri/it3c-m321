package ch.benedict.m321.batchwriter.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prueft, dass Flyway beim Start wirklich die Tabelle anlegt, die
 * PLANUNG.md 3.7 beschreibt.
 *
 * Ohne diesen Test wuerde ein Tippfehler im SQL erst im naechsten Test
 * auffallen, und dort saehe es nach einem Fehler im Repository aus.
 */
@SpringBootTest
@Testcontainers
class MessageSchemaTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void createsMessageTableWithAllColumns() {
        String sql = "SELECT column_name FROM information_schema.columns "
                + "WHERE table_name = 'message' ORDER BY column_name";

        List<String> columns = jdbcTemplate.queryForList(sql, String.class);

        List<String> expected = List.of(
                "content", "id", "room_id", "sender_id", "sender_name", "sent_at");
        assertEquals(expected, columns);
    }

    @Test
    void createsIndexForHistoryQuery() {
        String sql = "SELECT indexname FROM pg_indexes WHERE tablename = 'message'";

        List<String> indexes = jdbcTemplate.queryForList(sql, String.class);

        boolean hasHistoryIndex = indexes.contains("idx_message_room_sent_at");
        assertTrue(hasHistoryIndex, "Index fuer die Historie-Abfrage fehlt");
    }
}
