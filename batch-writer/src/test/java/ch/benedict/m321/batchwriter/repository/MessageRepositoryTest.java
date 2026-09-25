package ch.benedict.m321.batchwriter.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prueft den Kern des Dienstes an einer echten Datenbank: schreibt der
 * Stapel-INSERT, und was passiert bei einer doppelten ID?
 *
 * Bewusst ohne Queue. Faellt dieser Test durch, liegt es an der
 * Datenbankseite und nicht am Broker.
 */
@SpringBootTest
@Testcontainers
class MessageRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Die Tabelle ist gemeinsamer Zustand ueber alle Testmethoden
     * hinweg. Ohne Leeren wuerde die zweite Methode die Zeilen der
     * ersten mitzaehlen.
     */
    @BeforeEach
    void emptyMessageTable() {
        jdbcTemplate.execute("TRUNCATE TABLE message");
    }

    @Test
    void writesWholeBatch() {
        List<MessageRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageRow row = createRow(UUID.randomUUID(), "Nachricht " + i);
            rows.add(row);
        }

        messageRepository.insertBatch(rows);

        int count = countMessages();
        assertEquals(10, count);
    }

    @Test
    void ignoresDuplicateId() {
        UUID sameId = UUID.randomUUID();
        MessageRow first = createRow(sameId, "Original");
        MessageRow duplicate = createRow(sameId, "Zweiter Versuch");

        messageRepository.insertBatch(List.of(first));
        messageRepository.insertBatch(List.of(duplicate));

        int count = countMessages();
        assertEquals(1, count);

        // Die erste Fassung bleibt stehen: DO NOTHING, nicht DO UPDATE.
        String content = jdbcTemplate.queryForObject(
                "SELECT content FROM message WHERE id = ?", String.class, sameId);
        assertEquals("Original", content);
    }

    /** Zaehlt die Zeilen der Tabelle, damit die Tests kurz bleiben. */
    private int countMessages() {
        Integer count = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
        return count;
    }

    /** Baut eine vollstaendige Zeile mit den uebergebenen Unterschieden. */
    private MessageRow createRow(UUID id, String content) {
        UUID roomId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new MessageRow(id, roomId, "anna", "Anna Muster", content, sentAt);
    }
}
