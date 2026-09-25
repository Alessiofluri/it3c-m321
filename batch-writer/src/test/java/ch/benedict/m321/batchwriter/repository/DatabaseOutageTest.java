package ch.benedict.m321.batchwriter.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

/**
 * T-7: Szenario S7 — die Datenbank ist kurz weg.
 *
 * Erwartet wird: Der Dienst stirbt nicht, braucht keinen Neustart von
 * Hand und wirft nichts ins Abstellgleis. Er wartet, versucht es erneut,
 * und sobald die Datenbank antwortet, steht der Stapel in der Tabelle.
 *
 * Der Ausfall wird hier nachgestellt, statt den Container anzuhalten.
 * Grund: Ein angehaltener Testcontainer bekommt beim Start eine neue
 * Portnummer, und dann scheitert der Test an der Testumgebung statt an
 * unserem Code. Nachgestellt wird genau das, was Postgres im Ausfall
 * liefert — eine DataAccessResourceFailureException.
 */
@SpringBootTest
@Testcontainers
class DatabaseOutageTest {

    /** So viele Versuche schlagen fehl, bevor die Datenbank antwortet. */
    private static final int FAILURES_BEFORE_RECOVERY = 3;

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private MessageRepository messageRepository;

    /**
     * Ein Spion auf dem echten JdbcTemplate: die ersten Aufrufe lassen
     * wir scheitern, danach arbeitet das Original ganz normal weiter.
     */
    @MockitoSpyBean
    private JdbcTemplate jdbcTemplate;

    private final AtomicInteger attempts = new AtomicInteger();

    @BeforeEach
    void emptyMessageTable() {
        attempts.set(0);
        jdbcTemplate.execute("TRUNCATE TABLE message");
    }

    @Test
    void retriesUntilDatabaseIsBackAndThenWrites() {
        letFirstAttemptsFail();

        MessageRow row = createRow();
        List<MessageRow> batch = List.of(row);

        // Kein Neustart, kein Zutun von aussen: der Aufruf kehrt
        // irgendwann normal zurueck.
        messageRepository.insertBatch(batch);

        int written = countMessages();
        assertEquals(1, written);

        int usedAttempts = attempts.get();
        assertTrue(usedAttempts > FAILURES_BEFORE_RECOVERY,
                "Es haette mehr als " + FAILURES_BEFORE_RECOVERY + " Versuche brauchen muessen");
    }

    /**
     * Laesst die ersten Schreibversuche mit genau der Ausnahme
     * scheitern, die Spring wirft, wenn die Datenbank nicht erreichbar
     * ist.
     */
    private void letFirstAttemptsFail() {
        doAnswer(invocation -> {
            int attempt = attempts.incrementAndGet();

            if (attempt <= FAILURES_BEFORE_RECOVERY) {
                throw new DataAccessResourceFailureException("Datenbank ist im Test nicht erreichbar");
            }

            return invocation.callRealMethod();
        }).when(jdbcTemplate).batchUpdate(anyString(), anyList());
    }

    /** Zaehlt die Zeilen der Tabelle. Gestubbt ist nur batchUpdate. */
    private int countMessages() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM message", Integer.class);
        return count;
    }

    /** Baut eine vollstaendige Zeile. */
    private MessageRow createRow() {
        UUID messageId = UUID.randomUUID();
        UUID roomId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new MessageRow(messageId, roomId, "anna", "Anna Muster", "Hallo", sentAt);
    }
}
