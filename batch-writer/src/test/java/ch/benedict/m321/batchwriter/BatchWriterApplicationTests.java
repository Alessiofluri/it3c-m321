package ch.benedict.m321.batchwriter;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Der erste Test des Moduls: startet der Spring-Kontext ueberhaupt?
 *
 * Er wirkt klein, faengt aber jeden Tippfehler in application.yml und
 * jede fehlende Abhaengigkeit sofort ab — und zwar bevor irgendein
 * anderer Test scheitern kann.
 */
@SpringBootTest
@Testcontainers
class BatchWriterApplicationTests {

    /**
     * Eine echte Datenbank im Container.
     *
     * @ServiceConnection setzt URL, Benutzer und Passwort selbst, damit
     * im Test keine Zugangsdaten von Hand gepflegt werden muessen.
     */
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Test
    void contextLoads() {
        // Kein Inhalt noetig: schlaegt der Start fehl, faellt schon die
        // Vorbereitung dieses Tests durch.
    }
}
