package ch.benedict.m321.chatservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Prüft, dass der Spring-Kontext überhaupt hochfährt.
 *
 * Weder RabbitMQ noch Postgres werden dafür gebraucht, und das ist keine
 * Bequemlichkeit, sondern die Zusage aus Spezifikation 3.4: Der
 * chat-service muss starten, wenn beide unten sind.
 *
 * Für den Broker gilt das von Anfang an — die Verbindung entsteht erst beim
 * ersten Senden oder mit dem ersten Consumer. Für die Datenbank sorgt
 * `spring.datasource.hikari.initialization-fail-timeout: -1` dafür: Ohne
 * diesen Wert würde HikariCP beim Aufbau des Pools eine Verbindung
 * verlangen und der Kontext scheiterte.
 *
 * Dieser Test ist damit der Nachweis für Szenario S5 auf der Testseite;
 * L8 misst dasselbe am laufenden Stack.
 */
@SpringBootTest
class ChatServiceApplicationTest {

    @Test
    void contextLoadsWithoutBrokerAndWithoutDatabase() {
        // Kein Assert nötig. Fährt der Kontext nicht hoch, wirft Spring
        // eine Exception und der Test wird rot.
    }
}
