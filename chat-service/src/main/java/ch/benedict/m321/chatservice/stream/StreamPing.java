package ch.benedict.m321.chatservice.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Schickt allen offenen Streams regelmässig ein Lebenszeichen.
 *
 * Zwei Aufgaben in einer: Der Ping hält die Verbindung durch Proxys offen,
 * und er ist die einzige Gelegenheit, bei der ein abgerissener Client in
 * einem stillen Raum auffällt. Erst beim Schreiben wirft Tomcat
 * (Spezifikation 3.7.6) — in einem Raum ohne Verkehr wird nie geschrieben,
 * also nie geworfen, also nie aufgeräumt.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamPing {

    /**
     * Alle 15 Sekunden.
     *
     * Kurz genug, dass eine tote Verbindung nicht lange im Register steht,
     * und lang genug, dass bei vielen Clients kaum Verkehr entsteht: ein
     * Kommentar sind sieben Bytes.
     */
    private static final long PING_INTERVAL_MS = 15_000L;

    private final StreamRegistry streamRegistry;

    private final StreamWriter streamWriter;

    /**
     * Eine Ping-Runde über alle Räume.
     *
     * Läuft auf einer Kopie der Verbindungsliste, weil das Schreiben in
     * einen toten Client ihn aus dem Register austrägt — mitten im
     * Durchlauf.
     *
     * fixedRate und nicht fixedDelay: Der Abstand soll vom Takt abhängen
     * und nicht davon, wie lange die letzte Runde gedauert hat. Spring
     * lässt dieselbe Methode trotzdem nie zweimal gleichzeitig laufen.
     */
    @Scheduled(fixedRate = PING_INTERVAL_MS)
    public void pingAllStreams() {
        List<StreamConnection> connections = streamRegistry.allConnections();
        if (connections.isEmpty()) {
            return;
        }
        for (StreamConnection connection : connections) {
            streamWriter.sendPing(connection);
        }
        int pinged = connections.size();
        log.debug("Ping sent to {} open streams", pinged);
    }
}
