package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.repository.MessageHistoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Der Zustandsautomat einer Verbindung: Nachliefern, dann live
 * (Spezifikation 3.7.2, Schritte 6 bis 12).
 *
 * Live-Zustellung und Nachlieferung stehen absichtlich in EINER Klasse.
 * Beide fassen dieselbe Sperre an, und die Regel, wann unter welcher Sperre
 * was passieren darf, lässt sich nur beurteilen, wenn man sie an einer
 * Stelle liest.
 */
@Slf4j
@Service
public class StreamDelivery {

    /**
     * Wie lange gewartet wird, bevor die Nachlieferung beginnt.
     *
     * In dieser Sekunde schreibt der batch-writer mit seinem Sammelfenster
     * von 200 ms weg, was kurz vor der Registrierung veröffentlicht wurde.
     * Ohne die Verzögerung fehlten genau diese Nachrichten: in der
     * Datenbank noch nicht da, in `pending` nicht drin, weil vor der
     * Registrierung veröffentlicht.
     *
     * Die Verzögerung ist EINGEPLANT, kein Thread wartet dafür.
     */
    private static final Duration CATCH_UP_DELAY = Duration.ofSeconds(1);

    /**
     * Wie oft der Anker gesucht wird, bevor aufgegeben wird.
     *
     * Der batch-writer kann die Nachricht trotz der Sekunde oben noch nicht
     * geschrieben haben, etwa wenn er gerade einen Stapel abarbeitet.
     */
    private static final int ANCHOR_ATTEMPTS = 4;

    private static final long ANCHOR_RETRY_PAUSE_MS = 500L;

    /**
     * Wie viele Nachrichten eine Verbindung höchstens zwischenspeichert
     * (Spezifikation 3.7.7).
     *
     * Die Grenze ist nötig, weil `pending` im Hauptspeicher dieser Instanz
     * liegt. Ohne sie könnte ein einziger Raum mit viel Verkehr während
     * einer langen Nachlieferung den Speicher füllen.
     *
     * Beim Erreichen wird der Stream geschlossen und nicht etwa die
     * Nachricht verworfen: Der Browser verbindet sich ab seiner letzten
     * wirklich erhaltenen ID neu, und die Nachlieferung beginnt dort. Würde
     * stattdessen verworfen, fehlte dem Client die Nachricht für immer, ohne
     * dass es jemand merkt.
     */
    private static final int MAX_PENDING = 1000;

    /**
     * Wie viele Nachlieferungen gleichzeitig laufen oder warten dürfen.
     *
     * Vier Threads arbeiten sie ab (SchedulingConfig). 200 ist die Länge
     * der Warteschlange davor; darüber wird abgewiesen, statt eine immer
     * längere Schlange aufzubauen, in der niemand mehr rechtzeitig
     * drankommt.
     */
    private static final int MAX_CONCURRENT_CATCH_UPS = 200;

    private final StreamWriter streamWriter;

    private final MessageHistoryRepository messageHistoryRepository;

    private final TaskScheduler catchUpScheduler;

    /**
     * Wie viele Nachlieferungen gerade laufen oder in der Warteschlange
     * stehen.
     *
     * AtomicInteger und nicht int: Hochgezaehlt wird im Request-Thread,
     * heruntergezaehlt im Scheduler-Thread. Mit einem gewoehnlichen int
     * koennten zwei gleichzeitige Verbindungen denselben Platz belegen.
     */
    private final AtomicInteger runningCatchUps = new AtomicInteger();

    /**
     * Der Scheduler kommt per Qualifier herein.
     *
     * Es gibt zwei TaskScheduler-Beans: diesen und den für @Scheduled. Ohne
     * den Qualifier wüsste Spring nicht, welchen es nehmen soll — und der
     * Ping darf nicht hinter den Nachlieferungen warten.
     */
    public StreamDelivery(
            StreamWriter streamWriter,
            MessageHistoryRepository messageHistoryRepository,
            @Qualifier("catchUpScheduler") TaskScheduler catchUpScheduler) {

        this.streamWriter = streamWriter;
        this.messageHistoryRepository = messageHistoryRepository;
        this.catchUpScheduler = catchUpScheduler;
    }

    /**
     * Eine live eingetroffene Nachricht (Spezifikation 3.7.2, Schritt 12).
     *
     * Läuft im Listener-Thread und unter der Sperre der Verbindung. Ohne
     * die Sperre könnte eine Nachricht, die genau während des Umschaltens
     * eintrifft, vor den nachgelieferten beim Client landen — oder ganz
     * verloren gehen, weil sie in ein `pending` geschrieben wird, das
     * gerade geleert wurde.
     */
    public void deliverLive(StreamConnection connection, ChatMessage message) {
        boolean bufferIsFull = false;
        synchronized (connection.lock()) {
            if (connection.isLive()) {
                streamWriter.sendMessage(connection, message);
                return;
            }
            int buffered = connection.pendingCount();
            if (buffered >= MAX_PENDING) {
                bufferIsFull = true;
            } else {
                connection.addPending(message);
            }
        }
        if (bufferIsFull) {
            // Ausserhalb der Sperre, weil complete() die Callbacks des
            // Controllers auslöst und die ihrerseits ins Register greifen.
            String reason = "buffer full during catch-up (" + MAX_PENDING + " messages)";
            streamWriter.closeStream(connection, reason);
        }
    }

    /**
     * Plant die Nachlieferung ein und gibt sofort zurück.
     *
     * Der Request-Thread darf hier nicht warten: Er muss den Emitter
     * zurückgeben, damit Tomcat die Antwort überhaupt beginnt. Erst danach
     * kann geschrieben werden.
     */
    public void scheduleCatchUp(StreamConnection connection, UUID anchorId) {
        int running = runningCatchUps.incrementAndGet();
        if (running > MAX_CONCURRENT_CATCH_UPS) {
            runningCatchUps.decrementAndGet();
            String reason = "too many catch-ups running (" + MAX_CONCURRENT_CATCH_UPS + ")";
            streamWriter.closeStream(connection, reason);
            return;
        }
        Instant startAt = Instant.now().plus(CATCH_UP_DELAY);
        Runnable task = () -> runCountedCatchUp(connection, anchorId);
        try {
            catchUpScheduler.schedule(task, startAt);
        } catch (Exception notScheduled) {
            // Beim Herunterfahren nimmt der Pool keine Aufgaben mehr an.
            // Ohne diesen Zweig bliebe der Zaehler oben haengen.
            runningCatchUps.decrementAndGet();
            String reason = "catch-up could not be scheduled: " + notScheduled.getMessage();
            streamWriter.closeStream(connection, reason);
        }
    }

    /**
     * Fuehrt die Nachlieferung aus und gibt den Platz danach wieder frei.
     *
     * Der Zaehler MUSS im finally sinken. Sonst waeren nach 200
     * gescheiterten Nachlieferungen alle Plaetze dauerhaft belegt, und kein
     * Client koennte mehr nachliefern.
     */
    private void runCountedCatchUp(StreamConnection connection, UUID anchorId) {
        try {
            catchUp(connection, anchorId);
        } finally {
            runningCatchUps.decrementAndGet();
        }
    }

    /** Wie viele Nachlieferungen gerade laufen oder warten. */
    public int runningCatchUps() {
        return runningCatchUps.get();
    }

    /**
     * Liefert nach und schaltet danach auf live (Schritte 8 bis 11).
     *
     * Öffentlich und nicht privat, weil es eine für sich sinnvolle
     * Handlung ist: "liefere für diese Verbindung ab diesem Anker nach".
     * Das Einplanen darüber ist nur die Terminierung.
     *
     * Umgeschaltet wird in JEDEM Fall. Fehlt der Anker oder ist die
     * Datenbank weg, bekommt der Client eben nichts nachgeliefert — aber er
     * bekommt ab sofort alles Neue. Ein Client, der in CATCHING_UP
     * steckenbliebe, sähe nie wieder eine Nachricht.
     */
    public void catchUp(StreamConnection connection, UUID anchorId) {
        UUID roomId = connection.roomId();
        Optional<Instant> anchorSentAt = findAnchor(anchorId, roomId);
        if (anchorSentAt.isEmpty()) {
            log.warn("Anchor {} not found in room {}, going live without catch-up", anchorId, roomId);
            switchToLive(connection);
            return;
        }
        Instant foundAt = anchorSentAt.get();
        deliverPages(connection, foundAt, anchorId);
        switchToLive(connection);
    }

    /**
     * Sucht den Anker, mit Wiederholungen (Schritt 8).
     *
     * Nicht gefunden heisst zweierlei, und beides endet gleich: Entweder
     * hat der batch-writer die Nachricht noch nicht geschrieben — dann
     * hilft Warten —, oder die ID gehört zu einem anderen Raum, dann hilft
     * es nicht. Unterscheiden könnte der Dienst das nur mit einer zweiten
     * Abfrage ohne room_id, und die wäre ihren Preis nicht wert.
     *
     * Thread.sleep hält einen der vier Threads auf. Bei vielen
     * gleichzeitigen Neuverbindungen stehen die übrigen in der Warteschlange
     * — das ist die Kehrseite der vier Threads aus Spezifikation 3.7.7 und
     * im Plan so vorgesehen.
     */
    private Optional<Instant> findAnchor(UUID anchorId, UUID roomId) {
        for (int attempt = 1; attempt <= ANCHOR_ATTEMPTS; attempt = attempt + 1) {
            Optional<Instant> found = findAnchorOnce(anchorId, roomId);
            if (found.isPresent()) {
                return found;
            }
            if (attempt < ANCHOR_ATTEMPTS) {
                pauseBetweenAttempts();
            }
        }
        return Optional.empty();
    }

    /**
     * Eine einzelne Ankersuche.
     *
     * Eine nicht erreichbare Datenbank sieht hier aus wie "nicht gefunden".
     * Das ist Absicht: Der Client soll in beiden Fällen live weiterlaufen,
     * und die Lücke steht im Log (Spezifikation, offener Punkt 5).
     */
    private Optional<Instant> findAnchorOnce(UUID anchorId, UUID roomId) {
        try {
            return messageHistoryRepository.findSentAt(anchorId, roomId);
        } catch (Exception databaseNotReachable) {
            log.warn("Anchor lookup failed for room {}: {}", roomId, databaseNotReachable.getMessage());
            return Optional.empty();
        }
    }

    private void pauseBetweenAttempts() {
        try {
            Thread.sleep(ANCHOR_RETRY_PAUSE_MS);
        } catch (InterruptedException interrupted) {
            // Beim Herunterfahren bricht der Scheduler die Aufgabe ab. Das
            // Flag wird wieder gesetzt, damit der Thread-Pool es sieht.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sendet Seite für Seite, bis keine volle Seite mehr kommt (Schritte 9
     * und 10).
     *
     * Weitergeblättert wird per Keyset: Die letzte Zeile der Seite ist der
     * neue Anker. Damit liegen nie mehr als 500 Zeilen im Speicher, egal
     * wie lange der Client weg war.
     */
    private void deliverPages(StreamConnection connection, Instant anchorSentAt, UUID anchorId) {
        UUID roomId = connection.roomId();
        Instant currentSentAt = anchorSentAt;
        UUID currentId = anchorId;
        int delivered = 0;
        boolean morePages = true;
        while (morePages) {
            List<ChatMessage> page = readPage(roomId, currentSentAt, currentId);
            if (page == null) {
                log.warn("Catch-up for room {} stopped after {} messages, database not reachable", roomId, delivered);
                return;
            }
            for (ChatMessage message : page) {
                streamWriter.sendMessage(connection, message);
                connection.rememberSentId(message);
                delivered = delivered + 1;
            }
            if (page.size() < MessageHistoryRepository.PAGE_SIZE) {
                morePages = false;
            } else {
                ChatMessage lastOfPage = page.get(page.size() - 1);
                currentSentAt = lastOfPage.sentAt();
                currentId = lastOfPage.id();
            }
        }
        log.info("Catch-up for room {} delivered {} messages", roomId, delivered);
    }

    /** Eine Seite, oder null, wenn die Datenbank nicht antwortet. */
    private List<ChatMessage> readPage(UUID roomId, Instant sentAt, UUID messageId) {
        try {
            return messageHistoryRepository.findPageAfter(roomId, sentAt, messageId);
        } catch (Exception databaseNotReachable) {
            log.warn("Page lookup failed for room {}: {}", roomId, databaseNotReachable.getMessage());
            return null;
        }
    }

    /**
     * Schaltet auf Normalbetrieb um (Schritt 11).
     *
     * Gesendet wird UNTER der Sperre, und das ist kein Versehen. Würde die
     * Sperre vorher freigegeben, könnte eine live eintreffende Nachricht
     * sofort hinausgehen und damit VOR den wartenden älteren beim Client
     * landen. Der Preis: Ein langsamer Client hält während des Umschaltens
     * seinen Listener-Thread auf.
     *
     * Übersprungen wird, was beim Nachliefern schon gesendet wurde. Das ist
     * der Überschneidungsbereich: Eine Nachricht, die nach der
     * Registrierung veröffentlicht wurde und noch rechtzeitig in der
     * Datenbank landete, steht in beidem — in `pending` und auf einer Seite.
     */
    private void switchToLive(StreamConnection connection) {
        UUID roomId = connection.roomId();
        int sentFromPending = 0;
        int skipped = 0;
        synchronized (connection.lock()) {
            List<ChatMessage> pending = connection.pending();
            for (ChatMessage message : pending) {
                boolean alreadySent = connection.wasSentDuringCatchUp(message);
                if (alreadySent) {
                    skipped = skipped + 1;
                } else {
                    streamWriter.sendMessage(connection, message);
                    sentFromPending = sentFromPending + 1;
                }
            }
            connection.markLive();
        }
        log.info("Room {} switched to live, {} buffered messages sent, {} skipped as duplicates",
                roomId, sentFromPending, skipped);
    }
}
