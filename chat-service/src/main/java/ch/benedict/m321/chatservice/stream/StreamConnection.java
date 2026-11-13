package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Ein offener SSE-Stream mit allem, was zu ihm gehört (Spezifikation 3.7.1).
 *
 * In Schritt 2 war das noch ein record mit zwei Feldern. Jetzt hält die
 * Verbindung veränderlichen Zustand, und damit hört ein record auf, die
 * richtige Form zu sein.
 *
 * Gleichheit ist absichtlich die Objektidentität, nicht der Feldvergleich
 * des früheren record. Das Register ist eine Menge, aus der einzelne
 * Verbindungen ausgetragen werden; zwei Clients im selben Raum mit
 * demselben Zeitstempel dürfen nicht als dieselbe Verbindung gelten.
 *
 * Die Sperre gehört zur Verbindung und nicht zum Register. Eine Sperre für
 * alle wäre einfacher, würde aber bedeuten: Ein langsamer Client hält die
 * Nachlieferung aller anderen auf.
 */
public class StreamConnection {

    /**
     * Wie weit `sentIds` zurückreicht.
     *
     * Nur Zeilen, die kurz vor der Registrierung entstanden sind, können
     * überhaupt doppelt sein: Alles in `pending` wurde NACH der
     * Registrierung veröffentlicht, und sentAt wird höchstens
     * Millisekunden vor dem Veröffentlichen gesetzt. Fünf Sekunden sind
     * reichlich Luft für diese Spanne — und sie halten das Set klein, auch
     * wenn ein Client eine Stunde weg war (Spezifikation 3.7.5).
     */
    private static final Duration DUPLICATE_WINDOW = Duration.ofSeconds(5);

    private final SseEmitter emitter;

    private final UUID roomId;

    /**
     * Wann die Verbindung ins Register kam.
     *
     * Der Bezugspunkt für das Fenster oben. Gemerkt wird der Zeitpunkt des
     * Eintragens und nicht der des Verbindungsaufbaus, weil ab dem
     * Eintragen live eintreffende Nachrichten in `pending` landen.
     */
    private final Instant registeredAt;

    /** Die Sperre, unter der Zustand und `pending` angefasst werden. */
    private final Object lock = new Object();

    /**
     * Live eingetroffen, während die Nachlieferung läuft.
     *
     * Eine Liste und keine Menge: Die Reihenfolge des Eintreffens ist
     * genau das, was erhalten bleiben muss.
     */
    private final List<ChatMessage> pending = new ArrayList<>();

    /** Beim Nachliefern gesendete IDs, zeitlich begrenzt (siehe oben). */
    private final Set<UUID> sentIds = new HashSet<>();

    private StreamState state;

    /**
     * Eine Verbindung, die sofort live ist.
     *
     * Der Fall ohne Anker: Ein Client, der sich zum ersten Mal verbindet,
     * hat nichts verpasst.
     */
    public StreamConnection(SseEmitter emitter, UUID roomId) {
        this(emitter, roomId, Instant.now(), StreamState.LIVE);
    }

    public StreamConnection(SseEmitter emitter, UUID roomId, Instant registeredAt, StreamState state) {
        this.emitter = emitter;
        this.roomId = roomId;
        this.registeredAt = registeredAt;
        this.state = state;
    }

    public SseEmitter emitter() {
        return emitter;
    }

    public UUID roomId() {
        return roomId;
    }

    public Instant registeredAt() {
        return registeredAt;
    }

    /** Die Sperre für Aufrufer, die Zustand und `pending` zusammen brauchen. */
    public Object lock() {
        return lock;
    }

    /** Nur unter der Sperre aufrufen. */
    public boolean isLive() {
        return state == StreamState.LIVE;
    }

    /** Nur unter der Sperre aufrufen. */
    public int pendingCount() {
        return pending.size();
    }

    /** Nur unter der Sperre aufrufen. */
    public void addPending(ChatMessage message) {
        pending.add(message);
    }

    /** Nur unter der Sperre aufrufen. Die lebende Liste, keine Kopie. */
    public List<ChatMessage> pending() {
        return pending;
    }

    /**
     * Merkt eine beim Nachliefern gesendete ID, falls sie überhaupt doppelt
     * sein kann.
     *
     * Alles, was älter ist als das Fenster, kann nicht in `pending` stehen —
     * es zu merken würde das Set nur wachsen lassen.
     */
    public void rememberSentId(ChatMessage message) {
        Instant sentAt = message.sentAt();
        Instant oldestRelevant = registeredAt.minus(DUPLICATE_WINDOW);
        if (sentAt.isBefore(oldestRelevant)) {
            return;
        }
        UUID messageId = message.id();
        sentIds.add(messageId);
    }

    /** Wurde diese Nachricht schon beim Nachliefern gesendet? */
    public boolean wasSentDuringCatchUp(ChatMessage message) {
        UUID messageId = message.id();
        return sentIds.contains(messageId);
    }

    /**
     * Schaltet auf Normalbetrieb und gibt den Zwischenspeicher frei.
     *
     * Nur unter der Sperre aufrufen, und erst nachdem `pending` gesendet
     * wurde. `sentIds` wird verworfen: Ab jetzt kann nichts mehr doppelt
     * kommen, denn es wird nicht mehr aus der Datenbank gelesen.
     */
    public void markLive() {
        pending.clear();
        sentIds.clear();
        state = StreamState.LIVE;
    }
}
