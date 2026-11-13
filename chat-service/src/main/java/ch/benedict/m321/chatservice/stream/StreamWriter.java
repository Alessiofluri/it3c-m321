package ch.benedict.m321.chatservice.stream;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * Schreibt in offene SSE-Streams — die einzige Stelle im Dienst, die das
 * tut.
 *
 * Warum nur eine Stelle: Live und beim Nachliefern (Schritt 7) muss für
 * dieselbe Nachricht genau dieselbe Zeichenkette entstehen. Kriterium L10
 * vergleicht die `data:`-Zeile beider Wege. Zwei Stellen, die beide JSON
 * bauen, laufen irgendwann auseinander — und der Unterschied fiele erst
 * auf, wenn ein Client eine Nachricht doppelt anzeigt.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamWriter {

    /**
     * Wie lange der Browser nach einem Abbruch wartet, in Millisekunden.
     *
     * Vom Dienst vorgegeben und nicht dem Browser überlassen: Szenario S7
     * verlangt, dass sich der Client neu verbindet, und zwei Sekunden sind
     * die Frist, mit der die Spezifikation rechnet.
     */
    private static final long RECONNECT_DELAY_MS = 2000L;

    /**
     * Der Text des Ping-Kommentars.
     *
     * Ein SSE-Kommentar beginnt mit einem Doppelpunkt und hat kein `id:`.
     * Er verändert deshalb die Last-Event-ID des Browsers nicht — ein Ping
     * darf das Nachliefern nicht beeinflussen.
     */
    private static final String PING_COMMENT = "ping";

    private final ObjectMapper objectMapper;

    private final StreamRegistry streamRegistry;

    /**
     * Die erste Zeile jedes Streams.
     *
     * Ein Ereignis ohne `data:`, nur mit `retry:`. Der Browser merkt sich
     * den Wert für alle späteren Abbrüche dieser Verbindung.
     */
    public void sendReconnectDelay(StreamConnection connection) {
        SseEmitter.SseEventBuilder event = SseEmitter.event().reconnectTime(RECONNECT_DELAY_MS);
        sendEvent(connection, event);
    }

    /**
     * Eine Nachricht als SSE-Ereignis.
     *
     * `id:` trägt die Nachrichten-UUID, weil der Browser genau sie beim
     * Neuverbinden als `Last-Event-ID` zurückschickt — daran hängt das
     * Nachliefern.
     *
     * Kein `event:`, weil es nur eine Ereignisart gibt. Ohne `event:`
     * landet alles in `onmessage` des Browsers, und der Client braucht
     * keinen Listener je Typ.
     *
     * Das JSON geht als TEXT_PLAIN hinaus. Ohne diese Angabe würde Jackson
     * die fertige Zeichenkette ein zweites Mal serialisieren und in
     * Anführungszeichen setzen.
     */
    public void sendMessage(StreamConnection connection, ChatMessage message) {
        UUID messageId = message.id();
        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException notSerializable) {
            // Bei diesem Record praktisch unmöglich — sechs Felder aus
            // UUID, String und Instant. Hier steht es trotzdem, weil die
            // Ausnahme geprüft ist und der Listener nie werfen darf.
            log.error("Message {} could not be serialized", messageId, notSerializable);
            return;
        }
        String eventId = messageId.toString();
        SseEmitter.SseEventBuilder event = SseEmitter.event()
                .id(eventId)
                .data(json, MediaType.TEXT_PLAIN);
        sendEvent(connection, event);
    }

    /**
     * Ein Lebenszeichen an einen Stream.
     *
     * Das ist der einzige Weg, auf dem der Dienst einen abgerissenen Client
     * in einem stillen Raum bemerkt: Erst beim Schreiben wirft Tomcat
     * (Spezifikation 3.7.6). Ohne den Ping blieben die Verbindungen eines
     * Raums ohne Verkehr für immer im Register stehen.
     *
     * Nebenwirkung, auf die sich Kriterium L6 stützt: Der Client in Raum B
     * bekommt kein `data:`, aber `:ping` — und das beweist, dass seine
     * Verbindung die ganze Zeit offen war und nicht bloss stillschweigend
     * abgerissen ist.
     */
    public void sendPing(StreamConnection connection) {
        SseEmitter.SseEventBuilder event = SseEmitter.event().comment(PING_COMMENT);
        sendEvent(connection, event);
    }

    /**
     * Beendet einen Stream von sich aus, weil eine Grenze erreicht ist.
     *
     * complete() und nicht completeWithError(): Für den Browser soll das
     * wie ein normales Ende aussehen, damit er sich nach den 2000 ms aus
     * dem `retry:` wieder meldet — und zwar ab der letzten ID, die er
     * WIRKLICH bekommen hat. Genau das ist bei einer überlaufenden
     * Warteschlange die Rettung: Die Nachlieferung beginnt dort neu, wo der
     * Client stehen geblieben ist (Spezifikation 3.7.7).
     *
     * WARN und nicht DEBUG: Im Normalbetrieb darf das nicht vorkommen.
     */
    public void closeStream(StreamConnection connection, String reason) {
        UUID roomId = connection.roomId();
        log.warn("Closing stream for room {}: {}", roomId, reason);
        streamRegistry.unregister(connection);
        SseEmitter emitter = connection.emitter();
        try {
            emitter.complete();
        } catch (Exception alreadyGone) {
            log.debug("Emitter for room {} was already closed: {}", roomId, alreadyGone.getMessage());
        }
    }

    /**
     * Schreibt ein fertiges Ereignis und fängt dabei jeden Fehler.
     *
     * Gefangen wird absichtlich jede Ausnahme und nicht nur die
     * IOException. Diese Methode steht zwischen dem Broker und einem
     * fremden Client; was der Client tut, darf den Listener nicht
     * umbringen, denn eine Ausnahme dort verhinderte das ACK.
     */
    private void sendEvent(StreamConnection connection, SseEmitter.SseEventBuilder event) {
        SseEmitter emitter = connection.emitter();
        try {
            emitter.send(event);
        } catch (Exception sendFailed) {
            handleDeadStream(connection, sendFailed);
        }
    }

    /**
     * Räumt eine Verbindung auf, in die nicht mehr geschrieben werden kann.
     *
     * Ein fehlgeschlagenes Senden ist der einzige Zeitpunkt, an dem ein
     * abgerissener Client auffällt — vorher merkt der Dienst nichts
     * (Spezifikation 3.7.6). Austragen ist dabei wichtiger als Schliessen:
     * Bliebe die Verbindung im Register, versuchte jede weitere Nachricht
     * dieses Raums erneut, in sie zu schreiben.
     *
     * completeWithError löst zusätzlich die Callbacks des Controllers aus
     * und gibt den offenen Request bei Tomcat frei. Auch das steht in einem
     * try: Ist der Request schon weg, wirft es — und kein Fehler beim
     * Aufräumen darf nach oben durchkommen.
     */
    private void handleDeadStream(StreamConnection connection, Exception cause) {
        UUID roomId = connection.roomId();
        streamRegistry.unregister(connection);
        SseEmitter emitter = connection.emitter();
        try {
            emitter.completeWithError(cause);
        } catch (Exception alreadyGone) {
            log.debug("Emitter for room {} could not be closed: {}", roomId, alreadyGone.getMessage());
        }
        log.warn("Stream for room {} removed after a failed send: {}", roomId, cause.getMessage());
    }
}
