package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.stream.StreamConnection;
import ch.benedict.m321.chatservice.stream.StreamRegistry;
import ch.benedict.m321.chatservice.stream.StreamWriter;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * Der Zustellweg nach aussen: ein offener SSE-Stream je Client.
 *
 * Erreichbar nur aus dem Docker-Netz. Kein Token wird geprüft — das hat
 * später das Gateway getan, und Keycloak ist nicht Teil dieses Auftrags
 * (Spezifikation 1).
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class StreamController {

    /**
     * Zeitlimit des Emitters: keines.
     *
     * Nach der Servlet-Spezifikation bedeutet 0 "kein Timeout". Ohne diesen
     * Wert würde Tomcat den Stream nach 30 Sekunden schliessen, der Browser
     * verbände sich neu, und jede Neuverbindung löste eine Nachlieferung
     * aus — zweimal pro Minute, für jeden Client, ohne Grund.
     */
    private static final long NO_TIMEOUT = 0L;

    private final StreamRegistry streamRegistry;

    private final StreamWriter streamWriter;

    /**
     * Öffnet einen Stream für einen Raum.
     *
     * Fehlt `roomId` oder ist sie keine UUID, antwortet Spring mit 400 —
     * der Parameter ist als UUID deklariert, und mehr braucht es dafür
     * nicht. Eine UNBEKANNTE roomId ergibt dagegen 200 mit einem stillen
     * Stream: Es gibt keine Raumliste, gegen die geprüft werden könnte, und
     * Räume anzulegen ist nicht Teil dieses Auftrags.
     *
     * Reihenfolge ist wichtig: Die Verbindung wird eingetragen, BEVOR etwas
     * gesendet wird. Ab Schritt 7 muss sie sogar vor jeder Datenbankabfrage
     * stehen, damit zwischen Registrierung und Nachlieferung keine
     * Nachricht verloren geht.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam UUID roomId,
            @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId,
            HttpServletResponse response) {

        // Ein Proxy darf einen Stream nicht zwischenspeichern. Die übrigen
        // Kopfzeilen setzt Spring: Content-Type aus "produces", und
        // Transfer-Encoding chunked ergibt sich daraus, dass die Länge
        // unbekannt ist.
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");

        UUID anchorId = readAnchorId(lastEventId);
        SseEmitter emitter = new SseEmitter(NO_TIMEOUT);
        StreamConnection connection = new StreamConnection(emitter, roomId);
        registerCallbacks(connection);
        streamRegistry.register(connection);
        streamWriter.sendReconnectDelay(connection);

        if (anchorId == null) {
            log.info("Stream opened for room {} without an anchor", roomId);
        } else {
            // Der Anker wird gelesen und geprüft, aber noch nicht benutzt:
            // Das Nachliefern baut Schritt 7.
            log.info("Stream opened for room {} with anchor {}", roomId, anchorId);
        }
        return emitter;
    }

    /**
     * Liest die Kopfzeile `Last-Event-ID`.
     *
     * Fehlt sie, ist sie leer oder keine UUID, gilt der Anker als "keiner".
     * Leer ist ausdrücklich dabei, weil ein Prüfskript, das die ID falsch
     * aus dem Stream schneidet, genau das schickt — und dann soll der
     * Stream normal starten statt mit einem Fehler.
     *
     * Eine ungültige Kopfzeile ist kein Grund für 400: Sie kommt vom
     * Browser, nicht vom Programmierer, und der Client käme nach einem
     * Fehler nie wieder hoch. WARN, weil es im Normalbetrieb nicht
     * vorkommen darf.
     */
    private UUID readAnchorId(String lastEventId) {
        if (lastEventId == null) {
            return null;
        }
        if (lastEventId.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(lastEventId);
        } catch (IllegalArgumentException notAUuid) {
            log.warn("Last-Event-ID '{}' is not a UUID, treated as absent", lastEventId);
            return null;
        }
    }

    /**
     * Meldet an, was beim Ende der Verbindung passieren soll.
     *
     * Alle drei, weil jeder von ihnen der einzige sein kann, der feuert:
     * onCompletion beim sauberen Schliessen, onTimeout theoretisch nie
     * (siehe NO_TIMEOUT) und onError beim Abbruch durch den Client. Fehlt
     * einer, bleibt die Verbindung im Register stehen, und der Ping aus
     * Schritt 4 schreibt alle 15 Sekunden in ein totes Objekt.
     */
    private void registerCallbacks(StreamConnection connection) {
        SseEmitter emitter = connection.emitter();
        emitter.onCompletion(() -> streamRegistry.unregister(connection));
        emitter.onTimeout(() -> streamRegistry.unregister(connection));
        emitter.onError(error -> streamRegistry.unregister(connection));
    }
}
