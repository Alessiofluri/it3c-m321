package ch.benedict.m321.chatservice.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.UUID;

/**
 * Schliesst beim Herunterfahren alle offenen Streams.
 *
 * Warum das Pflicht ist: server.shutdown steht in Spring Boot 3.5.16
 * standardmässig auf "graceful", mit 30 Sekunden pro Phase. Ein offener
 * SSE-Stream ist ein LAUFENDER Request. Ohne diese Klasse würde Tomcat auf
 * Streams warten, die von sich aus nie enden — bis Docker nach zehn
 * Sekunden mit SIGKILL nachhilft. Szenario S4 verlangt aber, dass die
 * Instanz sauber geht und ihre Queue binnen 30 Sekunden verschwindet.
 *
 * ContextClosedEvent und nicht @PreDestroy: Das Ereignis feuert, BEVOR
 * Spring die Lebenszyklus-Phasen anhält. Später wäre zu spät, denn dann
 * wartet Tomcat schon.
 *
 * Für den Client ist das kein Verlust: complete() beendet den Stream
 * ordentlich, der Browser verbindet sich nach den 2000 ms aus dem
 * `retry:` neu, landet bei der verbleibenden Instanz und holt mit
 * Last-Event-ID nach, was er verpasst hat (Spezifikation 3.3).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamShutdown {

    private final StreamRegistry streamRegistry;

    /**
     * Beendet jeden Stream dieser Instanz.
     *
     * Die Methode nimmt das Ereignis nicht als Parameter — gebraucht wird
     * nichts daraus, und der Typ steht in der Annotation.
     */
    @EventListener(ContextClosedEvent.class)
    public void closeAllStreams() {
        List<StreamConnection> connections = streamRegistry.allConnections();
        int openStreams = connections.size();
        log.info("Shutting down, closing {} open streams", openStreams);
        for (StreamConnection connection : connections) {
            closeOneStream(connection);
        }
    }

    /**
     * Schliesst einen einzelnen Stream.
     *
     * Erst austragen, dann schliessen: complete() löst den Callback
     * onCompletion aus, der ebenfalls austrägt. Doppeltes Austragen ist
     * harmlos, eine Reihenfolge, die beim Durchlaufen die Liste verändert,
     * wäre es nicht — deshalb läuft der Aufrufer auf einer Kopie.
     *
     * Jeder Fehler wird gefangen. Ein Client, der in derselben Sekunde
     * abreisst, darf das Herunterfahren der anderen Streams nicht
     * abbrechen.
     */
    private void closeOneStream(StreamConnection connection) {
        UUID roomId = connection.roomId();
        streamRegistry.unregister(connection);
        SseEmitter emitter = connection.emitter();
        try {
            emitter.complete();
        } catch (Exception alreadyGone) {
            log.debug("Emitter for room {} was already closed: {}", roomId, alreadyGone.getMessage());
        }
    }
}
