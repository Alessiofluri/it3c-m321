package ch.benedict.m321.chatservice.stream;

import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Prüft das Schliessen beim Herunterfahren aus Spezifikation 3.3.
 *
 * Daran hängt Szenario S4: Bleibt ein Stream offen, wartet der graceful
 * Shutdown von Spring Boot auf einen Request, der von sich aus nie endet —
 * bis Docker nach zehn Sekunden mit SIGKILL beendet.
 */
class StreamShutdownTest {

    private final StreamRegistry streamRegistry = new StreamRegistry();

    private final StreamShutdown streamShutdown = new StreamShutdown(streamRegistry);

    /**
     * Jeder Stream wird beendet und ausgetragen.
     *
     * complete() und nicht completeWithError(): Für den Client ist das
     * Herunterfahren kein Fehler. Er verbindet sich nach den 2000 ms aus
     * dem `retry:` neu und holt bei der verbleibenden Instanz nach.
     */
    @Test
    void completesAndRemovesEveryStream() throws Exception {
        SseEmitter emitterA = mock(SseEmitter.class);
        SseEmitter emitterB = mock(SseEmitter.class);
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        streamRegistry.register(new StreamConnection(emitterA, roomA));
        streamRegistry.register(new StreamConnection(emitterB, roomB));

        streamShutdown.closeAllStreams();

        verify(emitterA).complete();
        verify(emitterB).complete();
        List<StreamConnection> remaining = streamRegistry.allConnections();
        assertTrue(remaining.isEmpty());
    }

    /**
     * Ein Client, der in derselben Sekunde abreisst, darf das
     * Herunterfahren der anderen nicht abbrechen.
     */
    @Test
    void closesTheOthersEvenIfOneFails() throws Exception {
        SseEmitter brokenEmitter = mock(SseEmitter.class);
        SseEmitter healthyEmitter = mock(SseEmitter.class);
        IllegalStateException alreadyGone = new IllegalStateException("response already closed");
        doThrow(alreadyGone).when(brokenEmitter).complete();
        streamRegistry.register(new StreamConnection(brokenEmitter, UUID.randomUUID()));
        streamRegistry.register(new StreamConnection(healthyEmitter, UUID.randomUUID()));

        assertDoesNotThrow(() -> streamShutdown.closeAllStreams());

        verify(healthyEmitter).complete();
        List<StreamConnection> remaining = streamRegistry.allConnections();
        assertTrue(remaining.isEmpty());
    }

    /** Ohne offene Streams passiert nichts. */
    @Test
    void doesNothingWithoutOpenStreams() {
        assertDoesNotThrow(() -> streamShutdown.closeAllStreams());
    }
}
