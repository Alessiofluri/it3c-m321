package ch.benedict.m321.chatservice.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Prüft den Ping aus Spezifikation 3.7.6.
 *
 * Zwei Dinge hängen daran: Kriterium L6 sucht `:ping` in der Ausgabe des
 * Clients in Raum B, und ohne den Ping bleibt eine abgerissene Verbindung
 * in einem stillen Raum für immer im Register.
 */
class StreamPingTest {

    private final StreamRegistry streamRegistry = new StreamRegistry();

    private final StreamWriter streamWriter = new StreamWriter(new ObjectMapper(), streamRegistry);

    private final StreamPing streamPing = new StreamPing(streamRegistry, streamWriter);

    /**
     * Der Ping geht als SSE-Kommentar hinaus, also als `:ping`.
     *
     * Geprüft wird die fertige Zeichenkette und nicht bloss, DASS gesendet
     * wurde: Ein Kommentar darf kein `id:` tragen, sonst überschriebe der
     * Ping die Last-Event-ID des Browsers und das Nachliefern setzte an der
     * falschen Stelle an.
     */
    @Test
    void sendsAnSseCommentToAnOpenStream() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter emitter = mock(SseEmitter.class);
        StreamConnection connection = new StreamConnection(emitter, roomId);
        streamRegistry.register(connection);

        streamPing.pingAllStreams();

        ArgumentCaptor<SseEmitter.SseEventBuilder> captor = ArgumentCaptor.forClass(SseEmitter.SseEventBuilder.class);
        verify(emitter).send(captor.capture());
        SseEmitter.SseEventBuilder sentEvent = captor.getValue();
        String sentText = textOf(sentEvent);
        assertTrue(sentText.startsWith(":ping"), "Kein SSE-Kommentar, sondern: " + sentText);
        assertTrue(!sentText.contains("id:"), "Ein Ping darf kein id: tragen: " + sentText);
    }

    /** Jeder Raum wird bedient, nicht nur der erste. */
    @Test
    void reachesStreamsInEveryRoom() throws Exception {
        SseEmitter emitterA = mock(SseEmitter.class);
        SseEmitter emitterB = mock(SseEmitter.class);
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        streamRegistry.register(new StreamConnection(emitterA, roomA));
        streamRegistry.register(new StreamConnection(emitterB, roomB));

        streamPing.pingAllStreams();

        verify(emitterA).send(any(SseEmitter.SseEventBuilder.class));
        verify(emitterB).send(any(SseEmitter.SseEventBuilder.class));
    }

    /**
     * Genau der Zweck des Pings: Die tote Verbindung verschwindet.
     *
     * In einem stillen Raum passiert sonst nie etwas, was den Abriss
     * aufdecken würde.
     */
    @Test
    void removesAStreamThatCannotBeWrittenToAnyMore() throws Exception {
        UUID roomId = UUID.randomUUID();
        SseEmitter deadEmitter = mock(SseEmitter.class);
        IOException clientGone = new IOException("client aborted");
        doThrow(clientGone).when(deadEmitter).send(any(SseEmitter.SseEventBuilder.class));
        StreamConnection connection = new StreamConnection(deadEmitter, roomId);
        streamRegistry.register(connection);

        assertDoesNotThrow(() -> streamPing.pingAllStreams());

        Set<StreamConnection> remaining = streamRegistry.connectionsForRoom(roomId);
        assertTrue(remaining.isEmpty());
    }

    /** Ohne offene Streams passiert nichts, insbesondere kein Fehler. */
    @Test
    void doesNothingWithoutOpenStreams() {
        assertDoesNotThrow(() -> streamPing.pingAllStreams());
    }

    /**
     * Baut das Ereignis zu der Zeichenkette zusammen, die über die Leitung
     * ginge.
     */
    private String textOf(SseEmitter.SseEventBuilder event) {
        Set<ResponseBodyEmitter.DataWithMediaType> parts = event.build();
        StringBuilder text = new StringBuilder();
        for (ResponseBodyEmitter.DataWithMediaType part : parts) {
            Object data = part.getData();
            text.append(data);
        }
        return text.toString();
    }
}
