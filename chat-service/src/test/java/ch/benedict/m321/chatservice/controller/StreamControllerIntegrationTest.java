package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.stream.StreamConnection;
import ch.benedict.m321.chatservice.stream.StreamRegistry;
import ch.benedict.m321.chatservice.stream.StreamWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Prüft die Antwort von GET /stream gegen Spezifikation 2.3.
 *
 * Geprüft werden die echten Bytes, nicht die Absicht: Die Zeilen heissen
 * `retry:2000`, `id:<UUID>` und `data:<JSON>` — ohne Leerzeichen nach dem
 * Doppelpunkt, so wie Spring sie schreibt. Genau daran hängen die Kriterien
 * L4 und L10, und genau das war in der ersten Fassung der Spezifikation
 * falsch beschrieben.
 *
 * MockMvc statt eines echten Ports, weil ein SSE-Stream nie endet: Ein
 * HTTP-Client müsste mit einem Zeitlimit abgebrochen werden, und der Test
 * hinge an dessen Länge. MockMvc gibt heraus, was bis zur Rückkehr des
 * Controllers geschrieben wurde.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class StreamControllerIntegrationTest {

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private StreamRegistry streamRegistry;

    @Autowired
    private StreamWriter streamWriter;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * L4: Content-Type, Cache-Control und die erste Körperzeile.
     *
     * `startsWith` und nicht `contains`: Die Spezifikation sagt "einmal am
     * Anfang". Käme `retry:` später, hätte der Browser bei einem frühen
     * Abbruch noch seinen eigenen Vorgabewert benutzt.
     */
    @Test
    void answersWithEventStreamAndTheRetryLineFirst() throws Exception {
        UUID roomId = UUID.randomUUID();

        MvcResult result = mockMvc.perform(get("/stream").param("roomId", roomId.toString()))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-cache"))
                .andReturn();

        String contentType = result.getResponse().getContentType();
        assertNotNull(contentType);
        assertTrue(contentType.startsWith("text/event-stream"));
        String body = result.getResponse().getContentAsString();
        assertTrue(body.startsWith("retry:2000"), "Körper beginnt nicht mit retry:2000, sondern: " + body);
    }

    /**
     * Keine Content-Length, also chunked.
     *
     * Stünde eine Länge in der Antwort, wäre der Stream nach genau so
     * vielen Bytes zu Ende — und der Dienst weiss nicht, wie viele
     * Nachrichten noch kommen.
     */
    @Test
    void sendsNoContentLength() throws Exception {
        UUID roomId = UUID.randomUUID();

        MvcResult result = mockMvc.perform(get("/stream").param("roomId", roomId.toString()))
                .andReturn();

        boolean hasContentLength = result.getResponse().containsHeader("Content-Length");
        assertFalse(hasContentLength);
    }

    /**
     * Das Format einer einzelnen Nachricht, Zeile für Zeile.
     *
     * Die `data:`-Zeile wird zeichenweise mit dem ObjectMapper verglichen,
     * nicht nur auf einzelne Felder geprüft. Das ist die Zusage aus 2.3,
     * auf die sich L10 stützt: live und beim Nachliefern entsteht dieselbe
     * Zeichenkette.
     */
    @Test
    void writesIdAndSingleLineDataForOneMessage() throws Exception {
        UUID roomId = UUID.randomUUID();
        MvcResult result = mockMvc.perform(get("/stream").param("roomId", roomId.toString()))
                .andReturn();
        StreamConnection connection = onlyConnectionOf(roomId);
        ChatMessage message = messageForRoom(roomId);

        streamWriter.sendMessage(connection, message);

        String body = result.getResponse().getContentAsString();
        String expectedJson = objectMapper.writeValueAsString(message);
        String expectedId = "id:" + message.id();
        String expectedData = "data:" + expectedJson;
        assertTrue(body.contains(expectedId), "Keine id-Zeile in: " + body);
        assertTrue(body.contains(expectedData), "data-Zeile stimmt nicht in: " + body);
        assertFalse(body.contains("event:"), "Es darf kein event: geben: " + body);
        assertFalse(expectedJson.contains("\n"), "Das JSON muss einzeilig sein");
    }

    /** L5, erster Teil: ohne roomId gibt es keinen Stream. */
    @Test
    void rejectsAMissingRoomId() throws Exception {
        mockMvc.perform(get("/stream"))
                .andExpect(status().isBadRequest());
    }

    /** L5, zweiter Teil: etwas, das keine UUID ist, ebenfalls nicht. */
    @Test
    void rejectsARoomIdThatIsNotAUuid() throws Exception {
        mockMvc.perform(get("/stream").param("roomId", "abc"))
                .andExpect(status().isBadRequest());
    }

    /**
     * Eine unbekannte roomId ergibt einen stillen Stream, keine 404.
     *
     * Es gibt keine Raumliste, gegen die geprüft werden könnte. Räume
     * anzulegen ist nicht Teil dieses Auftrags, und eine 404 würde dem
     * Client eine Auskunft geben, die der Dienst nicht hat.
     */
    @Test
    void acceptsAnUnknownRoomIdWithASilentStream() throws Exception {
        UUID neverUsedRoom = UUID.randomUUID();

        MvcResult result = mockMvc.perform(get("/stream").param("roomId", neverUsedRoom.toString()))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertFalse(body.contains("data:"), "Stiller Stream, aber es kam: " + body);
    }

    /**
     * Eine kaputte Last-Event-ID wird wie keine behandelt.
     *
     * Kein 400: Die Kopfzeile kommt vom Browser, nicht vom Programmierer.
     * Mit einem Fehler käme der Client nie wieder hoch, denn er würde
     * dieselbe ID bei jedem Versuch erneut schicken.
     */
    @Test
    void treatsAnInvalidLastEventIdLikeNone() throws Exception {
        UUID roomId = UUID.randomUUID();

        MvcResult result = mockMvc.perform(get("/stream")
                        .param("roomId", roomId.toString())
                        .header("Last-Event-ID", "nicht-einmal-eine-uuid"))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertTrue(body.startsWith("retry:2000"));
    }

    /** Die eine Verbindung, die der Test gerade eröffnet hat. */
    private StreamConnection onlyConnectionOf(UUID roomId) {
        Set<StreamConnection> connections = streamRegistry.connectionsForRoom(roomId);
        assertEquals(1, connections.size());
        StreamConnection found = null;
        for (StreamConnection connection : connections) {
            found = connection;
        }
        return found;
    }

    /** Eine Nachricht, wie der chat-service sie veröffentlicht. */
    private ChatMessage messageForRoom(UUID roomId) {
        UUID messageId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "s1", "Test", "Hallo", sentAt);
    }
}
