package ch.benedict.m321.batchwriter.config;

import ch.benedict.m321.batchwriter.dto.ChatMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.core.ParameterizedTypeReference;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Der Test, an dem Szenario S5 haengt.
 *
 * Eine Nachricht, die NUR content_type: application/json traegt und
 * keinen Header __TypeId__, muss sich trotzdem in unseren eigenen Record
 * verwandeln lassen. Waere das nicht so, koennte der batch-writer nur
 * Nachrichten lesen, die eine Klasse des chat-service nennen — und genau
 * diese Kopplung wollen wir nicht.
 *
 * Bewusst ohne Broker und ohne Datenbank: hier wird nur die Umwandlung
 * geprueft. Ueber die echte Queue prueft das der Duplikat-Test.
 */
class ChatMessageConversionTest {

    @Test
    void convertsJsonWithoutTypeHeader() {
        UUID messageId = UUID.fromString("3f120a73-1922-4577-ae9a-d28063636992");
        UUID roomId = UUID.fromString("39acf48b-5d49-40f0-a5d6-cd5bfdb59385");
        String json = """
                {
                  "id": "3f120a73-1922-4577-ae9a-d28063636992",
                  "roomId": "39acf48b-5d49-40f0-a5d6-cd5bfdb59385",
                  "senderId": "anna",
                  "senderName": "Anna Muster",
                  "content": "Hallo",
                  "sentAt": "2026-09-25T13:48:35.746Z"
                }
                """;

        Message message = createJsonMessage(json);
        Jackson2JsonMessageConverter converter = createConverter();

        // Der Zieltyp kommt von aussen — genauso, wie ihn spaeter die
        // Signatur der Listener-Methode liefert.
        ParameterizedTypeReference<ChatMessage> targetType = new ParameterizedTypeReference<>() {
        };
        Object converted = converter.fromMessage(message, targetType);

        ChatMessage chatMessage = (ChatMessage) converted;
        assertEquals(messageId, chatMessage.id());
        assertEquals(roomId, chatMessage.roomId());
        assertEquals("anna", chatMessage.senderId());
        assertEquals("Anna Muster", chatMessage.senderName());
        assertEquals("Hallo", chatMessage.content());
        assertEquals(Instant.parse("2026-09-25T13:48:35.746Z"), chatMessage.sentAt());
    }

    /**
     * Baut eine Nachricht mit genau dem einen Header, den Szenario S5
     * vorgibt. Kein __TypeId__.
     */
    private Message createJsonMessage(String json) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);

        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        return new Message(body, properties);
    }

    /**
     * Derselbe Konverter wie im Betrieb. Das JavaTimeModule ist noetig,
     * damit aus dem ISO-8601-Text ein Instant wird; im laufenden Dienst
     * bringt Spring Boot es von selbst mit.
     */
    private Jackson2JsonMessageConverter createConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
