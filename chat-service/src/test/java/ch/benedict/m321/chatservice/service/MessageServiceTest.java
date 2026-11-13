package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.dto.AcceptedResponse;
import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.dto.SendMessageRequest;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Prüft die Regeln des Dienstes ohne Broker: wird eine ID vergeben,
 * wird ein Zeitstempel gesetzt, geht genau eine Nachricht raus.
 */
class MessageServiceTest {

    /**
     * Ein Test-Doppel: es merkt sich, was veröffentlicht wurde, statt
     * wirklich zu senden. Bewusst von Hand geschrieben statt mit einem
     * Mock-Framework — so sieht man beim Lesen, was passiert.
     */
    private static class RecordingPublisher extends MessagePublisher {

        private ChatMessage published;
        private int publishCount;

        RecordingPublisher() {
            // Das Doppel benutzt das RabbitTemplate nie, deshalb null.
            super(null);
        }

        @Override
        public void publish(ChatMessage message) {
            this.published = message;
            this.publishCount = this.publishCount + 1;
        }
    }

    @Test
    void assignsIdAndTimestamp() {
        RecordingPublisher publisher = new RecordingPublisher();
        MessageService messageService = new MessageService(publisher);
        UUID roomId = UUID.randomUUID();
        SendMessageRequest request =
                new SendMessageRequest(roomId, "anna", "Anna Muster", "Hallo");

        AcceptedResponse response = messageService.accept(request);

        assertNotNull(response.id());
        assertNotNull(response.sentAt());
    }

    @Test
    void publishesExactlyOnceWithTheSameId() {
        RecordingPublisher publisher = new RecordingPublisher();
        MessageService messageService = new MessageService(publisher);
        UUID roomId = UUID.randomUUID();
        SendMessageRequest request =
                new SendMessageRequest(roomId, "anna", "Anna Muster", "Hallo");

        AcceptedResponse response = messageService.accept(request);

        assertEquals(1, publisher.publishCount);
        assertEquals(response.id(), publisher.published.id());
        assertEquals(roomId, publisher.published.roomId());
        assertEquals("Anna Muster", publisher.published.senderName());
        assertEquals("Hallo", publisher.published.content());
    }

    /**
     * Der Zeitstempel darf nicht genauer sein, als die Datenbank speichert.
     * Sonst sähe eine nachgelieferte Nachricht anders aus als dieselbe
     * Nachricht live. Mikrosekunden heisst: die Nanosekunden sind ein
     * Vielfaches von 1000.
     */
    @Test
    void truncatesTimestampToMicroseconds() {
        RecordingPublisher publisher = new RecordingPublisher();
        MessageService messageService = new MessageService(publisher);
        SendMessageRequest request =
                new SendMessageRequest(UUID.randomUUID(), "anna", "Anna Muster", "Hallo");

        AcceptedResponse response = messageService.accept(request);

        int nanos = response.sentAt().getNano();
        int belowMicroseconds = nanos % 1000;
        assertEquals(0, belowMicroseconds);
        assertEquals(response.sentAt(), publisher.published.sentAt());
    }

    @Test
    void assignsADifferentIdEveryTime() {
        RecordingPublisher publisher = new RecordingPublisher();
        MessageService messageService = new MessageService(publisher);
        SendMessageRequest request =
                new SendMessageRequest(UUID.randomUUID(), "anna", "Anna Muster", "Hallo");

        AcceptedResponse first = messageService.accept(request);
        AcceptedResponse second = messageService.accept(request);

        assertEquals(2, publisher.publishCount);
        assertNotEquals(first.id(), second.id());
    }
}
