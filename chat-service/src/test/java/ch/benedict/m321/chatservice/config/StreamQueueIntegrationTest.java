package ch.benedict.m321.chatservice.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft gegen einen ECHTEN Broker, dass die Stream-Queue dieser Instanz so
 * angelegt wird, wie Spezifikation 4.1 es verlangt.
 *
 * Ein Mock wäre hier wertlos: Ob der Broker "x-overflow" akzeptiert und ob
 * eine Bindung entsteht, entscheidet der Broker, nicht unser Code.
 *
 * Gelesen wird über die Management-Schnittstelle, weil nur sie "durable",
 * "exclusive", "auto_delete" und die Argumente zurückgibt. RabbitAdmin
 * liefert bloss Name, Nachrichten- und Verbraucherzahl.
 *
 * Der Test legt nichts selbst an. In Schritt 1 musste er das noch, weil
 * x-expires die Queue nach zehn Sekunden ohne Verbraucher löscht. Seit
 * Schritt 2 hält der Listener einen Consumer offen: Die Queue entsteht beim
 * Hochfahren und bleibt, solange der Kontext läuft. Dass der Test ohne
 * Selbst-Anlegen grün ist, beweist deshalb zusätzlich, dass die ANWENDUNG
 * die Queue deklariert — nicht der Test.
 */
@SpringBootTest
@Testcontainers
class StreamQueueIntegrationTest {

    /**
     * ServiceConnection setzt spring.rabbitmq.host und -port automatisch auf
     * den gestarteten Container. Das Image braucht die Variante
     * "-management", sonst gibt es die Schnittstelle nicht, die dieser Test
     * abfragt.
     */
    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    @Qualifier("liveQueue")
    private Queue liveQueue;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private CachingConnectionFactory connectionFactory;

    @Autowired
    private ObjectMapper objectMapper;

    /** Das Präfix macht die Queues in der Abnahme zählbar (L3). */
    @Test
    void nameStartsWithStreamPrefix() {
        String queueName = liveQueue.getName();

        assertTrue(queueName.startsWith(QueueNames.STREAM_QUEUE_PREFIX));
    }

    /** Ohne diese Prüfung wüssten wir nur, dass unser Bean existiert. */
    @Test
    void existsInBroker() {
        String queueName = liveQueue.getName();
        Properties properties = rabbitAdmin.getQueueProperties(queueName);

        assertNotNull(properties);
    }

    /**
     * durable ja, exclusive und auto-delete nein. Die beiden Letzteren
     * würden die Queue mit der Verbindung löschen, und das darf eine
     * pausierte Instanz nicht umbringen (S6).
     */
    @Test
    void isDurableAndSurvivesAClosedConnection() throws Exception {
        JsonNode queue = readQueueFromBroker();
        JsonNode durable = queue.get("durable");
        JsonNode exclusive = queue.get("exclusive");
        JsonNode autoDelete = queue.get("auto_delete");

        assertTrue(durable.asBoolean());
        assertFalse(exclusive.asBoolean());
        assertFalse(autoDelete.asBoolean());
    }

    /**
     * Die drei Argumente aus Spezifikation 4.1. Zusammen mit Prefetch 50
     * ergeben 950 "ready" die Grenze von 1000 aus S6, und drop-head verwirft
     * dabei die älteste Nachricht.
     */
    @Test
    void hasTheThreeLimitArguments() throws Exception {
        JsonNode queue = readQueueFromBroker();
        JsonNode arguments = queue.get("arguments");
        JsonNode expires = arguments.get("x-expires");
        JsonNode maxLength = arguments.get("x-max-length");
        JsonNode overflow = arguments.get("x-overflow");

        assertEquals(10_000, expires.asInt());
        assertEquals(950, maxLength.asInt());
        assertEquals("drop-head", overflow.asText());
    }

    /**
     * Genau eine Bindung an den Fanout.
     *
     * Gezählt statt nur gesucht: Der Broker führt für jede Queue zusätzlich
     * eine Bindung an den Standard-Exchange ("") — die darf nicht
     * mitgezählt werden, und eine zweite Bindung an chat.delivery würde jede
     * Nachricht doppelt zustellen.
     */
    @Test
    void isBoundToDeliveryExchangeExactlyOnce() throws Exception {
        JsonNode bindings = readBindingsFromBroker();
        int bindingsToFanout = 0;
        for (JsonNode binding : bindings) {
            JsonNode source = binding.get("source");
            String sourceName = source.asText();
            if (QueueNames.DELIVERY_EXCHANGE.equals(sourceName)) {
                bindingsToFanout = bindingsToFanout + 1;
            }
        }

        assertEquals(1, bindingsToFanout);
    }

    /**
     * Nach einem Verbindungsabbruch benutzt die Instanz denselben Namen.
     *
     * Das ist der Grund, warum die UUID in der @Bean-Methode entsteht und
     * nicht bei jedem Zugriff: Nur so findet die Instanz nach einem
     * Abbruch ihre alte Queue mit dem gepufferten Inhalt wieder. Käme bei
     * jeder Neuverbindung ein neuer Name heraus, wäre das Gepufferte weg.
     */
    @Test
    void keepsItsNameAfterReconnect() {
        String nameBefore = liveQueue.getName();

        connectionFactory.resetConnection();

        Properties properties = rabbitAdmin.getQueueProperties(nameBefore);
        String nameAfter = liveQueue.getName();
        assertEquals(nameBefore, nameAfter);
        assertNotNull(properties);
    }

    /** Die Queue, wie der Broker sie sieht. */
    private JsonNode readQueueFromBroker() throws Exception {
        String path = "/api/queues/%2F/" + liveQueue.getName();
        return askBroker(path);
    }

    /** Alle Bindungen dieser Queue, wie der Broker sie sieht. */
    private JsonNode readBindingsFromBroker() throws Exception {
        String path = "/api/queues/%2F/" + liveQueue.getName() + "/bindings";
        return askBroker(path);
    }

    /**
     * Ein GET auf die Management-Schnittstelle des Test-Brokers.
     *
     * Die Adresse wird als URI übergeben und nicht als Zeichenkette, damit
     * das "%2F" für den Standard-Vhost nicht ein zweites Mal kodiert wird.
     */
    private JsonNode askBroker(String path) throws Exception {
        String user = rabbitMq.getAdminUsername();
        String password = rabbitMq.getAdminPassword();
        BasicAuthenticationInterceptor authentication = new BasicAuthenticationInterceptor(user, password);
        RestClient client = RestClient.builder()
                .requestInterceptor(authentication)
                .build();
        String url = rabbitMq.getHttpUrl() + path;
        URI uri = URI.create(url);
        String body = client.get()
                .uri(uri)
                .retrieve()
                .body(String.class);
        return objectMapper.readTree(body);
    }
}
