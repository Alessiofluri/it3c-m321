package ch.benedict.m321.chatservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Legt beim Start alles an, was der Broker braucht.
 *
 * Spring meldet diese Beans beim Verbindungsaufbau an RabbitMQ. Existiert
 * eine Queue schon, passiert nichts — das Anlegen ist wiederholbar.
 */
@Slf4j
@Configuration
public class RabbitConfig {

    /**
     * Wie lange die Stream-Queue ohne Verbraucher weiterlebt, in
     * Millisekunden.
     *
     * Nach oben begrenzt das Szenario S4: Faehrt eine Instanz herunter, muss
     * ihre Queue binnen 30 Sekunden weg sein, und der Broker laesst sich beim
     * Loeschen etwas Zeit. Nach unten muss der Wert ueber dem
     * recoveryInterval von Spring (5 s) liegen, damit ein kurzer
     * Netzaussetzer die Queue nicht kostet.
     *
     * Ab Schritt 9 kommt der Wert aus der Umgebung.
     */
    private static final int STREAM_QUEUE_EXPIRES_MS = 10_000;

    /**
     * Wie viele "ready" Nachrichten die Stream-Queue hoechstens haelt.
     *
     * x-max-length zaehlt nur "ready", nicht die bereits beim Verbraucher
     * liegenden unbestaetigten. Mit Prefetch 50 (Schritt 2) sind es zusammen
     * hoechstens 1000 — die Grenze aus Szenario S6.
     *
     * Ab Schritt 9 kommt der Wert aus der Umgebung.
     */
    private static final int STREAM_QUEUE_MAX_LENGTH = 950;

    /**
     * Der Schreibweg. "durable" heisst: die Queue überlebt einen Neustart
     * des Brokers. Was der Verbraucher endgültig ablehnt, landet über den
     * Standard-Exchange ("") in der Dead-Letter-Queue.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /** Das Abstellgleis für Nachrichten, die niemand verarbeiten konnte. */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Der Zustellweg. Fanout heisst: JEDE gebundene Queue bekommt eine
     * Kopie. Das ist genau richtig, weil jede web-gateway-Instanz jede
     * Nachricht sehen muss — nur sie weiss, welche ihrer WebSocket-Clients
     * im betroffenen Raum sitzen.
     */
    @Bean
    public FanoutExchange deliveryExchange() {
        return new FanoutExchange(QueueNames.DELIVERY_EXCHANGE, true, false);
    }

    /**
     * Die eigene Queue dieser Instanz am Zustellweg.
     *
     * Der Name entsteht GENAU EINMAL pro Prozess, naemlich hier. RabbitAdmin
     * meldet bei jeder Neuverbindung dasselbe Bean-Objekt erneut an; nur
     * deshalb findet die Instanz nach einem Verbindungsabbruch ihre alte
     * Queue mit dem gepufferten Inhalt wieder. Ein Hostname als Name waere
     * falsch: Ein neu gestarteter Container uebernaehme sonst die Queue
     * seines Vorgaengers samt veralteter Nachrichten, und die kaemen beim
     * Client nach dem Nachliefern als Duplikate an.
     *
     * Weder exclusive noch auto-delete, also auch keine AnonymousQueue (die
     * ist immer beides): Beides wuerde die Queue loeschen, sobald der
     * Heartbeat die Verbindung schliesst — genau das muss eine pausierte
     * Instanz aber ueberleben (Szenario S6). Aufgeraeumt wird stattdessen
     * ueber x-expires.
     *
     * Keine Dead-Letter-Queue: Ein Ueberlauf ist hier kein Defekt, und das
     * Original liegt ohnehin auf dem Schreibweg in chat.persist.
     *
     * "durable" nicht, weil der Inhalt wertvoll waere, sondern weil
     * RabbitMQ 3.13 nicht-exklusive, nicht-durable Classic Queues als
     * veraltet fuehrt und in 4.0 entfernt. Nachtraeglich aendern laesst sich
     * das nicht — eine abweichende Deklaration scheitert mit
     * PRECONDITION_FAILED.
     */
    @Bean
    public Queue liveQueue() {
        UUID instanceId = UUID.randomUUID();
        String queueName = QueueNames.STREAM_QUEUE_PREFIX + instanceId;
        Queue queue = QueueBuilder.durable(queueName)
                .expires(STREAM_QUEUE_EXPIRES_MS)
                .maxLength(STREAM_QUEUE_MAX_LENGTH)
                .overflow(QueueBuilder.Overflow.dropHead)
                .build();
        // Die Meldung ordnet in der Abnahme jeder Instanz ihre Queue zu.
        // Angelegt wird sie im Broker erst mit der ersten Verbindung; hier
        // steht der Name fest, und genau den braucht man zum Zuordnen.
        log.info("Live queue {} declared", queueName);
        return queue;
    }

    /**
     * Haengt die Queue dieser Instanz an den Fanout.
     *
     * Ohne Bindung bekaeme sie nie eine Kopie. Ein Routing-Key fehlt
     * absichtlich: Ein Fanout ignoriert ihn, der Publisher setzt ohnehin ""
     * und die roomId steht nur im Koerper der Nachricht. Gefiltert wird
     * deshalb erst im Dienst, nach dem Konsumieren.
     *
     * Die Queue kommt per Qualifier herein, weil es drei Queue-Beans gibt
     * und der Typ allein nicht entscheidet.
     */
    @Bean
    public Binding liveQueueBinding(@Qualifier("liveQueue") Queue liveQueue,
                                    FanoutExchange deliveryExchange) {
        return BindingBuilder.bind(liveQueue).to(deliveryExchange);
    }

    /**
     * Nachrichten gehen als JSON über die Leitung, nicht als serialisiertes
     * Java-Objekt. Nur so kann später ein Dienst in einer anderen Sprache
     * mitlesen.
     *
     * Der ObjectMapper kommt von Spring Boot und kann bereits Instant im
     * ISO-8601-Format schreiben.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
