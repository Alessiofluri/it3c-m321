package ch.benedict.m321.batchwriter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Alles, was dieser Dienst beim Start am Broker einrichtet.
 *
 * Die Queues werden hier noch einmal deklariert, obwohl der chat-service
 * sie auch anlegt. Das ist gewollt: Startet der batch-writer zuerst,
 * findet er seine Queue trotzdem vor. Existiert sie schon, passiert
 * nichts — das Anlegen ist wiederholbar, solange beide Seiten dieselben
 * Eigenschaften angeben.
 */
@Configuration
public class RabbitConfig {

    /** Stapelgroesse aus PLANUNG.md 3.6. */
    private static final int BATCH_SIZE = 500;

    /**
     * Sammelzeit aus PLANUNG.md 3.6, in Millisekunden.
     *
     * Ohne sie bliebe bei wenig Betrieb eine einzelne Nachricht liegen,
     * bis 499 weitere kommen.
     */
    private static final long BATCH_RECEIVE_TIMEOUT = 200L;

    /**
     * Der Schreibweg. Eigenschaften exakt wie im chat-service, sonst
     * lehnt RabbitMQ die zweite Deklaration ab.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /** Das Abstellgleis fuer Nachrichten, die niemand verarbeiten kann. */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Wandelt das JSON aus der Queue in unseren eigenen Record.
     *
     * Wichtig ist, was hier NICHT passiert: Es wird kein Klassenname aus
     * dem Header __TypeId__ gelesen. Der Zieltyp kommt aus der Signatur
     * der Listener-Methode. Nur deshalb kann eine Nachricht auch dann
     * verarbeitet werden, wenn sie allein den Header
     * content_type: application/json traegt.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * Die Factory fuer den Stapelbetrieb.
     *
     * consumerBatchEnabled sammelt einzelne Nachrichten zu einer Liste,
     * statt sie einzeln an den Listener zu geben. Der Stapel geht an den
     * Listener, sobald BATCH_SIZE erreicht ist ODER
     * BATCH_RECEIVE_TIMEOUT abgelaufen ist — was zuerst eintritt.
     *
     * prefetch muss mindestens so gross sein wie der Stapel. Waere es
     * kleiner, wuerde der Broker nie genug Nachrichten herausgeben,
     * damit der Stapel voll wird, und jeder Stapel liefe in die Zeit.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory batchListenerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setBatchListener(true);
        factory.setConsumerBatchEnabled(true);
        factory.setBatchSize(BATCH_SIZE);
        factory.setReceiveTimeout(BATCH_RECEIVE_TIMEOUT);
        factory.setPrefetchCount(BATCH_SIZE);

        return factory;
    }
}
