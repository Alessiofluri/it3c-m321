package ch.benedict.m321.chatservice.config;

/**
 * Die Namen der Queues und des Exchange an genau EINER Stelle.
 *
 * Sie werden an drei Orten gebraucht — beim Anlegen, beim Senden und im
 * Test. Ein Tippfehler in einem String wäre sonst erst zur Laufzeit
 * sichtbar, und zwar als "die Nachricht kommt nie an".
 */
public final class QueueNames {

    /** Schreibweg: hier holt der batch-writer die Nachrichten ab. */
    public static final String PERSIST_QUEUE = "chat.persist";

    /** Zustellweg: fanout an alle chat-service-Instanzen. */
    public static final String DELIVERY_EXCHANGE = "chat.delivery";

    /**
     * Zustellweg, Verbraucherseite: Praefix der Queue, die sich jede
     * chat-service-Instanz fuer ihre eigenen SSE-Streams anlegt.
     *
     * Hier steht nur das Praefix, weil der vollstaendige Name erst zur
     * Laufzeit entsteht — jede Instanz haengt eine eigene UUID an. Das
     * Praefix macht die Queues zaehlbar: "list_queues | grep chat.stream"
     * findet alle, egal wie viele Instanzen laufen.
     */
    public static final String STREAM_QUEUE_PREFIX = "chat.stream.";

    /** Dead Letter: was der batch-writer endgültig nicht verarbeiten kann. */
    public static final String DEAD_LETTER_QUEUE = "chat.dlq";

    /** Diese Klasse ist eine reine Namenssammlung und wird nie erzeugt. */
    private QueueNames() {
    }
}
