package ch.benedict.m321.batchwriter.config;

/**
 * Die Namen aus dem Queue-Vertrag an genau EINER Stelle.
 *
 * Ebenfalls eine eigene Kopie, aus demselben Grund wie bei ChatMessage:
 * kein gemeinsames Modul zwischen den Diensten. Die Namen sind Teil des
 * Vertrags und stehen so im chat-service und in PLANUNG.md 3.5.
 */
public final class QueueNames {

    /** Der Schreibweg. Hier holt dieser Dienst seine Arbeit ab. */
    public static final String PERSIST_QUEUE = "chat.persist";

    /** Das Abstellgleis fuer Nachrichten, die sich nicht lesen lassen. */
    public static final String DEAD_LETTER_QUEUE = "chat.dlq";

    /** Diese Klasse ist eine reine Namenssammlung und wird nie erzeugt. */
    private QueueNames() {
    }
}
