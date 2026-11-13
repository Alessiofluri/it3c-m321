package ch.benedict.m321.chatservice.stream;

/**
 * In welcher Phase eine Verbindung steckt (Spezifikation 3.7.1).
 *
 * Nur zwei Zustände, und der Übergang geht nur in eine Richtung. Eine
 * Verbindung, die einmal LIVE ist, wird nie wieder CATCHING_UP — ein
 * Client, der erneut nachliefern muss, baut eine neue Verbindung auf.
 */
public enum StreamState {

    /**
     * Es läuft eine Nachlieferung aus der Datenbank. Live eintreffende
     * Nachrichten warten in `pending`, damit sie nicht vor den älteren beim
     * Client landen.
     */
    CATCHING_UP,

    /** Normalbetrieb: Was eintrifft, geht sofort hinaus. */
    LIVE
}
