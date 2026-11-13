package ch.benedict.m321.chatservice.stream;

import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Set;

/**
 * Kleine Helfer, um ein SSE-Ereignis in Tests zu lesen.
 *
 * Steht hier und nicht in jedem Test erneut, weil zwei Testklassen dasselbe
 * brauchen: StreamPingTest prüft den Kommentar, StreamDeliveryTest die
 * Reihenfolge der gesendeten Nachrichten.
 *
 * Gelesen wird aus dem FERTIG gebauten Ereignis, nicht aus einem
 * Zwischenobjekt. Damit prüfen die Tests die Bytes, die über die Leitung
 * gegangen wären.
 */
final class StreamTestSupport {

    private static final String DATA_PREFIX = "data:";

    /** Die Zeichenkette, die der Emitter geschrieben hätte. */
    static String textOf(SseEmitter.SseEventBuilder event) {
        Set<ResponseBodyEmitter.DataWithMediaType> parts = event.build();
        StringBuilder text = new StringBuilder();
        for (ResponseBodyEmitter.DataWithMediaType part : parts) {
            Object data = part.getData();
            text.append(data);
        }
        return text.toString();
    }

    /**
     * Das JSON aus der `data:`-Zeile, oder null bei einem Ereignis ohne
     * Daten — etwa einem Ping oder der `retry:`-Zeile.
     */
    static String dataJsonOf(String eventText) {
        String[] lines = eventText.split("\n");
        for (String line : lines) {
            if (line.startsWith(DATA_PREFIX)) {
                return line.substring(DATA_PREFIX.length());
            }
        }
        return null;
    }

    private StreamTestSupport() {
    }
}
