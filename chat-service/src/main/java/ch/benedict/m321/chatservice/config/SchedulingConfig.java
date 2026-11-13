package ch.benedict.m321.chatservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Schaltet geplante Aufgaben ein und gibt ihnen einen eigenen Thread-Pool.
 *
 * Ohne @EnableScheduling wuerde @Scheduled stillschweigend ignoriert — der
 * Ping liefe nie, und das faellt erst auf, wenn ein stiller Raum seine
 * toten Verbindungen behaelt.
 *
 * Der Pool heisst absichtlich "taskScheduler". Gibt es mehrere
 * TaskScheduler-Beans, sucht Spring fuer @Scheduled den mit genau diesem
 * Namen. Der Pool fuer das Nachliefern in Schritt 7 bekommt deshalb einen
 * anderen Namen, und der Ping steht nicht hinter bis zu 200
 * Nachlieferungen in der Warteschlange.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {

    /**
     * Zwei Threads.
     *
     * Einer genuegt fuer den Ping, denn Spring laesst dieselbe
     * @Scheduled-Methode nie zweimal gleichzeitig laufen. Der zweite ist
     * Luft fuer spaetere geplante Aufgaben.
     *
     * Was damit NICHT geloest ist: Der Ping schreibt der Reihe nach in jede
     * Verbindung. Ein Client mit vollem TCP-Puffer haelt die ganze Runde
     * auf. Das ist offener Punkt 6 der Spezifikation und bewusst nicht
     * behoben.
     */
    private static final int SCHEDULER_POOL_SIZE = 2;

    /**
     * Wie viele Nachlieferungen gleichzeitig laufen (Spezifikation 3.7.7).
     *
     * Vier, und nicht mehr: Jede Nachlieferung liest Seiten von 500 Zeilen
     * aus der Datenbank. Der Hikari-Pool hat zehn Verbindungen, es bleibt
     * also Luft für den uebrigen Betrieb. Mehr Threads wuerden vor allem
     * die Datenbank beschaeftigen.
     */
    private static final int CATCH_UP_POOL_SIZE = 4;

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(SCHEDULER_POOL_SIZE);
        scheduler.setThreadNamePrefix("stream-ping-");
        return scheduler;
    }

    /**
     * Der Pool fuer das Nachliefern beim Neuverbinden.
     *
     * Bewusst ein ZWEITER Scheduler und nicht derselbe wie oben. Eine
     * Nachlieferung kann Sekunden dauern — sie wartet auf den Anker und
     * liest Seiten. Teilten sich Ping und Nachlieferung einen Pool, stuende
     * der Ping hinter bis zu 200 wartenden Nachlieferungen, und abgerissene
     * Clients in stillen Raeumen fielen minutenlang nicht auf.
     *
     * Der Name ist nicht "taskScheduler", damit @Scheduled weiterhin den
     * anderen nimmt.
     */
    @Bean
    public ThreadPoolTaskScheduler catchUpScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(CATCH_UP_POOL_SIZE);
        scheduler.setThreadNamePrefix("stream-catchup-");
        return scheduler;
    }
}
