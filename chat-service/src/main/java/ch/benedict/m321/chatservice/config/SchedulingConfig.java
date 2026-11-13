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

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(SCHEDULER_POOL_SIZE);
        scheduler.setThreadNamePrefix("stream-ping-");
        return scheduler;
    }
}
