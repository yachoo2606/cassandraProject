package org.cassandraproject;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.util.Properties;
import java.util.Random;

@Slf4j
@AllArgsConstructor
public class ClientThread implements Runnable {

    private Properties properties;

    @Override
    public void run() {
        log.info(Thread.currentThread().getName());

        int numUsers = Integer.parseInt(properties.getProperty("stadium.num_users"));
        int numSectors = Integer.parseInt(properties.getProperty("stadium.num_sectors"));
        int numSeatsPerSectors = Integer.parseInt(properties.getProperty("stadium.num_seats_per_sector"));
        int numMatches = Integer.parseInt(properties.getProperty("stadium.num_matches"));

        try {
            log.info("Trying to declare cassandraService in " + Thread.currentThread().getName());
            CassandraService cassandraService = new CassandraService(properties);
            cassandraService.useKeyspace();
            cassandraService.prepareStatements();

            log.info("Declared cassandraService in " + Thread.currentThread().getName());

            Random random = new Random();
            long userId = random.nextInt(numUsers) + 1;
            long seatId = random.nextInt(numSeatsPerSectors) + 1;
            long matchId = random.nextInt(numMatches) + 1;

            cassandraService.reserveSeat(matchId, userId, seatId);

            log.info("Thread " + Thread.currentThread().getName() + " executed completed!");

        } catch (Exception e) {
            log.error("Error occurred");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
