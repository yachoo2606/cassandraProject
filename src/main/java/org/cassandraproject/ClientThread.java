package org.cassandraproject;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.Random;

@Slf4j
@AllArgsConstructor
public class ClientThread implements Runnable {

    private Properties properties;
    int numUsers;
    int numSectors;
    int numSeatsPerSectors;
    int numMatches;

    public ClientThread(Properties properties) {
        this.properties = properties;
        this.numUsers = Integer.parseInt(System.getenv().getOrDefault("ENV_USERS",properties.getProperty("stadium.num_users")));
        this.numSectors = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_SECTORS",properties.getProperty("stadium.num_sectors")));
        this.numSeatsPerSectors = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_SEATS_SECTOR",properties.getProperty("stadium.num_seats_per_sector")));
        this.numMatches = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_MATCHES",properties.getProperty("stadium.num_matches")));
    }
    @Override
    public void run() {
        log.info(Thread.currentThread().getName());
        try {
            log.debug("Trying to declare cassandraService in " + Thread.currentThread().getName());
            CassandraService cassandraService = new CassandraService(properties);
            cassandraService.useKeyspace();
            cassandraService.prepareStatements();

            log.debug("Declared cassandraService in " + Thread.currentThread().getName());

            Random random = new Random();
            long userId = random.nextInt(numUsers) + 1;
            long seatId = random.nextInt(numSeatsPerSectors * numSectors) + 1;
            long matchId = random.nextInt(numMatches) + 1;

            cassandraService.reserveSeat(matchId, userId, seatId);

            log.info("Thread " + Thread.currentThread().getName() + " executed completed!");

        } catch (Exception e) {
            log.error("Error occurred: {}", e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
