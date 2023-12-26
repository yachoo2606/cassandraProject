package org.cassandraproject;

import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = loadProperties();

        CassandraService cassandraService = new CassandraService(properties);

        int numberOfClients = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_NUMBER_OF_CLIENTS", properties.getProperty("clientsNumber")));

        int numUsers = Integer.parseInt(System.getenv().getOrDefault("ENV_USERS",properties.getProperty("stadium.num_users")));
        int numSectors = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_SECTORS",properties.getProperty("stadium.num_sectors")));
        int numSeatsPerSectors = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_SEATS_SECTOR",properties.getProperty("stadium.num_seats_per_sector")));
        int numMatches = Integer.parseInt(System.getenv().getOrDefault("ENV_NUM_MATCHES",properties.getProperty("stadium.num_matches")));


        try {
            cassandraService.createKeySpace();
            cassandraService.useKeyspace();
            cassandraService.initTables();
            // Prepare statements after creating tables
            cassandraService.prepareStatements();

            // Seed data after preparing statements
            cassandraService.seedUsers(numUsers);
            cassandraService.seedSectors(numSectors, numSeatsPerSectors);
            cassandraService.seedMatches(numMatches);

        } catch (BackendException e) {
            log.error("Error occurred while initializing tables in the database");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        List<Thread> threadList= new ArrayList<>();
        for(int i = 0; i< numberOfClients; i++){
            threadList.add(new Thread(new ClientThread(properties)));
            threadList.get(i).start();
            Thread.currentThread().sleep(1000);
        }
        for (int i = 0; i < numberOfClients; i++) {
            try {
                threadList.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("Program executed successfully");
        System.exit(0);
    }

    public static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("application.properties file not found");
            }
            properties.load(input);
            log.info("Properties loaded in "+Thread.currentThread().getName());
            return properties;
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

}