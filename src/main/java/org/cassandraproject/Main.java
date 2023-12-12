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

    private final static Integer numberOfClients = 10;

    public static void main(String[] args){

        Properties properties = loadProperties();

        CassandraService cassandraService = new CassandraService(properties);
        try {
            cassandraService.createKeySpace();
            cassandraService.useKeyspace();
            cassandraService.initTables();
            // Prepare statements after creating tables
            cassandraService.prepareStatements();

            // Seed data after preparing statements
            cassandraService.seedUsers(10);
            cassandraService.seedSectors(4, 15);
            cassandraService.seedMatches(10);

        } catch (BackendException e) {
            log.error("Error occur on initializing tables in database ");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        List<Thread> threadList= new ArrayList<>();
        for(int i=0;i<numberOfClients;i++){
            threadList.add(new Thread(new ClientThread(properties)));
            threadList.get(i).start();
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