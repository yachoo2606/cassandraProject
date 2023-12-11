package org.cassandraproject;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@AllArgsConstructor
public class ClientThread implements Runnable{

    String type;

    @Override
    public void run() {

        log.info(Thread.currentThread().getName());
        Properties properties = loadProperties();
        try {
            CassandraService cassandraService = new CassandraService(properties);

            if(this.type.equals("initializer")){
                // Seed data after preparing statements
                cassandraService.seedUsers(10);
                cassandraService.seedSectors(4, 15);
                cassandraService.seedMatches(10);
            }else {
                cassandraService.seedUsers(50);
            }

            log.info("Thread "+Thread.currentThread().getName()+" executed completed!");
        } catch (BackendException e) {
            log.error("error occur");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("application.properties file not found");
            }
            properties.load(input);
            return properties;
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
