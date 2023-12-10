package org.cassandraproject;

import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Main {

    public static void main(String[] args) throws BackendException {
        Properties properties = loadProperties();

        CassandraService cassandraService = new CassandraService(properties);

//        String output = cassandraService.selectAll();
//		System.out.println("Users: \n" + output);
//
//        cassandraService.upsertUser("PP", "Adam", "609", "A St");
//		cassandraService.upsertUser("PP", "Ola", "509", null);
//		cassandraService.upsertUser("UAM", "Ewa", "720", "B St");
//		cassandraService.upsertUser("PP", "Kasia", "713", "C St");
//
//        output = cassandraService.selectAll();
//		System.out.println("Users: \n" + output);
//
//        cassandraService.deleteAll();


        System.out.println("ELOOOOOOOOOOOOOOOOO");
        log.info("Program executed successfully");
        System.exit(0);
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