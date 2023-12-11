package org.cassandraproject;

import ch.qos.logback.core.net.server.Client;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Main {

    public static void main(String[] args){

        Thread thread = new Thread(new ClientThread("initializer"));
        thread.start();

        for(int i=0;i<10;i++){
            Thread client = new Thread(new ClientThread("client"));
            client.start();
        }

        System.out.println("ELOOOOOOOOOOOOOOOOO");
        log.info("Program executed successfully");
        System.exit(0);
    }
}