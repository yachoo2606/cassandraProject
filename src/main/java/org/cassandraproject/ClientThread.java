package org.cassandraproject;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.util.Properties;

@Slf4j
@AllArgsConstructor
public class ClientThread implements Runnable{

    private Properties properties;

    @Override
    public void run() {
        log.info(Thread.currentThread().getName());
        try {
            log.info("Trying declare cassandraService in "+Thread.currentThread().getName());
            CassandraService cassandraService = new CassandraService(properties);
            cassandraService.useKeyspace();
            cassandraService.prepareStatements();

            log.info("Declared cassandraService in "+Thread.currentThread().getName());

            cassandraService.selectAllUsers();

            log.info("Thread "+Thread.currentThread().getName()+" executed completed!");
        }catch (BackendException e){
            log.error("error occur");
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("error occur");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
