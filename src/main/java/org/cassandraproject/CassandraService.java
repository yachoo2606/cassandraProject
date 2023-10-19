package org.cassandraproject;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.util.Map;
import java.util.Properties;

@AllArgsConstructor
@Slf4j
public class CassandraService {
    private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";

    private String address;
    private Integer port;
    private String keySpace;
    private String passwordDB;
    private String usernameDB;

    private Session session;

    public CassandraService(Properties properties) throws BackendException {
        initVariables(properties);

        Cluster cluster = Cluster.builder()
                .addContactPoint(this.address)
                .withPort(this.port)
                .withCredentials(this.usernameDB,this.passwordDB)
                .build();
        try{
            log.info("trying to connect");
            this.session = cluster.connect();
            log.info("Connected to cluster");
        }catch (Exception e){
            log.error(e.getMessage());
            log.error("connect to cluster failed");
            throw new BackendException(e.getMessage());
        }
        createKeySpace();
        session.execute("use " + this.keySpace + ";");
        createTableUsers();
        prepareStatements();
    }

    private void initVariables(Properties properties){
        this.address = System.getenv().getOrDefault("CASSANDRA_SERVER_ADDRESS", properties.getProperty("server.address"));
        this.port = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_SERVER_PORT",properties.getProperty("server.port")));
        this.keySpace = System.getenv().getOrDefault("CASSANDRA_KEYSPACE",properties.getProperty("db.keyspace"));
        this.usernameDB = System.getenv().getOrDefault("CASSANDRA_USER",properties.getProperty("db.username"));
        this.passwordDB = System.getenv().getOrDefault("CASSANDRA_PASSWORD",properties.getProperty("db.password"));

        System.out.println(this.address);
        System.out.println(this.port);
        System.out.println(this.keySpace);
        System.out.println(this.usernameDB);
        System.out.println(this.passwordDB);

        if(this.address == null){
            System.out.println("ERROR INITIALIZE VARIABLE address");
            log.error("ERROR INITIALIZE VARIABLES address");
            System.exit(1);
        }
        if(this.port == null){
            System.out.println("ERROR INITIALIZE VARIABLE port");
            log.error("ERROR INITIALIZE VARIABLES port");
            System.exit(1);
        }
        if(this.keySpace == null){
            System.out.println("ERROR INITIALIZE VARIABLE keySpace");
            log.error("ERROR INITIALIZE VARIABLES keySpace");
            System.exit(1);
        }
        if(this.usernameDB == null){
            System.out.println("ERROR INITIALIZE VARIABLE usernameDB");
            log.error("ERROR INITIALIZE VARIABLES usernameDB");
            System.exit(1);
        }
        if(this.passwordDB == null){
            System.out.println("ERROR INITIALIZE VARIABLE passwordDB");
            log.error("ERROR INITIALIZE VARIABLES passwordDB");
            System.exit(1);
        }
    }
    private void prepareStatements() throws BackendException {

        try{
            SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
            INSERT_INTO_USERS = session.prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
            log.info("Prepared statements");
        }catch (Exception e){
            throw new BackendException("Could not prepare statements. "+e.getMessage(),e);
        }
    }

    public void createKeySpace() throws BackendException {
        KeyspaceOptions keyspaceOptions = SchemaBuilder.createKeyspace(this.keySpace)
                .ifNotExists()
                .with()
                .replication(Map.of("class","SimpleStrategy", "replication_factor",3));
        keyspaceOptions.setConsistencyLevel(ConsistencyLevel.ANY);

        try{
            session.execute(keyspaceOptions);
        }catch (Exception e){
            log.error("creation of keyspace failed! "+e.getMessage());
            throw new BackendException("creation of keyspace failed! "+e.getMessage(),e);
        }
    }

    public void createTableUsers() throws BackendException {
        Create create = SchemaBuilder.createTable(this.keySpace, "users")
                .ifNotExists()
                .addPartitionKey("companyName", DataType.varchar())
                .addPartitionKey("name",DataType.varchar())
                .addColumn("phone",DataType.varchar())
                .addColumn("street",DataType.varchar())
                .addColumn("pets",DataType.list(DataType.varchar()));

        session.execute(create);
    }

    public String selectAll() throws BackendException{
        StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String rcompanyName = row.getString("companyName");
			String rname = row.getString("name");
			String rphone = row.getString("phone");
			String rstreet = row.getString("street");

			builder.append(String.format(USER_FORMAT, rcompanyName, rname, rphone, rstreet));
		}

		return builder.toString();
    }

    public void upsertUser(String companyName, String name, String phone, String street) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);
		bs.bind(companyName, name, phone, street);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		log.info("User " + name + " upserted");
	}

    public void deleteAll() throws BackendException {
        BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);

        try {
            session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
        }

        log.info("All users deleted");
    }


    protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			log.error("Could not close existing cluster", e);
		}
	}
}
