package org.cassandraproject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import lombok.extern.slf4j.Slf4j;
import org.cassandraproject.exception.BackendException;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class CassandraService {
    private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static final String USER_FORMAT = "- ID: %-10d name: %-16s";

    private final List<String> addresses = new ArrayList<>();
    private String selectedAddress;
    private Integer port;
    private String keySpace;
    private String passwordDB;
    private String usernameDB;

    private Session session;

    public CassandraService(Properties properties){
        System.out.println("Initializing variables in CassandraService in "+Thread.currentThread().getName());
        log.info("Initializing variables in CassandraService in "+Thread.currentThread().getName());
        initVariables(properties);
        this.selectedAddress = getRandomAddress();
        log.info("Threar: "+Thread.currentThread().getName()+" picked: "+this.selectedAddress);

        try {
            Cluster cluster = Cluster.builder()
                .addContactPoint(this.selectedAddress)
                .withPort(this.port)
                .withCredentials(this.usernameDB, this.passwordDB)
                .build();

            log.info("trying to connect");
            log.info("Trying to connect to Cassandra cluster at " + selectedAddress);
            this.session = cluster.connect();
            log.info("Connected to cluster");
        } catch (Exception e) {
            log.error(e.getMessage());
            log.error("connect to cluster failed");
        }
    }

    public void initTables() throws BackendException {
        // Create tables first
            createTableUsers();
            createTableSectors();
            createTableSeats();
            createTableMatches();
            createMatchUsersSeatsTable();
    }

    private String getRandomAddress() {
        Random random = new Random();
        int index = random.nextInt(this.addresses.size());
        return this.addresses.get(index);
    }

    private void initVariables(Properties properties){
        String addressOne = System.getenv().getOrDefault("CASSANDRA_SERVER_ADDRESS_ONE", properties.getProperty("server.address_one"));
        String addressTwo = System.getenv().getOrDefault("CASSANDRA_SERVER_ADDRESS_TWO", properties.getProperty("server.address_two"));
        String addressThree = System.getenv().getOrDefault("CASSANDRA_SERVER_ADDRESS_THREE", properties.getProperty("server.address_three"));

        this.port = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_SERVER_PORT",properties.getProperty("server.port")));
        this.keySpace = System.getenv().getOrDefault("CASSANDRA_KEYSPACE",properties.getProperty("db.keyspace"));
        this.usernameDB = System.getenv().getOrDefault("CASSANDRA_USER",properties.getProperty("db.username"));
        this.passwordDB = System.getenv().getOrDefault("CASSANDRA_PASSWORD",properties.getProperty("db.password"));

        if(addressOne == null){
            System.out.println("ERROR INITIALIZE VARIABLE address");
            log.error("ERROR INITIALIZE VARIABLES address one");
            System.exit(1);
        }
        if(addressTwo == null){
            System.out.println("ERROR INITIALIZE VARIABLE address");
            log.error("ERROR INITIALIZE VARIABLES address two");
            System.exit(1);
        }
        if(addressThree == null){
            System.out.println("ERROR INITIALIZE VARIABLE address");
            log.error("ERROR INITIALIZE VARIABLES address three");
            System.exit(1);
        }

        this.addresses.add(addressOne);
        this.addresses.add(addressTwo);
        this.addresses.add(addressThree);

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

        System.out.println(this.addresses);
        System.out.println(this.port);
        System.out.println(this.keySpace);
        System.out.println(this.usernameDB);
        System.out.println(this.passwordDB);

    }
    public void prepareStatements() throws BackendException {

        try{
            SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
            INSERT_INTO_USERS = session.prepare("INSERT INTO users (id,name) VALUES (?, ?);");
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
            log.info("Keyspace created successful");
        }catch (Exception e){
            log.error("creation of keyspace failed! "+e.getMessage());
            throw new BackendException("creation of keyspace failed! "+e.getMessage(),e);
        }
    }

    public void useKeyspace() {
        session.execute("use " + this.keySpace + ";");
        log.info("Keyspace switched successful");
    }

    public void createTableUsers() {
        Create create = SchemaBuilder.createTable(this.keySpace, "users")
                .ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.varchar());
        session.execute(create);
        log.info("Table users created successful");
    }


    public void createTableSectors(){
        Create create = SchemaBuilder.createTable(this.keySpace, "sectors")
                .ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name",DataType.varchar())
                .addColumn("active",DataType.cboolean());
        session.execute(create);
        log.info("Table sectors created successful");
    }

    public void createTableSeats(){
        Create create = SchemaBuilder.createTable(this.keySpace, "seats")
                .ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("number",DataType.varchar())
                .addPartitionKey("sector_id", DataType.bigint())
                .addColumn("active",DataType.cboolean());
        session.execute(create);
        log.info("Table seats created successful");
    }

    public void createTableMatches(){
        Create create = SchemaBuilder.createTable(this.keySpace, "matches")
                .ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.varchar())
                .addColumn("match_datetime", DataType.timestamp());
        session.execute(create);
        log.info("Table matches created successful");
    }

    public void createMatchUsersSeatsTable(){
        Create create = SchemaBuilder.createTable(this.keySpace, "match_users_seats")
                .ifNotExists()
                .addPartitionKey("match_id", DataType.bigint())
                .addClusteringColumn("user_id", DataType.bigint())
                .addClusteringColumn("seat_id", DataType.bigint());
        session.execute(create);
        log.info("Table MatchuserSeats created successful");
    }

    public String selectAllUsers() throws BackendException{
        StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);

		ResultSet rs;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			Long rid = row.getLong("id");
			String rname = row.getString("name");

			builder.append(String.format(USER_FORMAT, rid, rname));
            log.info(String.format(USER_FORMAT, rid, rname));
		}

		return builder.toString();
    }

    public void upsertUser(BigInteger id, String name) throws BackendException {
        BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);

        // Convert BigInteger to Long
        Long longId = id.longValue();

        bs.bind(longId, name);

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

    public void seedUsers(int numberOfUsers) throws BackendException {
        for (int i = 1; i <= numberOfUsers; i++) {
            upsertUser(BigInteger.valueOf(i), "User" + i);
        }
        log.info(numberOfUsers + " users seeded.");
    }

    public void seedSectors(int numberOfSectors, int seatsPerSector) throws BackendException {
        int currentVal = 1;
        for (int i = 1; i <= numberOfSectors; i++) {
            createSector(BigInteger.valueOf(i), seatsPerSector, BigInteger.valueOf(currentVal));
            currentVal += seatsPerSector;
        }
        log.info(numberOfSectors + " sectors seeded.");
    }

    private void createSector(BigInteger sectorId, int seatsPerSector, BigInteger currentSeatId) throws BackendException {
        try {
            // Create sector
            session.execute("INSERT INTO sectors (id, name, active) VALUES (?, ?, ?);",
                    sectorId.longValue(), "Sector" + sectorId.longValue(), true);

            // Seed seats for the sector
            seedSeats(sectorId, seatsPerSector, currentSeatId);

        } catch (Exception e) {
            throw new BackendException("Error seeding sectors: " + e.getMessage(), e);
        }
    }

    private void seedSeats(BigInteger sectorId, int seatsPerSector, BigInteger currentSeatId) throws BackendException {
        try {
            long currentId = currentSeatId.intValue();
            for (long seatNumber = 1; seatNumber <= seatsPerSector; seatNumber++) {
                session.execute("INSERT INTO seats (id, number, sector_id, active) VALUES (?, ?, ?, ?);",
                        currentId, "Seat" + seatNumber, sectorId.longValue(), true);
                currentId++;
            }
        } catch (Exception e) {
            throw new BackendException("Error seeding seats: " + e.getMessage(), e);
        }
    }

    public void seedMatches(int numberOfMatches) throws BackendException {
        int day = 1;
        for (int i = 1; i <= numberOfMatches; i++) {
            createMatch(i, "Match" + i, LocalDate.of(2024, 1, day), LocalTime.of(20, 0));
            day++;
        }
        log.info(numberOfMatches + " matches seeded.");
    }

    private void createMatch(long matchId, String name, LocalDate matchDate, LocalTime matchTime) throws BackendException {
        try {
            // Combine LocalDate and LocalTime to create a LocalDateTime
            LocalDateTime localDateTime = LocalDateTime.of(matchDate, matchTime);

            // Convert LocalDateTime to Timestamp
            Timestamp timestamp = Timestamp.valueOf(localDateTime);

            session.execute("INSERT INTO matches (id, name, match_datetime) VALUES (?, ?, ?);",
                    matchId, name, timestamp);
        } catch (Exception e) {
            throw new BackendException("Error seeding matches: " + e.getMessage(), e);
        }
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
