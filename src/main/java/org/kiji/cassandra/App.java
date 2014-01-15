package org.kiji.cassandra;

import com.datastax.driver.core.*;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Test C* byte[] wrapper.
 */
public class App {

  static final String KEYSPACE_NAME = "demo";
  static final String TABLE_NAME = "users";

  private Cluster cluster;
  private Session session;

  private static EmbeddedCassandraService cassandra;

  public void setup() throws IOException {
    cassandra = new EmbeddedCassandraService();
    cassandra.start();


    // Connect to the cluster and open a session
    //Cluster cluster = Cluster.builder().addContactPoint("172.16.7.2").build();
    cluster = Cluster.builder().addContactPoint("localhost").build();

    session = cluster.connect();

    // Create the keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME +
        " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE_NAME + "." + TABLE_NAME + " " +
        "( name text PRIMARY KEY, name_as_bytes blob );");
  }

  public void insertData(String name) {
    //----------------------------------------------------------------------------------------------
    // Insert name as String and blob, read back, and very that everything looks good!

    // Emulate what most of Kiji would do, and turn the String into byte[].

    byte[] nameToWriteAsByteArray = Bytes.toBytes(name);
    //byte[] nameToWriteAsByteArray = new byte[10];

    // Now do what we need to do for a C* blob: Turn the byte[] into a ByteBuffer
    ByteBuffer nameToWriteAsByteBuffer = ByteBuffer.wrap(nameToWriteAsByteArray);

    // Some quick sanity checks here
    assert(name.equals(Bytes.toString(nameToWriteAsByteArray)));
    assert(name.equals(Bytes.toString(Bytes.toBytes(nameToWriteAsByteBuffer))));



    // Now actually insert the name as a string and as a blob into the table!
    String queryText = "INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + "(name, name_as_bytes) " +
        "VALUES (?,?);";
    PreparedStatement preparedStatement = session.prepare(queryText);
    session.execute(preparedStatement.bind(name, nameToWriteAsByteBuffer));
  }

  public void readAndCheckData(String name) {
    // Now read the value back out
    String queryText = "SELECT * FROM " + KEYSPACE_NAME + "." + TABLE_NAME + " WHERE name=?";
    PreparedStatement preparedStatement = session.prepare(queryText);
    ResultSet resultSet = session.execute(preparedStatement.bind(name));

    // Now convert the blob into a string
    List<Row> rows = resultSet.all();
    assert(rows.size() == 1);
    Row row = rows.get(0);

    String nameReadAsString = row.getString("name");
    if (!nameReadAsString.equals(name)) {
      System.err.println("Oopsies!  Expected to get name " + name + " from table, but got " + nameReadAsString + " instead!");
    }


    // Read the blob out as a ByteBuffer and convert to String through byte[]
    ByteBuffer nameReadAsBlobByteBuffer = row.getBytes("name_as_bytes");
    System.out.println("Raw byte buffer: " + nameReadAsBlobByteBuffer);

    // This is what we would pass to the normal Kiji code (that is designed to work with HBase):
    byte[] nameReadAsBlobByteArray = new byte[nameReadAsBlobByteBuffer.remaining()];
    nameReadAsBlobByteBuffer.get(nameReadAsBlobByteArray);

    // Now convert back to a string and check
    String nameReadAsBlobString = Bytes.toString(nameReadAsBlobByteArray);

    if (!nameReadAsBlobString.equals(name)) {
      System.err.println("Problem reading back blob!");
      System.err.println("Expected to read back >:" + name +":<");
      System.err.println("Got instead >:" + nameReadAsBlobString + ":<");
      //System.err.println("Client -> table byte[] = " + nameToWriteAsByteArray);
      //System.err.println("Table -> client byte[] = " + nameReadAsBlobByteArray);
    } else {
        System.out.println("Blob matches fine!");
    }
  }

  public void close() {
    cluster.shutdown();
  }

  public static void main(String[] args) {
    App app = new App();
    try {
      app.setup();
      String name = "49ers in the superbowl?!";
      app.insertData(name);
      app.readAndCheckData(name);
      app.close();
    } catch (IOException ioe) {
      System.err.println("Could not start embedded C*!");
    }
  }
}
