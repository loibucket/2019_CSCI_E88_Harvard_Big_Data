package cscie88.cassandra;

import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

public class BasicCRUD {
	private String cassandraSeedNode = "localhost";
	private String keyspaceName = "lab10";
	private String tableName = "sensor_metrics";
	private Cluster cluster = null;
	private Session session = null; 
	private PreparedStatement insertStatement;
	private PreparedStatement getAllByTimeRangeStmt;
	private String getAllByTimeRangeCQL = 
			"select * from lab10.sensor_metrics where sensor_type=? " + 
			" and time_hour=? and reading_time > ? and reading_time <= ?;";
	
	public BasicCRUD() {
	}

	public BasicCRUD(String cassandraSeedNode, String keyspaceName, String tableName) {
		super();
		this.cassandraSeedNode = cassandraSeedNode;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
	}

	private void init() {
		try {
			cluster = Cluster.builder().addContactPoint(cassandraSeedNode).build();
			cluster.getConfiguration().getCodecRegistry().register(InstantCodec.instance);
			session = cluster.connect(keyspaceName);	
			insertStatement = session.prepare(
					"insert into " + keyspaceName + "." + tableName +
					" (sensor_type, time_hour, reading_time, sensor_id, metric) " +
					"values (:sensorType, :timeHour, :readingTime, :sensorId, :metric)" );
			getAllByTimeRangeStmt = session.prepare(getAllByTimeRangeCQL);
		} catch (Exception e) {
			System.out.println("Error connecting to Cassandra: " + e.getMessage());
			throw new RuntimeException("Error connecting to Cassandra: ", e);
		}
	}
	
	private void cleanup() {
		if (cluster != null && session != null) {
			try {
				session.close();
				cluster.close();
			} catch (Exception e) {
				System.out.println("Error closing resources for Cassandra: " + e.getMessage());
			}
		}
	}
	
	public void runDemoJob() {
		init();
		String sensorType1 = "type1";
		String sensorType2 = "type2";
		Float baseMetricValue = 1.0f;
		Instant hourNow = Instant.now().truncatedTo(ChronoUnit.HOURS);
		int numberOfEvents = 5;
		// generate specified number of events for each sensor type, 2 minutes apart
		for (int i=0; i<numberOfEvents; i++) {
			insertRow(sensorType1, hourNow, hourNow.plus(i+2, ChronoUnit.MINUTES), 
					baseMetricValue+i);
			insertRow(sensorType2, hourNow, hourNow.plus(i+2, ChronoUnit.MINUTES), 
					baseMetricValue+i);
		}
		executeQuery(sensorType1, hourNow, hourNow, hourNow.plus(30, ChronoUnit.MINUTES));
		
		cleanup();
	}
	
	public void insertRow(String sensorType, Instant timeHour, Instant readingTime, Float metricValue) {
		BoundStatement boundStmt = insertStatement
				.bind()
				.setUUID("sensorId", UUID.randomUUID())
				.setTimestamp("timeHour", Date.from(timeHour)) 
				.setTimestamp("readingTime", Date.from(readingTime)) 
				.setString("sensorType", sensorType)
				.setFloat("metric", metricValue);
		session.execute(boundStmt);
		System.out.println("Inserted event into " + tableName + ": timeHour=" + 
				timeHour + ", readingTime=" + readingTime + ", metric=" + metricValue);
	}

	// TODO do real exception handling!
	public void executeQuery(String sensorType, Instant timeHour, Instant readingTimeStart, Instant readingTimeEnd) {
		ResultSet resultSet = session.execute(getAllByTimeRangeStmt.bind(
			sensorType, timeHour, readingTimeStart, readingTimeEnd));
		System.out.println("Result of query: ");
		//It should return one or none records
		resultSet.forEach(r -> {
			System.out.println("sensorType: " + r.getString("sensor_type"));
			System.out.println("timeHour: " + r.get("time_hour", InstantCodec.instance));
			System.out.println("readingTime: " + r.get("reading_time", InstantCodec.instance));
			System.out.println("metric: " + String.valueOf(r.getFloat("metric")));
		});
	}
	
	public static void main(String[] args) {
		BasicCRUD basicCRUD = new BasicCRUD();
		basicCRUD.runDemoJob();
	}
}
