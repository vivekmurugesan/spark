package com.edureka.cassandra;


import java.util.Set;
import java.util.HashSet;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.edureka.cassandra.csv.VideoDetails;
import com.datastax.driver.core.Cluster.Builder;

/**
 * 
 * @author vivek
 *
 */
public class CassandraClient {

	private Cluster cluster;
	private Session session;
	
	private String nodeIp;
	private Integer port;
	
	public static void main(String[] args) {
		
		String nodeIp = "ip-20-0-31-210.ec2.internal";
		Integer port = 9042;
		
		CassandraClient client = new CassandraClient(nodeIp, port);
		
		client.parseAndLoadData();
		
	}
	
	public CassandraClient(String nodeIp, Integer port) {
		this.nodeIp = nodeIp;
		this.port = port;
	}
	
	public void parseAndLoadData() {
		
		this.connect(nodeIp, port);
		
		// Creating tables
		this.createSchema();
		
		
	}
	
	public void connect() {
		this.connect(this.nodeIp, this.port);
	}
	
	public void connect(String node, Integer port) {
		
		Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        this.cluster = b.build();
 
        this.session = cluster.connect();
        
		/*
		 * cluster = Cluster.builder().addContactPoint(node).build(); Metadata metadata
		 * = cluster.getMetadata(); System.out.println("Connected to cluster:" +
		 * metadata.getClusterName()); for (Host host : metadata.getAllHosts()) {
		 * System.out.println("Datatacenter: " + host.getDatacenter() + "; Host: " +
		 * host.getAddress() + "; Rack: " + host.getRack()); }
		 */
	}

	public void getSession() {
		session = cluster.connect();
	}

	public void closeSession() {
		session.close();
	}

	public void close() {
		cluster.close();
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS Youtube_Stats WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		
		session.execute("CREATE TABLE IF NOT EXISTS Youtube_Stats.Meta_Data ("
				+ "video_id text PRIMARY KEY," + "region text," + "title text," + "channel_title text,"
				+ "trending_date text," + "category_id text," + "publish_time text" + ");");
		
		session.execute("CREATE TABLE IF NOT EXISTS Youtube_Stats.Count_Stats ("
				+ "video_id text PRIMARY KEY," + "views text," + "likes text,"
				+ "dislikes text," + "comments text" + ");");
		
		session.execute("CREATE TABLE IF NOT EXISTS Youtube_Stats.Flag_Details ("
				+ "video_id text PRIMARY KEY," + "comments_disabled text," + "rating_disabled text,"
				+ "video_err_removed text" + ");");
		
	}
	
	public void insertDataFrom(VideoDetails videoDetails) {
		
		// Meta_data
		PreparedStatement statement = session
				.prepare("INSERT INTO Youtube_Stats.Meta_Data "
						+ "(video_id, region, title, channel_title, trending_date, category_id, publish_time) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);
		
		ResultSet res = session.execute(boundStatement.bind(
				videoDetails.getVideoId(),videoDetails.getRegionId(),
				videoDetails.getTitle(), videoDetails.getChannelTitle(),
				videoDetails.getTrendingDate(), videoDetails.getCategoryId(),
				videoDetails.getPublishTime()
				));
		
		System.err.println(res.toString());
		
		// Count_Stats
		statement = session
				.prepare("INSERT INTO Youtube_Stats.Count_Stats "
						+ "(video_id, views, likes, dislikes, comments) "
						+ "VALUES (?, ?, ?, ?, ?);");

		boundStatement = new BoundStatement(statement);
		
		res = session.execute(boundStatement.bind(
				videoDetails.getVideoId(),
				videoDetails.getViewCount(), videoDetails.getLikeCount(),
				videoDetails.getDislikeCount(), videoDetails.getCommentCount()
				));
		
		System.err.println(res.toString());
		
		// Count_Stats
		statement = session
				.prepare("INSERT INTO Youtube_Stats.Flag_Details "
						+ "(video_id, comments_disabled, rating_disabled, video_err_removed) "
						+ "VALUES (?, ?, ?, ?);");

		boundStatement = new BoundStatement(statement);

		res = session.execute(boundStatement.bind(
				videoDetails.getVideoId(),
				videoDetails.getCommentsDisabled(), videoDetails.getRatingsDisabled(),
				videoDetails.getVideoErrOrRemoved()));

		System.err.println(res.toString());
	}

	
	public void querySchema() {
		Statement statement = QueryBuilder.select().all().from("Youtube_Stats", "Meta_Data");
		ResultSet results = session.execute(statement);
		/*
		 * System.out .println(String
		 * .format("%-70s\t%-60s\t%-50s\t%-30s\t%-20s\t%-20s\n%s", "video_id", "region",
		 * "title", "channel_title", "trending_date", "category_id", "publish_time",
		 * "------------------------------------------------------+-------------------------------+------------------------+-----------"
		 * ));
		 */
		for (Row row : results) {
			System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					row.getString("video_id"),
					row.getString("region"),
					row.getString("title"),
					row.getString("channel_title"),
					row.getString("trending_date"),
					row.getString("category_id"),
					row.getString("publish_time")
							);
		}
		System.out.println();
	}

}
