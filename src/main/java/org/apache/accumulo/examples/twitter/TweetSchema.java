package org.apache.accumulo.examples.twitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.examples.index.Indexer;
import org.apache.accumulo.examples.schema.IngestSchema;
import org.apache.accumulo.examples.schema.QuerySchema;
import org.apache.accumulo.examples.schema.Schema;
import twitter4j.Status;
import twitter4j.User;

/**
 * This schema takes Status objects from Twitter and maps them to
 * TwitterUser and Tweet objects, which are defined by this code
 * 
 */
public class TweetSchema implements Schema {
	
	private static TweetSchema instance = null;
	private final QuerySchema querySchema = new TweetQuerySchema();
	private final IngestSchema ingestSchema = new StatusIngestSchema();
	private final Indexer indexer = new TweetIndexer();

	private TweetSchema() {}
	
	public static TweetSchema getInstance() {
		if(instance == null)
			instance = new TweetSchema();
		return instance;
	}
	@Override
	public QuerySchema getQuerySchema() {
		return querySchema;
	}

	@Override
	public IngestSchema getIngestSchema() {
		return ingestSchema;
	}

	private class StatusIngestSchema extends IngestSchema<Status> {

		@Override
		public Mutation apply(Status status) {

			Mutation m = new Mutation(indexer.recordID(status));

			String idString = Long.toHexString(status.getId());

			// update user info
			User user = status.getUser();
			if(user != null) {
				m.put("user", "description", user.getDescription() == null ? "" : user.getDescription());
				m.put("user", "followers", Long.toString(user.getFollowersCount()));
				m.put("user", "location", user.getLocation() == null ? "" : user.getLocation());
				m.put("user", "name", user.getName() == null ? "" : user.getName());
				m.put("user", "statusesCount", Long.toString(user.getStatusesCount()));
			}
			
			// write tweet info
			m.put("tweet", idString + "\ttext", status.getText());
			m.put("tweet", idString + "\tsource", status.getSource());
			m.put("tweet", idString + "\tfavoriteCount", Long.toString(status.getFavoriteCount()));
			m.put("tweet", idString + "\tlang", status.getLang());

			return m;
		}
	}
	
	private static class TweetQuerySchema extends QuerySchema<Tweet> {

		@Override
		public Tweet apply(Map<Key, Value> map) {
			Tweet t = new Tweet();
			
			for(Map.Entry<Key, Value> entry : map.entrySet()) {
				
				if(!entry.getKey().getColumnFamily().toString().equals("tweet"))
					continue;
				
				String[] parts = entry.getKey().getColumnQualifier().toString().split("\\t");
				t.id = parts[0];
				switch(parts[1]) {
					case "text":
						t.text = new String(entry.getValue().get());
						break;
					case "source":
						t.source = new String(entry.getValue().get());
						break;
					case "favoriteCount":
						t.favoriteCount = new String(entry.getValue().get());
						break;
					case "lang":
						t.lang = new String(entry.getValue().get());
						break;
				}
			}
			
			return t;
		}	
	}
	
	public static class Tweet {
		public String id = "";
		public String text = "";
		public String favoriteCount = "";
		public String lang = "";
		public String source = "";
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append("id\t" + id + "\n");
			sb.append("text\t" + text + "\n");
			sb.append("lang\t" + lang + "\n");
			
			return sb.toString();
		}
	}
	
	/**
	 * 
	 * Our own application object to represent what we 
	 * care about as far as a Twitter User
	 * 
	 */
	public static class TwitterUser {
		
		public String screenName = "";
		public String name;
		public String description = "";
		public String followers = "";
		public String location = "";
		public String statusesCount = "";
		
		public List<Tweet> tweets = new ArrayList<>();
		
		@Override
		public String toString() {
			
			StringBuilder sb = new StringBuilder();
			
			sb.append("screenName\t" + screenName + "\n");
			sb.append("name\t" + name + "\n");
			sb.append("location\t" + location + "\n");
			
			sb.append("---- tweets ----\n");
			for(Tweet tweet : tweets) {
				sb.append(tweet.toString() + "\n");
			}
			sb.append("\n");
			
			return sb.toString();
		}
	}
}
