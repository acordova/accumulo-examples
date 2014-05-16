/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.accumulo.examples;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author aaron
 */
public class TwitterQueryApp {
	
	public static class Tweet {
		public String id = "";
		public String text = "";
		public String favoriteCount = "";
		public String lang = "";
		public String source = "";
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append("text\t" + text + "\n");
			sb.append("lang\t" + lang + "\n");
			
			return sb.toString();
		}
	}
	
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
	
	private static class ReadSchema implements Function<Map<Key,Value>,TwitterUser> {

		@Override
		public TwitterUser apply(Map<Key, Value> row) {
			
			TwitterUser user = new TwitterUser();
			
			Tweet currentTweet = null;
			
			for(Map.Entry<Key, Value> e : row.entrySet()) {
				Key k = e.getKey();
				String value = new String(e.getValue().get());
				
				
				switch(k.getColumnFamily().toString()) {
					case "user":
						switch(k.getColumnQualifier().toString()) {
							case "description":
								user.description = value;
								break;
							case "followers":
								user.followers = value;
								break;
							case "location":
								user.location = value;
								break;
							case "name":
								user.name = value;
								break;
							case "statusesCount":
								user.statusesCount = value;
								break;
						}
						break;
					case "tweetContent":
						String parts[] = k.getColumnQualifier().toString().split("\t");
						String id = parts[0];
						if(currentTweet != null)
							user.tweets.add(currentTweet);
						currentTweet = new Tweet();
						currentTweet.id = id;
						
						switch(parts[1]) {
							case "text":
								currentTweet.text = value;
								break;
						}
						break;
					case "tweetDetails":
						String detailParts[] = k.getColumnQualifier().toString().split("\t");
						switch(detailParts[1]) {
							case "source":
								currentTweet.source = value;
								break;
							case "favoriteCount":
								currentTweet.favoriteCount = value;
								break;
							case "lang":
								currentTweet.lang = value;
								break;
						}
						break;
				}
			}
			
			if(currentTweet != null)
				user.tweets.add(currentTweet);
			
			return user;
		}	
	} 
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		
		System.out.println("connecting ...");
		ZooKeeperInstance instance = new ZooKeeperInstance("miniInstance", MiniAccumulo.HOST);
		Connector conn = instance.getConnector("root", new PasswordToken("password"));
		
		QueryClient client = new QueryClient(conn, "twitter", new ReadSchema());
		
		Iterable<TwitterUser> results = client.wildcardObjectLookup("a", new String[0]);
		
		for(TwitterUser user : results) {
			System.out.println(user);
		}
		
		
	}
}
