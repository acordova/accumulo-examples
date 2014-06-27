package org.apache.accumulo.examples.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.examples.schema.QuerySchema;
import org.apache.accumulo.examples.twitter.TweetSchema.Tweet;
import org.apache.accumulo.examples.twitter.TweetSchema.TwitterUser;

public class TwitterUserQuerySchema extends QuerySchema<TwitterUser> {
	
	private static TwitterUserQuerySchema instance = null;

	private TwitterUserQuerySchema() {}
	
	public static TwitterUserQuerySchema getInstance() {
		if(instance == null)
			instance = new TwitterUserQuerySchema();
		return instance;
	}
	
	@Override
	public TwitterUser apply(Map<Key, Value> row) {

		TwitterUser user = new TwitterUser();
		user.tweets = new ArrayList<>();
		
		String currentTweetId = "";
		Map<Key,Value> tweetKVPairs = new HashMap<>();
		
		for (Map.Entry<Key, Value> e : row.entrySet()) {
			Key k = e.getKey();
			String value = new String(e.getValue().get());
			String colQual = k.getColumnQualifier().toString();
			
			if (k.getColumnFamily().toString().equals("user")) {
				switch (colQual) {
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
			}
			else {
				String[] parts = colQual.split("\\t");
				String id = parts[0];
				if(!id.equals(currentTweetId)) {
					if(tweetKVPairs.size() > 0) {
						user.tweets.add((Tweet)TweetSchema.getInstance().getQuerySchema().apply(tweetKVPairs));
						tweetKVPairs = new HashMap();
					}
				}
				tweetKVPairs.put(e.getKey(), e.getValue());
				currentTweetId = id;
			}
		}
		
		// get last tweet
		if(tweetKVPairs.size() > 0)
			user.tweets.add((Tweet)TweetSchema.getInstance().getQuerySchema().apply(tweetKVPairs));
		
		return user;
	}
}
