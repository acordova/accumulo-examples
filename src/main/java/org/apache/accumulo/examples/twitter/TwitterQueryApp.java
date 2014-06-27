package org.apache.accumulo.examples.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.examples.util.MiniAccumulo;
import org.apache.accumulo.examples.schema.SchemaQueryClient;
import org.apache.accumulo.examples.twitter.TweetSchema.TwitterUser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class TwitterQueryApp {
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, FileNotFoundException, IOException {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		System.out.println("connecting ...");
		ZooKeeperInstance instance = new ZooKeeperInstance("miniInstance", MiniAccumulo.getZooHost());
		Connector conn = instance.getConnector("root", new PasswordToken("password"));
		String[] auths = new String[0]; // no authorizations needed
		
		SchemaQueryClient<TwitterUser> client = new SchemaQueryClient<>(conn, "twitter", "twitterIndex", TwitterUserQuerySchema.getInstance());
		
		// lookup all users with usernames beginning with 'a'
		System.out.println("===== usernames beginning with 'jo' =====");
		Iterable<TwitterUser> results = client.wildcardObjectLookup("jo", auths);
		
		for(TwitterUser user : results) {
			System.out.println(user.name);
		}
		
		// index lookup for tweets containing a word
		System.out.println("===== tweets containing the word 'today' in 'text' field =====");
		results = client.indexLookup("today", "text", auths);
		for(TwitterUser user : results) {
			System.out.println(user);
		}
	}
}
