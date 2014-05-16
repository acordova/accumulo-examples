package org.apache.accumulo.examples;

import com.google.common.base.Function;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.shell.Shell;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.auth.AccessToken;

public class TwitterIngestApp {
	
	private TwitterStream twitterStream;
	
	/**
	 *
	 * This method is event driven.
	 *
	 * For each tweet that comes in, we write a mutation to our IngestClient
	 *
	 * From http://twitter4j.org/en/code-examples.html
	 */
	void startTwitterStream(
			final IngestClient<Status> client,
			final String token,
			final String tokenSecret,
			final String consumerKey,
			final String consumerSecret) {
		
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				try {
					System.out.print(".");
					client.ingest(status);

				} catch (MutationsRejectedException ex) {
					Logger.getLogger(TwitterIngestApp.class.getName()).log(Level.ERROR, null, ex);
				}
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			public void onScrubGeo(long l, long l1) {
			}

			public void onStallWarning(StallWarning sw) {
			}
		};

		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);

		AccessToken accessToken = new AccessToken(token, tokenSecret);

		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(accessToken);
		
		twitterStream.sample();
	}
	
	void stopTwitterStream() {
		twitterStream.shutdown();
	}

	/**
	 * Define how we're going to store twitter data
	 *
	 * This table stores user information along with a list of their tweets
	 * within one row.
	 *
	 * The user information gets updated with each tweet.
	 */
	private static class StatusToMutation implements Function<Status, Mutation> {

		public Mutation apply(Status status) {

			Mutation m = new Mutation(status.getUser().getScreenName());

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
			m.put("tweetContent", idString + "\ttext", status.getText());

			m.put("tweetDetails", idString + "\tsource", status.getSource());
			m.put("tweetDetails", idString + "\tfavoriteCount", Long.toString(status.getFavoriteCount()));
			m.put("tweetDetails", idString + "\tlang", status.getLang());

			// write entity info
			UserMentionEntity[] musers = status.getUserMentionEntities();
			for (UserMentionEntity muser : musers) {
				m.put("tweetEntities", idString + "\tuser\t" + muser.getScreenName(), "");
			}

			HashtagEntity[] hashtags = status.getHashtagEntities();
			for (HashtagEntity hashtag : hashtags) {
				m.put("tweetEntities", idString + "\thashtags\t" + hashtag.getText(), "");
			}

			return m;
		}
	}

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, TableNotFoundException, TableExistsException {

		Logger.getRootLogger().setLevel(Level.WARN);
		
		
		System.out.println("connecting ...");
		ZooKeeperInstance instance = new ZooKeeperInstance("miniInstance", MiniAccumulo.HOST);
		Connector conn = instance.getConnector("root", new PasswordToken("password"));
		
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxWriteThreads(1);
		config.setMaxLatency(30, TimeUnit.SECONDS);
		config.setMaxMemory(1000000);
		config.setTimeout(1, TimeUnit.MINUTES);
		
		// create the table
		if (!conn.tableOperations().exists("twitter"))
			conn.tableOperations().create("twitter");
		
		
		IngestClient client = new IngestClient<Status>(conn, "twitter", new StatusToMutation());
		client.open(config);
		
		TwitterIngestApp app = new TwitterIngestApp();
		
		app.startTwitterStream(
				client,
				"16905891-JqlYGc28xQJz2qllA52j2oWI0QOqI4FIbq4vaJG8Y",
				"uMD8R3J78RdzrrTkAx6AcHsNQJ6u877ejZeCdDw",
				"ZErE2u2aeXhQZ0zOWo2L2Q",
				"0WTiOVkSKt9t65gep6haFsfKl65iwkvodyo0W2wdA"); 
		
		Thread.sleep(10000);
		System.out.println("stopping ...");
		app.stopTwitterStream();
		client.close();
		
	}
}
