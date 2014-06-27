package org.apache.accumulo.examples.twitter;

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
import org.apache.accumulo.examples.index.IndexingClient;
import org.apache.accumulo.examples.util.MiniAccumulo;
import org.apache.accumulo.examples.schema.SchemaIngestClient;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
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
			final SchemaIngestClient<Status> client,
			final IndexingClient<Status> indexingClient,
			final String token,
			final String tokenSecret,
			final String consumerKey,
			final String consumerSecret) {
		
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				try {
					System.out.print(".");
					client.ingest(status);
					indexingClient.index(status);
					
				} catch (MutationsRejectedException ex) {
					Logger.getLogger(TwitterIngestApp.class.getName()).log(Level.ERROR, null, ex);
				}
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
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


	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, TableNotFoundException, TableExistsException {

		Logger.getRootLogger().setLevel(Level.WARN);
		
		System.out.println("connecting ...");
		ZooKeeperInstance instance = new ZooKeeperInstance("miniInstance", MiniAccumulo.getZooHost());
		Connector conn = instance.getConnector("root", new PasswordToken("password"));
		
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxWriteThreads(1);
		config.setMaxLatency(30, TimeUnit.SECONDS);
		config.setMaxMemory(1000000);
		config.setTimeout(1, TimeUnit.MINUTES);
		
		// create the tables
		if (!conn.tableOperations().exists("twitter"))
			conn.tableOperations().create("twitter");
		
		if (!conn.tableOperations().exists("twitterIndex"))
			conn.tableOperations().create("twitterIndex");
		
		SchemaIngestClient<Status> client = new SchemaIngestClient<>(conn, "twitter", TweetSchema.getInstance().getIngestSchema());
		client.open(config);

		IndexingClient<Status> indexingClient = new IndexingClient<>(conn, "twitterIndex", new TweetIndexer());
		indexingClient.open(config);
		
		TwitterIngestApp app = new TwitterIngestApp();
		
		app.startTwitterStream(
				client,
				indexingClient,
				"accessToken",
				"accessTokenSecret",
				"APIKey",
				"APISecret"); 
		
		Thread.sleep(10000);
		System.out.println("stopping ...");
		app.stopTwitterStream();
		client.close();
		indexingClient.close();
	}
}
