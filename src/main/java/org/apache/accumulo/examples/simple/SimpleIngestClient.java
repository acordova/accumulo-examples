package org.apache.accumulo.examples.simple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.examples.util.MiniAccumulo;


public class SimpleIngestClient {
	
	public static final Logger logger = Logger.getLogger(SimpleIngestClient.class.getCanonicalName());
	
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, FileNotFoundException, IOException {
		
		String instance = "miniInstance";
		String zkServers = MiniAccumulo.getZooHost();
	
		String principal = "root";
		AuthenticationToken authToken = new PasswordToken("password");
		
		ZooKeeperInstance inst = new ZooKeeperInstance(instance, zkServers);
		Connector conn = inst.getConnector(principal, authToken);
		
		try {
			conn.tableOperations().create("testTable");
		} catch (TableExistsException ex) {
			logger.info("table already exists");
		}
		
		BatchWriterConfig config = new BatchWriterConfig();
		
		config.setMaxLatency(1, TimeUnit.MINUTES);
		config.setMaxMemory(10000000);
		config.setMaxWriteThreads(10);
		config.setTimeout(10, TimeUnit.MINUTES);
		
		BatchWriter writer = conn.createBatchWriter("testTable", config);
		
		Mutation m1 = new Mutation("record000");
		
		m1.put("attribute", "name", "Batman");
		m1.put("attribute", "primary super power", "None");
		m1.put("equipment", "personal", "Utility Belt");
		m1.put("transportation", "ground", "Batmobile");
		m1.put("transportation", "air", "Batwing");
		m1.put("transportation", "water", "Batboat");
		
		writer.addMutation(m1);
		
		Mutation m2 = new Mutation("record001");
		
		m2.put("attribute", "name", "Superman");
		m2.put("attribute", "powers", "Flight, Super Strength, Heat Vision");
		m2.put("equipment", "personal", "Cape");
		
		writer.addMutation(m2);
		
		Mutation m3 = new Mutation("record002");
		
		m3.put("attribute", "name", "Wonder Woman");
		m3.put("attribute", "powers", "Flight, Super Strength");
		m3.put("equipment", "personal", "Bracelets, Lasso of Truth");
		m3.put("transportation", "air", "Invisible Plane");
		
		writer.addMutation(m3);
		
		writer.close();
	}
}
