package org.apache.accumulo.examples.simple;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.util.MiniAccumulo;
import org.apache.hadoop.io.Text;

public class SimpleReadClient {
	
	public static void main(String[] args) throws AccumuloException, FileNotFoundException, IOException, AccumuloSecurityException, TableNotFoundException {
		
		String instance = "miniInstance";
		String zkServers = MiniAccumulo.getZooHost();
	
		String principal = "root";
		AuthenticationToken authToken = new PasswordToken("password");
		
		ZooKeeperInstance inst = new ZooKeeperInstance(instance, zkServers);
		Connector conn = inst.getConnector(principal, authToken);
		
		// scan the whole table
		System.out.println("=== whole table ===");
		Scanner tableScanner;
		try {
			tableScanner = conn.createScanner("testTable", new Authorizations());
		} catch (TableNotFoundException ex) {
			throw new RuntimeException("table not found - was SimpleIngestClient run?");
		}
		
		for(Map.Entry<Key, Value> kv : tableScanner) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
		}
		tableScanner.close();
		
		// scan a particular row
		System.out.println("\n=== one row ===");
		Scanner singleRowScanner = conn.createScanner("testTable", new Authorizations());
		singleRowScanner.setRange(Range.exact("record000"));
		
		for(Map.Entry<Key, Value> kv : singleRowScanner) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
		}
		singleRowScanner.close();
		
		// scan one column
		System.out.println("\n=== one column ===");
		Scanner singleColumnScanner = conn.createScanner("testTable", new Authorizations());
		singleColumnScanner.fetchColumn(new Text("attribute"), new Text("name"));
		
		for(Map.Entry<Key, Value> kv : singleColumnScanner) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
		}
		singleColumnScanner.close();
		
		// scan one value
		System.out.println("\n=== one value ===");
		Scanner singleValueScanner = conn.createScanner("testTable", new Authorizations());
		singleValueScanner.setRange(Range.exact("record000"));
		singleValueScanner.fetchColumn(new Text("attribute"), new Text("name"));
		
		for(Map.Entry<Key, Value> kv : singleValueScanner) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
		}
		singleValueScanner.close();
		
		// scan one value
		System.out.println("\n=== one value (alternate) ===");
		Scanner singleValueScanner2 = conn.createScanner("testTable", new Authorizations());
		singleValueScanner2.setRange(Range.exact("record000", "attribute", "name"));
		
		for(Map.Entry<Key, Value> kv : singleValueScanner2) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
		}
		singleValueScanner2.close();
	}
}
