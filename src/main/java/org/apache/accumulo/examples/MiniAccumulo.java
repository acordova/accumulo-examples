/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.accumulo.examples;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author aaron
 */
public class MiniAccumulo {

	static String HOST = "localhost:4533";
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		// run in Accumulo MAC
		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();
		MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");
		accumulo.start();

		System.out.println("starting up ...");
		Thread.sleep(3000);
		
		System.out.println("cluster running with instance name " + accumulo.getInstanceName() + " and zookeepers " + accumulo.getZooKeepers());
		
		System.out.println("hit Enter to shutdown ..");
		
		System.in.read();
		
		accumulo.stop();
	}
}
