/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.accumulo.examples;

import java.io.IOException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author aaron
 */
public class MiniAccumuloShell {
	
	public static void main(String[] args) throws IOException {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		// start a shell to explore the data
		String[] shellArgs = new String[] {"-u", "root", "-p", "password", "-z", 
			"miniInstance", MiniAccumulo.HOST};

		Shell.main(shellArgs);
	}
}
