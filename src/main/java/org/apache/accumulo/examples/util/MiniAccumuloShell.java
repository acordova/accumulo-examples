package org.apache.accumulo.examples.util;

import java.io.IOException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MiniAccumuloShell {
	
	public static void main(String[] args) throws IOException {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		// start a shell to explore the data
		String[] shellArgs = new String[] {"-u", "root", "-p", "password", "-z", 
			"miniInstance", MiniAccumulo.getZooHost()};

		Shell.main(shellArgs);
	}
}
