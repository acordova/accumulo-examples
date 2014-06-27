# Accumulo Examples

These are simple examples designed to help people getting started with Accumulo.

The examples here make use of the Accumulo Mini Cluster, which runs client code with Accumulo processes within a single JVM for convenience of learning the API without having to setup a cluster.

## Contents
- [Running MiniAccumulo](#running)
- [SimpleIngestClient.java](#simpleingest)
- [Using the Shell](#shell)
- [SimpleReadClient.java](#simpleread)
- [Twitter Ingest](#twitteringest)
- [Twitter Query](#twitteringest)

## <a name="running"></a> Running the MiniAccumulo cluster

The examples rely on the MiniAccumuloCluster to be running before they can ingest or read data. To start the MiniCluster, run the class org.apache.accumulo.examples.util.MiniAccumulo either in a terminal window or from within an IDE. 

It will print out information about when it is ready to be used. When finished, hit any key in the terminal window or input console in the IDE to cause the MiniAccumulo cluster to terminate. All data used will be deleted on shutdown.


## <a name="simpleingest"></a>SimpleIngestClient.java
This code is a toy example showing only how the basic write API of Accumulo is used. Typically production clients will have more structure in order to map external data or programmatic objects to and from Accumulo tables. This client also does not take advantage of all of the features of the Accumulo data model - specifically parts of the key like Column Visibility and other features like locality groups.

Every Accumulo client requires the following information to connect:

- instance name
- zookeeper server list
- principal
- authentication token


The example code connects to a locally running MiniAccumulo cluster thus:

	String instance = "miniInstance";
	String zkServers = MiniAccumulo.HOST;
	
	String principal = "root";
	AuthenticationToken authToken = new PasswordToken("password");
		
	ZooKeeperInstance inst = new ZooKeeperInstance(instance, zkServers);
	Connector conn = inst.getConnector(principal, authToken);

The Connector object is used to obtain Scanners, BatchScanners, and BatchWriters, and to configure tables and system settings. To create a table, use the TableOperations object:

	try {
		conn.tableOperations().create("testTable");
	} catch (TableExistsException ex) {
		logger.info("table already exists");
	}

Scanners and BatchScanners are for reading data, BatchWriters are for writing. The SimpleIngestClient class writes a few Key Value pairs to an Accumulo table. BatchWriters are configured via the BatchWriterConfig object:

	BatchWriterConfig config = new BatchWriterConfig();

	config.setMaxLatency(1, TimeUnit.MINUTES);
	config.setMaxMemory(10000000);
	config.setMaxWriteThreads(10);
	config.setTimeout(10, TimeUnit.MINUTES);
		
	conn.createBatchWriter("testTable", config);

Typically applications map existing data or programmatic objects to Key and Value objects. Keys are broken into the following components, in order:

- Row
- Column Family
- Column Qualifier
- Column Visibility
- Timestamp

All Keys that have the same Row are considered to be one logical row, and are stored next to each other in a table. Thus reading all the information in a single row can be done by looking up the first Key in the row and scanning the following Key Value pairs that share the same Row.

Key Value pairs are sorted by Key, and Keys in a table are sorted by comparing the Row, then Column Family, and so on.

To write information to a table in Accumulo, changes to a single Row are packaged up into a Mutation. Changes are added to a Mutation via the put() method. In this example we're filling out only the Column Family, Column Qualifier, and the Value. This version of the put() method takes Strings for each 

	Mutation m = new Mutation("record000");
		
	m.put("attribute", "name", "Batman");
	m.put("attribute", "primary super power", "None");
	m.put("equipment", "personal", "Utility Belt");
	m.put("transportation", "ground", "Batmobile");
	m.put("transportation", "air", "Batwing");
	m.put("transportation", "water", "Batboat");
	
	...
		
	writer.addMutation(m);		
	writer.close();
	
Upon closing the BatchWriter, the mutations will be flushed to the table in the MiniCluster and will be visible via the Shell or to clients.

## <a name="shell"></a> Viewing data using the Shell

The example code can connect to the MiniCluster using the Accumulo Shell to inspect tables and change configuration.

To start a shell, run the MiniClusterShell class in a terminal via 

	mvn exec:java -Dexec.mainClass="org.apache.accumulo.example.util.MiniClusterShell"
	
or from within an IDE.

The shell will display a prompt:

	Shell - Apache Accumulo Interactive Shell
	- 
	- version: 1.5.0
	- instance name: miniInstance
	- instance id: XXXXXX
	- 
	- type 'help' for a list of available commands
	- 
	root@miniInstance> 

A list of tables can be obtained via the *tables* command:

	root@miniInstance> tables
	tables
	!METADATA
	testTable
	root@miniInstance testTable>

A table can be selected to work with via the *table* command:

	root@miniInstance> table testTable
	table testTable
	root@miniInstance testTable>

To scan the key value pairs in this table from the beginning, use the *scan* command:

	root@miniInstance testTable> scan
	scan
	record000 attribute:name []    Batman
	record000 attribute:primary super power []    None
	record000 equipment:personal []    Utility Belt
	record000 transportation:air []    Batwing
	record000 transportation:ground []    Batmobile
	record000 transportation:water []    Batboat
	record001 attribute:name []    Superman
	record001 attribute:powers []    Flight, Super Strength, Heat Vision
	record001 equipment:personal []    Cape
	record002 attribute:name []    Wonder Woman
	record002 attribute:powers []    Flight, Super Strength
	record002 equipment:personal []    Bracelets, Lasso of Truth
	record002 transportation:air []    Invisible Plane
	root@miniInstance testTable> 
	
A single row can be specified in a scan:
	
	root@miniInstance testTable> scan -r record000
	scan -r record000
	record000 attribute:name []    Batman
	record000 attribute:primary super power []    None
	record000 equipment:personal []    Utility Belt
	record000 transportation:air []    Batwing
	record000 transportation:ground []    Batmobile
	record000 transportation:water []    Batboat
	
Individual key value pairs can be inserted via the shell:

	root@miniInstance testTable> insert record000 attribute height 6ft
	root@miniInstance testTable> scan -r record000
	scan -r record000
	record000 attribute:height []    6ft
	record000 attribute:name []    Batman
	record000 attribute:primary super power []    None
	record000 equipment:personal []    Utility Belt
	record000 transportation:air []    Batwing
	record000 transportation:ground []    Batmobile
	record000 transportation:water []    Batboat

A subset of columns can be scanned:

	root@miniInstance testTable> scan -c attribute:name
	scan -c attribute:name
	record000 attribute:name []    Batman
	record001 attribute:name []    Superman
	record002 attribute:name []    Wonder Woman
	
A complete list of commands can be obtained via the *help* command. Additional help for particular commands can be seen by typeing *help \[name of command\]*


## <a name="simpleread"></a> SimpleReadClient.java
The SimpleReadClient class demonstrates how to scan an entire table, how to select an entire row, how to scan specific columns across rows, and how to select a single value by specifying a row and column.

Most applications will use one or more of these techniques to retrieve data quickly in response to user requests. Note that records can only be selected efficiently by specifying the row.

To read data, we connect as before in the SimpleIngestClient. To read out the whole table, we only need to create a Scanner on our table. By default the Scanner will iterate over all key value pairs in the table:

	Scanner tableScanner = conn.createScanner("testTable", new Authorizations());
	for(Map.Entry<Key, Value> kv : tableScanner) {
			System.out.println(kv.getKey().getRow() + " " 
					+ kv.getKey().getColumnFamily() + " "
					+ kv.getKey().getColumnQualifier() + ": "
					+ new String(kv.getValue().get()));
	}
	tableScanner.close();

To scan one row, we can pass our Scanner a single Range over which to scan. There are lots of ways of specifying ranges. We'll do one of the simplest Ranges, which is from the beginning of a particular row to the end of that row:

	Scanner singleRowScanner = conn.createScanner("testTable", new Authorizations());
	singleRowScanner.setRange(Range.exact("record000"));
		
	for(Map.Entry<Key, Value> kv : singleRowScanner) {
		...
	}

We get all the columns found for each row, and the columns returned may not be the same from one row to the next. To retrieve only selected columns we can tell the scanner which columns to fetch:

	Scanner singleColumnScanner = conn.createScanner("testTable", new Authorizations());
	singleColumnScanner.fetchColumn(new Text("attribute"), new Text("name"));
		
	for(Map.Entry<Key, Value> kv : singleColumnScanner) {
		...
	}

Finally, we can combine both techniques to retrieve a subset of rows and subset of columns. The most extreme case is to retrieve a single value. 

	Scanner singleValueScanner = conn.createScanner("testTable", new Authorizations());
	singleValueScanner.setRange(Range.exact("record000"));
	singleValueScanner.fetchColumn(new Text("attribute"), new Text("name"));
		
	for(Map.Entry<Key, Value> kv : singleValueScanner) {
		...
	}

This can also be done by fully specifying the key when passing a Range to the Scanner:

	Scanner singleValueScanner2 = conn.createScanner("testTable", new Authorizations());
	singleValueScanner2.setRange(Range.exact("record000", "attribute", "name"));
		
	for(Map.Entry<Key, Value> kv : singleValueScanner2) {
		...
	}

## <a name="twitteringest"></a> TwitterIngestApp.java
The TwitterIngest class is designed to demonstrate how to import data from an actual external data source, how to map the external data to the Accumulo data model, and how to populate a secondary index table to allow searching the data by specifying query terms for several different fields.

This class also introduces secondary indexing, writing index entries for a few other fields to a separate table which is used to find Tweets and TwitterUsers.

### A Note on Retrieving Twitter Data
The ingest class requires four pieces of information in authenticate to Twitter and download a sample of live public tweets. To obtain these credentials, visit dev.twitter.com, sign in using a valid Twitter account, and create an application. 

You will then be able to retrieve the following items from the API keys tab after selecting your application:

- API key
- API secret
- Access token
- Access token secret

Note these and use them to fill in the parameters of the TwitterIngest class. You may have to explicitly generate the access token and access token secret via a button on that page.


Like all Accumulo apps, we first define how to connect to Accumulo, then we create two clients for writing data to our primary table, and an index table:

#### SchemaIngestClient.java
This class handles writing mutations to a primary table, in our case 'twitter'.

To configure it, we only need to pass it the name of the primary table and our Ingest Schema

	SchemaIngestClient<Status> client = new SchemaIngestClient<>(conn, "twitter", TweetSchema.getInstance().getIngestSchema());
	client.open(config);

#### IndexingClient.java

This class handles writing mutations to a secondary index table, in our case 'twitterIndex'. We pass it our TweetIndexer class which defines which fields will be indexed:

	IndexingClient<Status> indexingClient = new IndexingClient<>(conn, "twitterIndex", new TweetIndexer());
	indexingClient.open(config);


Then we create our ingest app and configure it with our credentials and our ingest client objects:
	
	TwitterIngestApp app = new TwitterIngestApp();
		
	app.startTwitterStream(
			client,
			indexingClient,
			"accessToken",
			"accessTokenSecret",
			"APIKey",
			"APISecret"); 

This example ingests a sample of the public Twitter stream for 10 seconds then exits.
	
	Thread.sleep(10000);
	System.out.println("stopping ...");
		app.stopTwitterStream();
		client.close();
		indexingClient.close();



## TwitterSchema.java
This class isolates the mapping of external data to the Accumulo data model. These schema classes are passed to a schema-aware ingest and query clients, *SchemaIngestClient* and *SchemaQueryClient*, respectively.

#### TwitterSchema.StatusIngestSchema
This class converts Twitter Status objects into Mutations for writing to our primary *twitter* table. In this schema, we're writing out information for a Twitter user, as well as creating a list of Tweets from that user, all stored within the same row thus. 

(Items in brackes are values read from external Status objects)

	row			colFam		colQual				value
	[username]	user		description			[user description]
	[username]	user		followers			[user followers]
	[username]	user		location			[user location]
	[username]	user		name				[user name]
	[username]	tweet		[id]\text			[tweet text]
	[username]	tweet		[id]\tsource		[tweet source]
	[username]	tweet		[id]\tfavoriteCount	[favorite count]
	[username]	tweet		[id]\tlang			[tweet language]

#### TwitterSchema.TweetQuerySchema 
This inner class is used to read Key Value pairs from our primary table into Tweet objects our application can use.

#### TwitterUserQuerySchema.java
This is used to convert one row of key value pairs into a TwitterUser object.

#### TwitterSchema.Tweet 
This inner class defines a Tweet for the purposes of our application.

#### TwitterSchema.TwitterUser
This inner class defines a Twitter user for the purposes of our application, and contains a list of Tweet objects.

### TweetIndexer.java
This class takes a Twitter Status object from the Twitter API and produces index entries, which are of the form:

	row		colFam		colQual		value
	term	fieldName	recordID	[blank]
	
Where *recordID* is stored as the row of our primary table. This allows users to quickly locate all of the recordIDs that contain a given term.

This class also defines what our recordID will be for each TwitterUser object we store.


## <a name="twitterquery"></a> TwitterQuery.java
The TwitterQuery class demonstrates how a client might use a secondary index table in conjunction with a record table to satisfy a wider range of user queries.

In here, we create a *SchemaQueryClient* and configure it to query using our primary table, secondary index table, and query schema this:

	SchemaQueryClient<TwitterUser> client = new SchemaQueryClient<>(conn, "twitter", "twitterIndex", TwitterUserQuerySchema.getInstance());

We can then perform several types of lookups on the primary table. Since our primary table is defined has having Twitter usernames in the row, we can lookup TwitterUser objects by username by scanning the primary table directly:

	Iterable<TwitterUser> results = client.wildcardObjectLookup("jo", auths);
		
	for(TwitterUser user : results) {
		System.out.println(user);
	}

We can also do lookups on the index table which returns recordIDs representing each record matching our query. We can then fetch the full TwitterUser objects for each recordID from our primary table.

	results = client.indexLookup("today", "text", auths);
	for(TwitterUser user : results) {
		System.out.println(user);
	}