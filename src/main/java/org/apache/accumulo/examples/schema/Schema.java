package org.apache.accumulo.examples.schema;


public interface Schema {
	
	QuerySchema getQuerySchema();
	
	IngestSchema getIngestSchema();
}
