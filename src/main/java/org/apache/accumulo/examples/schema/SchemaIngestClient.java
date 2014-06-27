package org.apache.accumulo.examples.schema;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;


public class SchemaIngestClient<T> {
	
	private final String table;
	private final Connector conn;
	private final Function<T, Mutation> schema;
	private boolean open = false;
	private BatchWriter writer;
	
	public SchemaIngestClient(
			final Connector conn, 
			final String table, 
			final Function<T, Mutation> schema) {
		this.conn = conn;
		this.table = table;
		this.schema = schema;
	}
	
	public void open(final BatchWriterConfig config) throws TableNotFoundException {
		if(!open) {
			this.writer = conn.createBatchWriter(table, config);
			open = true;
		}
	}
	
	public void ingest(final T object) throws MutationsRejectedException {
		if(!open) 
			throw new IllegalStateException("must open() ingest client before calling ingest()");
		
		Mutation m = schema.apply(object);
		writer.addMutation(m);
	}
	
	public void flush() throws MutationsRejectedException {
		writer.flush();
	}
	
	public void close() throws MutationsRejectedException {
		
		MutationsRejectedException mrex = null;
				
		if(open) {
			try {
				writer.close();
			} catch (MutationsRejectedException ex) {
				mrex = ex;
			} finally {
				open = false;
			}
			
			// allow caller to handle rejected mutations
			if(mrex != null)
				throw mrex;
		}
	}
}
