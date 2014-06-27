package org.apache.accumulo.examples.index;

import java.util.List;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

public class IndexingClient<T> {
	
	private final Indexer<T> indexedFields;
	private final Connector conn;
	private final String table;
	private boolean open = false;
	private BatchWriter writer;
	
	public IndexingClient(final Connector conn, 
			final String table, 
			final Indexer<T> indexedFields) {
		this.conn = conn;
		this.table = table;
		this.indexedFields = indexedFields;
	}
	
	public void open(final BatchWriterConfig config) throws TableNotFoundException {
		if(!open) {
			this.writer = conn.createBatchWriter(table, config);
			open = true;
		}
	}
	
	public void index(final T object) throws MutationsRejectedException {
		if(!open) 
			throw new IllegalStateException("must open() ingest client before calling ingest()");
		
		List<Mutation> indexEntries = indexedFields.apply(object);
		writer.addMutations(indexEntries);
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
