package org.apache.accumulo.examples.index;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Lists.newArrayList;

import java.util.Map.Entry;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class IndexQueryClient {
	
	private final Connector conn;
	private final String table;
	private final Authorizations authorizations;
	
	public IndexQueryClient(
			final Connector conn, 
			final String table,
			final Authorizations auths) {
		this.conn = conn;
		this.table = table;
		this.authorizations = auths;
	}
	
	public Iterable<String> recordIDsForTerm(String term) throws TableNotFoundException {
		Scanner scanner = conn.createScanner(table, authorizations);
		
		return Iterables.transform(scanner, new Function<Entry<Key,Value>,String>() {

			@Override
			public String apply(Entry<Key, Value> kv) {
				return kv.getKey().getColumnQualifier().toString();
			}
		});
	}
	
	public BatchScanner recordsScanner(
			final String recordTable, 
			final Iterable<String> recordIDs) throws TableNotFoundException {
		
		BatchScanner bscanner = conn.createBatchScanner(recordTable, authorizations, 10);
		
		bscanner.setRanges(
				newArrayList(
					Iterables.transform(
						recordIDs, 
						new Function<String,Range>() {
							@Override
							public Range apply(String rowID) {
								return Range.exact(rowID);
							}		
						})));
		
		return bscanner;
	}
}
