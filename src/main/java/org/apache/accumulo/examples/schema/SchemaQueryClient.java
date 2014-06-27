package org.apache.accumulo.examples.schema;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;


public class SchemaQueryClient<T> {
	private final QuerySchema<T> schema;
	private final Connector conn;
	private final String table;
	private final String indexTable;
	private final RowUnpacker rowUnpacker = new RowUnpacker();
	
	
	public SchemaQueryClient(
			final Connector conn, 
			final String table, 
			final String indexTable,
			final QuerySchema<T> schema) {
		
		this.conn = conn;
		this.table = table;
		this.indexTable = indexTable;
		this.schema = schema;
	}
	
	public Optional<String> singleValueLookup(
			final String row, 
			final String family,
			final String qualifier,
			final String[] auths) throws TableNotFoundException {
		
		Scanner scanner = conn.createScanner(table, new Authorizations(auths));
		
		Key key = new Key(row, family, qualifier);
		
		scanner.setRange(new Range(key, key));
		Iterator<Entry<Key,Value>> iter = scanner.iterator();
		
		if(!iter.hasNext())
			return Optional.absent();
		
		return Optional.of(new String(iter.next().getValue().get()));
	}
	
	public Optional<T> exactObjectLookup(
			final String term, 
			final String[] authorizations) throws TableNotFoundException, IOException {
		
		Range range = new Range(term);
		Authorizations auths = new Authorizations(authorizations);
		
		Scanner scanner = conn.createScanner(table, auths);
		
		IteratorSetting its = new IteratorSetting(10, "wri", WholeRowIterator.class);
		scanner.addScanIterator(its);
		
		scanner.setRange(range);
		
		Iterator<Entry<Key,Value>> iter = scanner.iterator();
		if(!iter.hasNext()) {
			scanner.close();
			return Optional.absent();
		}
		
		Entry<Key, Value> packedRow = iter.next();
		scanner.close();
		
		// unpack row
		SortedMap<Key, Value> row = WholeRowIterator.decodeRow(packedRow.getKey(), packedRow.getValue());
		
		// deserialize
		return Optional.of(schema.apply(row));
	}
	
	public Iterable<T> wildcardObjectLookup(
			final String term, 
			final String[] authorizations) throws TableNotFoundException {
		
		Range range = Range.prefix(term);
		Authorizations auths = new Authorizations(authorizations);
		
		Scanner scanner = conn.createScanner(table, auths);
		
		IteratorSetting its = new IteratorSetting(10, "wri", WholeRowIterator.class);
		scanner.addScanIterator(its);
		
		scanner.setRange(range);
		
		Iterable<Map<Key, Value>> unpackedRows = Iterables.transform(scanner, rowUnpacker);
		
		return Iterables.transform(unpackedRows, schema);
	}
	
	/**
	 * Lookup all records containing the given term in the given field.
	 * 
	 * The term may be just one word out of many in the field.
	 * 
	 * @param term
	 * @param field
	 * @param authorizations
	 * @return 
	 */
	public Iterable<T> indexLookup(
			final String term,
			final String field,
			final String[] authorizations) throws TableNotFoundException {
		
		Range range = Range.exact(term, field);
		Authorizations auths = new Authorizations(authorizations);
		
		// first lookup record IDs
		Scanner scanner = conn.createScanner(indexTable, auths);
		scanner.setRange(range);
		
		List<Range> recordIDRanges = new ArrayList<>();
		for(Entry<Key, Value> entry : scanner)
			recordIDRanges.add(Range.exact(entry.getKey().getColumnQualifier().toString()));
		
		if(recordIDRanges.isEmpty()) // no results
			return Collections.EMPTY_SET;
		
		// lookup objects
		BatchScanner bscanner = conn.createBatchScanner(table, auths, 10);
		
		IteratorSetting its = new IteratorSetting(10, "wri", WholeRowIterator.class);
		bscanner.addScanIterator(its);
		
		bscanner.setRanges(recordIDRanges);
		
		Iterable<Map<Key, Value>> unpackedRows = Iterables.transform(bscanner, rowUnpacker);
		
		return Iterables.transform(unpackedRows, schema);
	}
	
	private static class RowUnpacker implements Function<Entry<Key,Value>, Map<Key,Value>> {

		@Override
		public Map<Key, Value> apply(Entry<Key, Value> packedRow) {
			try {
				return WholeRowIterator.decodeRow(packedRow.getKey(), packedRow.getValue());
			} catch (IOException ex) {
				Logger.getLogger(SchemaQueryClient.class.getName()).log(Level.SEVERE, null, ex);
				return Collections.EMPTY_MAP;
			}
		}
	}
}
