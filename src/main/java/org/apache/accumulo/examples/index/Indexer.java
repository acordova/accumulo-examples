package org.apache.accumulo.examples.index;

import com.google.common.base.Function;
import java.util.List;
import org.apache.accumulo.core.data.Mutation;


public abstract class Indexer<T> implements Function<T,List<Mutation>> {
	
	public abstract String recordID(T t);
}
