package org.apache.accumulo.examples.schema;

import com.google.common.base.Function;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public abstract class QuerySchema<T> implements Function<Map<Key,Value>,T> {
	
}
