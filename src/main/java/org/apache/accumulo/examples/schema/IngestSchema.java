package org.apache.accumulo.examples.schema;

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Mutation;

public abstract class IngestSchema<T> implements Function<T,Mutation> {

}
