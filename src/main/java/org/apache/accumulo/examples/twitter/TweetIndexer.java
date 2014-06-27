package org.apache.accumulo.examples.twitter;

import static com.google.common.collect.Sets.newHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.examples.index.Indexer;
import twitter4j.Status;

public class TweetIndexer extends Indexer<Status> {

	@Override
	public List<Mutation> apply(Status status) {
		
		ArrayList<Mutation> indexEntries = new ArrayList<>();
		
		String rid = recordID(status);
		
		// choose what to index
		// for this example, we'll only index user locations, lang, 
		// words in the description, and words in the text

		for(String word : tokenize(status.getUser().getDescription())) {
			Mutation m = new Mutation(word);
			m.put("description", rid, "");
			indexEntries.add(m);
		}
		
		Mutation lang = new Mutation(status.getUser().getLang());
		lang.put("lang", rid, "");
		indexEntries.add(lang);
		
		Mutation location = new Mutation(status.getUser().getLocation());
		location.put("location", rid, "");
		indexEntries.add(location);
		
		for(String word : tokenize(status.getText())) {
			Mutation m = new Mutation(word);
			m.put("text", rid, "");
			indexEntries.add(m);
		}
		
		return indexEntries;
	}

	@Override
	public String recordID(Status status) {
		return status.getUser().getScreenName();
	}
	
	private Set<String> tokenize(String text) {
		if(text != null)
			return newHashSet(text.toLowerCase().split("\\s+"));
		return Collections.EMPTY_SET;
	}
}
