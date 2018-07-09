package it.polimi.spark.streaming.example.functions;

import java.util.*;

import org.apache.spark.api.java.function.Function2;

import it.polimi.spark.streaming.example.datatypes.WordCount;
import scala.Tuple2;

public class WindowBuilder implements Function2<List<WordCount>, List<WordCount>, List<WordCount>>{

	@Override
	public List<WordCount> call(List<WordCount> v1, List<WordCount> v2)
			throws Exception {
        Set<WordCount> set = new HashSet<WordCount>();

        set.addAll(v1);
        set.addAll(v2);

        return new ArrayList<WordCount>(set);	
	}

}
