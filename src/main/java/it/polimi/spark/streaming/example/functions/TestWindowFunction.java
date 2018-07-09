package it.polimi.spark.streaming.example.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import it.polimi.spark.streaming.example.datatypes.WordCount;
import scala.Tuple2;

public class TestWindowFunction implements FlatMapFunction<Tuple2<String, List<WordCount>>,WordCount>{

	private static final long serialVersionUID = 1L;
	List<String> supplierNames = Arrays.asList("sup1", "sup2", "sup3");
	
	@Override
	public Iterator<WordCount> call(Tuple2<String, List<WordCount>> t) throws Exception {
		int count = 0;

		List<WordCount> windowContent = t._2();
		String key = t._1;
		List<WordCount> out= new ArrayList<WordCount>();
		
		for(WordCount token: windowContent){
			count++;
		}

		out.add(new WordCount(key,count));
		
		return out.iterator();
	}
	
}
