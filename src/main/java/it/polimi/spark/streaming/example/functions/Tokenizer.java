package it.polimi.spark.streaming.example.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import it.polimi.spark.streaming.example.datatypes.WordCount;


public class Tokenizer implements FlatMapFunction<String, WordCount>{

	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<WordCount> call(String value) throws Exception {		
		List<WordCount> out = new ArrayList<WordCount>();
		
		/***** SPARK STYLE USER INPUT*****/
        for(String x: value.split(" ")) {
        	out.add(new WordCount(x, 1));
        }
        /*********************************/
        
        return out.iterator();
        
		/***** FLINK STYLE USER INPUT*****/
/*        for(String x: value.split(" ")) {
        	out.collect(new WordCount(x, 1));
        }*/
        /*********************************/

	}

}
