package it.polimi.spark.streaming.example.functions;

import org.apache.spark.api.java.function.MapFunction;

import it.polimi.spark.streaming.example.datatypes.WordCount;
import scala.Tuple2;

public class TestMapFunction implements MapFunction<Tuple2<String, WordCount>, WordCount>{

	private static final long serialVersionUID = 1L;

	@Override
	public WordCount call(Tuple2<String, WordCount> value) throws Exception {
		// TODO Auto-generated method stub
		return value._2();
	}

}
