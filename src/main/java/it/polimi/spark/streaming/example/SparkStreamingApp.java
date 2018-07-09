package it.polimi.spark.streaming.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import it.polimi.spark.streaming.example.datatypes.WordCount;
import it.polimi.spark.streaming.example.functions.TestWindowFunction;
import it.polimi.spark.streaming.example.functions.Tokenizer;
import it.polimi.spark.streaming.example.functions.WindowBuilder;
import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class SparkStreamingApp {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(100));

		jssc.checkpoint("~/Desktop/checkpoint");

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999,
				StorageLevels.MEMORY_AND_DISK_SER);

		// JavaDStream<String> lines =
		// jssc.textFileStream("/home/utente/eclipse-workspace/UML2JavaFlink/tmp");

		JavaDStream<WordCount> tokens = lines.flatMap(new Tokenizer());

		/*
		 * JavaDStream<WordCount> wordCounts = tokens.reduceByWindow(((i1, i2) ->
		 * {i1.setCount(i1.getCount() + i2.getCount()); return i1;} ),
		 * Durations.seconds(10), Durations.seconds(10));
		 */

		/********** SPARK EQUIVALENT OF KEYBEY (?) **********/
		/*
		 * JavaPairDStream<String, WordCount> keyedTokens =
		 * tokens.repartition(2).mapToPair((x -> new Tuple2<>(x.getWord(), x)));
		 * 
		 * 
		 * // RUNNING SUM Function2<List<WordCount>, Optional<WordCount>,
		 * Optional<WordCount>> updateFunction = new Function2<List<WordCount>,
		 * Optional<WordCount>, Optional<WordCount>>() { public Optional<WordCount>
		 * call(List<WordCount> values, Optional<WordCount> state) { WordCount updated=
		 * null; if(state.isPresent()) { updated = state.get(); if(values.size() > 0) {
		 * // System.out.println(values); for (WordCount i : values) {
		 * updated.setCount(updated.getCount() + i.getCount()); }
		 * 
		 * }
		 * 
		 * return Optional.of(updated); } else { if(values.size() > 0) { //
		 * System.out.println(values); updated = new WordCount(values.get(0).getWord(),
		 * 0); for (WordCount i : values) { updated.setCount(updated.getCount() +
		 * i.getCount()); } return Optional.of(updated); } else { return
		 * Optional.of(null); } }
		 * 
		 * } };
		 */

		// SECOND ARGUMENT IT THE PARALLELISM
		/*
		 * JavaDStream<WordCount> wordCounts =
		 * keyedTokens.updateStateByKey(updateFunction) // THE FOLLOWING IS A KIND OF
		 * UNKEYING // COULD BE AVOIDED, FOR INSTANCE IF THE NEXT STREAM NEEDS KEYING AS
		 * WELL // OR CAN BE SIMPLY DONE BY DEFAULT TO CLOSE A KEYED OPERATION .map(x ->
		 * x._2);
		 */
		// THE FOLLOWING IS FOR SUMMING ELEMENTS IN EACH MICROBACTH

		// SECOND ARGUMENT IT THE PARALLELISM
		/*
		 * JavaDStream<WordCount> wordCounts = keyedTokens .reduceByKey((i1, i2) -> new
		 * WordCount(i1.getWord(), i1.getCount() + i2.getCount()), 2)
		 * 
		 * // THE FOLLOWING IS A KIND OF UNKEYING // COULD BE AVOIDED, FOR INSTANCE IF
		 * THE NEXT STREAM NEEDS KEYING AS WELL // OR CAN BE SIMPLY DONE BY DEFAULT TO
		 * CLOSE A KEYED OPERATION .repartition(2) .map(x -> x._2);
		 */

		/*************
		 * "TUBMLING" WINDOWED CASE (the slide length is equal to the window length)
		 ***************/
		// SECOND ARGUMENT IT THE PARALLELISM

		/*
		 * JavaDStream<WordCount> wordCountsWindowed =
		 * keyedTokens.reduceByKeyAndWindow(((i1, i2) -> new WordCount(i1.getWord(),
		 * i1.getCount() + i2.getCount())), Durations.seconds(10),
		 * Durations.seconds(10), 2) // THE FOLLOWING IS A KIND OF UNKEYING COULD BE //
		 * AVOIDED, FOR INSTANCE IF THE NEXT STREAM NEEDS KEYING AS WELL OR CAN BE //
		 * SIMPLY DONE BY DEFAULT TO CLOSE A KEYED OPERATION .map(x -> x._2);
		 */

		/******************************************/

		// JavaPairDStream<String, WordCount> counts = tokens.mapToPair((x -> new
		// Tuple2<>(x.getWord(), x))).reduceByKeyAndWindow(new TestWindowFunction(),
		// Durations.seconds(5),
		// Durations.seconds(5));

/*		JavaDStream<WordCount> counts = tokens.mapToPair((x -> new Tuple2<>(x.getWord(), Arrays.asList(x))))
				.reduceByKeyAndWindow(new Function2<List<WordCount>, List<WordCount>, List<WordCount>>() {

					@Override
					public List<WordCount> call(List<WordCount> v1, List<WordCount> v2) throws Exception {
						Set<WordCount> set = new HashSet<WordCount>();

						set.addAll(v1);
						set.addAll(v2);

						return new ArrayList<WordCount>(set);
					}

				}, Durations.seconds(5), Durations.seconds(5)).flatMap(new TestWindowFunction());*/
		
		JavaDStream<WordCount> counts = tokens.mapToPair((x -> new Tuple2<>(x.getWord(), Arrays.asList(x))))
				.reduceByKeyAndWindow(new Function2<List<WordCount>, List<WordCount>, List<WordCount>>() {
					@Override 
					public List<WordCount> call(List<WordCount> v1, List<WordCount> v2) throws Exception {
return null;
					}

				}, Durations.seconds(5), Durations.seconds(5), 4).flatMap(new TestWindowFunction());
		
		counts.print();

		// tokens.print();
		// Print the first ten elements of each RDD generated in this DStream to the
		// console
		// wordCounts.dstream().saveAsTextFiles("/home/utente/eclipse-workspace/UML2JavaFlink/wordCountx",
		// "txt");
		// wordCounts.print();
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

}
