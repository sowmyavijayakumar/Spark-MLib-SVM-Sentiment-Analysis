
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.parse.HiveParser.unionType_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.UnionRDD;

import scala.Tuple2;

public class SparkWeatherStation implements Serializable{

	String location;
	 JavaRDD<Measurement> measurements;
	static List<SparkWeatherStation> stations = new ArrayList<SparkWeatherStation>();
	static JavaSparkContext ctx;
	
	public SparkWeatherStation(String name, JavaRDD<Measurement> mts ) {
		location = name;
		measurements = mts;
		}

	
	public static int  countTemperatures(double t){
		
		
		JavaRDD<Measurement> measure = ctx.emptyRDD();
		//for(Iterator<SparkWeatherStation> i =  stations.iterator(); i.hasNext();){
		for (SparkWeatherStation i : stations) {
		 measure = measure.union(i.measurements);
			
		}
		
		JavaPairRDD<Double,Integer> temperature  = measure.filter(f -> f.temp >=t-1 && f.temp<=t+1).mapToPair(f -> new Tuple2(0.0,1));
		List<Tuple2<Double, Integer>> counts = temperature.reduceByKey((x,y) ->x+y).collect();
		System.out.println(counts);
		return counts.get(0)._2;
		}
		
	    
	   
	    
	  

	      
	
	public static void main(String args[]) {
		// creating the galway station information
		

		SparkConf sparkConf = new SparkConf().setAppName("JavaTempCount").setMaster("local[4]");
	     ctx = new JavaSparkContext(sparkConf);
	    
	    
	    
		ArrayList<Measurement> galwayMeter = new ArrayList<Measurement>();
		galwayMeter.add(new Measurement(10, 8));
		galwayMeter.add(new Measurement(11, 7));
		galwayMeter.add(new Measurement(12, 11));
		galwayMeter.add(new Measurement(13, 10));
		galwayMeter.add(new Measurement(14, 9));
		galwayMeter.add(new Measurement(15, 8));
		galwayMeter.add(new Measurement(16, 7));
		galwayMeter.add(new Measurement(17, 6));
		galwayMeter.add(new Measurement(18, 5));
		JavaRDD<Measurement> galwayRDD= ctx.emptyRDD(); 
		galwayRDD = ctx.parallelize(galwayMeter);
				SparkWeatherStation galwayW	= new SparkWeatherStation("Galway", galwayRDD);
		// add the station
		stations.add(galwayW);
		// creating the dublin station information
		ArrayList<Measurement> dublinMeter = new ArrayList<Measurement>();
		dublinMeter.add(new Measurement(10, 10));
		dublinMeter.add(new Measurement(11, 12));
		dublinMeter.add(new Measurement(12, 16));
		dublinMeter.add(new Measurement(13, 15));
		dublinMeter.add(new Measurement(14, 13));
		dublinMeter.add(new Measurement(15, 15));
		dublinMeter.add(new Measurement(16, 10));
		dublinMeter.add(new Measurement(17, 9));
		dublinMeter.add(new Measurement(18, 9));
		JavaRDD<Measurement> dublinRDD= ctx.emptyRDD(); 
		dublinRDD = ctx.parallelize(dublinMeter);
		
		SparkWeatherStation dublinW = new SparkWeatherStation("Dublin", dublinRDD);
		// add the station
		stations.add(dublinW);
		// creating the dublin station information
		ArrayList<Measurement> corkMeter = new ArrayList<Measurement>();
		corkMeter.add(new Measurement(10, 10));
		corkMeter.add(new Measurement(11, 12));
		corkMeter.add(new Measurement(12, 16));
		corkMeter.add(new Measurement(13, 15));
		corkMeter.add(new Measurement(14, 13));
		corkMeter.add(new Measurement(15, 11));
		corkMeter.add(new Measurement(16, 15));
		corkMeter.add(new Measurement(17, 9));
		corkMeter.add(new Measurement(18, 9));
		JavaRDD<Measurement> corkRDD= ctx.emptyRDD(); 
		corkRDD = ctx.parallelize(corkMeter);
		
		SparkWeatherStation corkW = new SparkWeatherStation("Cork", corkRDD);
		// add the station
		stations.add(corkW);
		   
		System.out.println(countTemperatures(15));
       
		
		
	     
	}
}

