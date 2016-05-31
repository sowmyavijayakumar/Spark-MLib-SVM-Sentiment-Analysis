import scala.Tuple2;
import scala.util.grammar.LabelledRHS;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.*;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;

import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.feature.IDF;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class SentimentAnalysis {
 public static void main(String[] args) {
		    SparkConf conf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local[4]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    String path = "C:\\Users\\Sowmya\\Desktop\\NUI GALWAY\\Tools and Techniques\\Assignment\\sentiment labelled sentences\\sentiment labelled sentences\\imdb_labelled.txt";
		    
		   JavaRDD<String> data =  sc.textFile("C:\\Users\\Sowmya\\Desktop\\NUI GALWAY\\Tools and Techniques\\Assignment\\sentiment labelled sentences\\sentiment labelled sentences\\imdb_labelled.txt"
		    ,1); 
           
		// Create a  HashingTF instance to map email text to vectors of 100 features.
		   final HashingTF tf =  new HashingTF(1000);
		   
		   JavaRDD<LabeledPoint> parsedData = data.map((String s)->{
		    	 double score= s.endsWith("0")?0:1;
		    	 
		    	 // return the label for each line along with a transform based on term frequency in the document which splits on spaces
		    	 return new LabeledPoint(score, tf.transform(Arrays.asList(s.substring(0,s.length()-1).split(" "))));
		     });
		   
		   
		  
// Split initial RDD into two... [60% training data, 40% testing data].
				   JavaRDD<LabeledPoint> training = parsedData.sample(false, 0.6, 11L	);
				   training.cache();
				   JavaRDD<LabeledPoint> testing = parsedData.subtract(training);
				   
// Run training algorithm to build the model.
		   
		
		   SVMWithSGD learner = new SVMWithSGD();
		   
		    final SVMModel model =   learner.run(training.rdd());

Vector example = tf.transform(Arrays.asList("And those owls were adorable.".split(" ") ) );    
Vector example1 = tf.transform(Arrays.asList("Today the graphics are crap.".split(" ") ) );    
	   
System.out.println("Prediction for example: " + model.predict(example));
System.out.println("Prediction for example: " + model.predict(example1));		   
 // Clear the default threshold.
		  model.clearThreshold();  
		    
 // Compute raw scores on the test set.
		   JavaRDD<Tuple2<Object, Object>> scoreAndLabels = testing.map(
				   new Function<LabeledPoint, Tuple2<Object, Object>>(){
		    public Tuple2<Object, Object> call(LabeledPoint p) {
		    Double score1 = model.predict(p.features());
		     return new Tuple2<Object, Object>(score1, p.label());
		        }
				   }  );
				   
				   System.out.println("Scoreandlabels:"+scoreAndLabels.collect());
		 
		

		   
// Get evaluation metrics.
BinaryClassificationMetrics metrics =
  new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
double auROC = metrics.areaUnderROC();
System.out.println("Area under ROC = " + auROC); 
}
}
		   
