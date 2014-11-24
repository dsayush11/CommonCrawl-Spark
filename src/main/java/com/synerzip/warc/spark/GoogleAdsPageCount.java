package com.synerzip.warc.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import scala.Tuple2;

//import com.synerzip.warcformatreader.WARCInputFormat;
//import com.synerzip.warcformatreader.WARCWritable;




import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCInputFormat;


public class GoogleAdsPageCount  implements Serializable {

	private static final Logger LOG = Logger.getLogger(GoogleAdsPageCount.class);	
	
	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_MASTER = "-master";
	private static final String ARGNAME_OVERWRITE = "-overwrite";
	private static final String ARGNAME_S3ACCESSKEY = "-accesskey";
	private static final String ARGNAME_S3SECRETKEY = "-secretkey";	
	
	public static void usage() {
		System.out.println("\n  com.synerzip.warc.spark.GoogleAdsPageCount \n"
					+ "                           "
					+ ARGNAME_INPATH
					+ " <inputpath>\n"
					+ "                           "
					+ ARGNAME_OUTPATH
					+ " <outputpath>\n"
					+ "                           "
					+ ARGNAME_MASTER
					+ " <master>\n"
					+ "                         [ "
					+ ARGNAME_S3ACCESSKEY + " <accesskey> ]\n"
					+ "                         [ "
					+ ARGNAME_S3SECRETKEY + " <secretkey> ]\n"
					+ "                         [ "
					+ ARGNAME_OVERWRITE
					+ " ]");
		System.out.println("");		
	}	
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException, Exception {
		
		
		String inputPath = null;
		String outputPath = null;
		String master = null;
		boolean overwrite = false;
		String s3AccessKey = null;
		String s3SecretKey = null;
		
		// Read the command line arguments.
		for (int i = 0; i < args.length; i++) {
			try {
				if (args[i].equals(ARGNAME_INPATH)) {
					inputPath = args[++i];
				} else if (args[i].equals(ARGNAME_OUTPATH)) {
					outputPath = args[++i];
				} else if (args[i].equals(ARGNAME_MASTER)) {
					master = args[++i];
				} else if (args[i].equals(ARGNAME_S3ACCESSKEY)) {
					s3AccessKey = args[++i];
				} else if (args[i].equals(ARGNAME_S3SECRETKEY)) {
					s3SecretKey = args[++i];
				} else if (args[i].equals(ARGNAME_OVERWRITE)) {
					 overwrite = true;
				}else {
					LOG.warn("Unsupported argument: " + args[i]);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				usage();
				throw new IllegalArgumentException();
			}
		}
		LOG.info(" inputPath :" + inputPath);
		if (inputPath == null || outputPath == null || master == null) {
			usage();
			throw new IllegalArgumentException();
		}	
		
		if(inputPath.contains("s3n") && (s3AccessKey == null || s3SecretKey == null) )
		{
			usage();
			LOG.info("Please specify Access Key and Secret Key to access data on AWS S3 storage ");
			throw new IllegalArgumentException();
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("GoogleAdsUsageCount").setMaster(master);	
	
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		if(inputPath.contains("s3n") && (s3AccessKey != null && s3SecretKey != null) )
		{
			conf.set("AWS_ACCESS_KEY_ID",s3AccessKey);
			conf.set("AWS_SECRET_ACCESS_KEY",s3SecretKey);
		}
		
		//define the accumulators to count total response pages and total Google Ad Pages
		final Accumulator<Integer> totalResponsePagesAccumulator = sc.accumulator(0);
		final Accumulator<Integer> totalGoogleAdPagesAccumulator = sc.accumulator(0);
		
		
		
		JavaPairRDD<LongWritable, WARCWritable> records = sc.newAPIHadoopFile(inputPath,
				WARCInputFormat.class,LongWritable.class, WARCWritable.class,job.getConfiguration());     
		
		
		JavaPairRDD<String, Integer> warcRecords = records.mapToPair(new PairFunction<Tuple2<LongWritable, WARCWritable>, String, Integer>(){
			
			
			public Tuple2<String, Integer> call(Tuple2<LongWritable, WARCWritable> record) throws Exception {
				
				String recordType = record._2().getRecord().getHeader().getRecordType();
				
				if(recordType.equals("response")){
					totalResponsePagesAccumulator.add(1); // total response pages
				
					String recordContent = new String(record._2().getRecord().getContent());
					
					// parse Html content of web page using Jsoup
					Document doc = Jsoup.parse(recordContent);	
					
					// Get the <script> tag elements 
					Elements scriptElements = doc.getElementsByTag("script");					
						
					for (Element element : scriptElements) {
						
						// if web page has google ads, then <script> tag contains "google_ad_client" 
						if (element.data().contains("google_ad_client")) {
							
							totalGoogleAdPagesAccumulator.add(1);
						}
					}					
				}					
				return new Tuple2<String, Integer>(recordType, 1);				
			}			
		}
		);	
		
		JavaPairRDD<String, Integer> recordTypeCounts = warcRecords.reduceByKey(
		  		new Function2<Integer, Integer, Integer>() {
			    		public Integer call(Integer i1, Integer i2) {
					      return i1 + i2;
				}
		}
		);
		
		// Delete the output path directory if it already exists and user wants
		// to overwrite it.
		if (overwrite) {
			LOG.info("clearing the output path at '" + outputPath + "'");
			FileSystem fs = FileSystem.get(new URI(outputPath), conf);
			if (fs.exists(new Path(outputPath))) {
				fs.delete(new Path(outputPath), true);
			}
		}
		
		long startTime = System.currentTimeMillis();
		
		//writing output to file
		recordTypeCounts.saveAsNewAPIHadoopFile(outputPath,org.apache.hadoop.io.Text.class, 
				org.apache.hadoop.io.Text.class, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		
		
		//print accumulator values		
		LOG.info(" totalResponsePagesAccumulator value : " + totalResponsePagesAccumulator.value());
		LOG.info(" totalGoogleAdPagesAccumulator value : " + totalGoogleAdPagesAccumulator.value());
		long endTime = System.currentTimeMillis();
		long difference = endTime - startTime;
		LOG.info("Elapsed milliseconds: " + difference);
				
		//stop spark context
		sc.stop();
	}
}
