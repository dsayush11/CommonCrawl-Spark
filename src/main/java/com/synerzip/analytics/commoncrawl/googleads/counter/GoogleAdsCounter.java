/**
 * Copyright (C) 2004-2014 Synerzip. 
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.synerzip.analytics.commoncrawl.googleads.counter;

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

import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapreduce.WARCInputFormat;
import com.synerzip.analytics.commoncrawl.googleads.parser.DefaultParser;
import com.synerzip.analytics.commoncrawl.googleads.parser.GoogleAdParser;


public class GoogleAdsCounter  implements Serializable {

	/**
	 * Serial Version UID
	 */
	private static final long serialVersionUID = -6609696095449768948L;

	private static final Logger LOG = Logger.getLogger(GoogleAdsCounter.class);	
	
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
		
		SparkConf sparkConf = new SparkConf().setAppName("GoogleAdsCounter").setMaster(master);	
	
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
				
				String adType = null;
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
							
							GoogleAdParser parser = new DefaultParser(element.data());

							String siteUrl = record._2().getRecord().getHeader().getTargetURI();
							String title = "Default"; // FIXME
							
							String adClient = parser.getAttribute("google_ad_client") != null ? parser
									.getAttribute("google_ad_client") : "NA";
							String adSlot = "default"; // FIXME
							String width = parser.getAttribute("google_ad_width") != null ? parser
									.getAttribute("google_ad_width") : "NA";
							String height = parser.getAttribute("google_ad_height") != null ? parser
									.getAttribute("google_ad_height") : "NA";
							adType = parser.getAttribute("google_ad_type") != null ? parser
									.getAttribute("google_ad_type") : "text";																					
						}
					}				
					return new Tuple2<String, Integer>(adType, 1);
				}				
				else					
					return new Tuple2<String, Integer>(adType, 1);
				
			}			
		}
		);	
		
		JavaPairRDD<String, Integer> adTypeCounts = warcRecords.reduceByKey(
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
		adTypeCounts.saveAsNewAPIHadoopFile(outputPath,org.apache.hadoop.io.Text.class, 
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

