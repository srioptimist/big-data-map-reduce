package com.cts.bigdata.hadoop.snanalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ProcessSocialNetworkData {
	// Mapper class
	public static class E_EMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			Text> /* Output value Type */
	{
		// Map function
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (value != null) {
				final String line = value.toString();
				final ArrayList<String> messageContentKeysList = ProcessSocialNetworkData.getMessageContentKeysList();
				final HashMap<String, String> inputOutputKeysMap = ProcessSocialNetworkData.getInputOutputKeysMapping();
				final Set<String> inputFilterKeyList = inputOutputKeysMap.keySet();

				for (String messageContentKey : messageContentKeysList) {
					boolean messagefound = false;
					if (line.toLowerCase().contains(messageContentKey.toLowerCase().trim())) {
						final int messageStartPostion = line.indexOf(":");
						if (messageStartPostion != -1) {
							// Get the value of
							String messageValue = line.substring(line.indexOf(":") + 1).trim();
							if (messageValue != null) {
								for (String inputFilterKey : inputFilterKeyList) {
									if (messageValue.toLowerCase().contains(inputFilterKey.toLowerCase())) {
										messagefound = true;
										final String outputFilterKey = inputOutputKeysMap.get(inputFilterKey);

										// Remove first double quotes
										if (messageValue.trim().startsWith("\"")) {
											messageValue = messageValue
													.substring(messageValue.trim().indexOf("\"") + 1);
										}

										// Remove last double quotes
										if (messageValue.trim().endsWith("\"") || messageValue.trim().endsWith("\",")) {
											messageValue = messageValue.substring(0,
													messageValue.trim().lastIndexOf("\""));
										}

										output.collect(new Text(outputFilterKey), new Text(messageValue));
										break;
									}
								}
							}
						}
					}
					if (messagefound) {
						messagefound = false;
						break;
					}
				}
			}

		}
	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		// Reduce function
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			final String inputSearchKey = key.toString();
			while (values.hasNext()) {
				final Text messageText = values.next();
				final String messageValue = messageText.toString();
				output.collect(new Text(inputSearchKey), new Text(messageValue));
			}
		}
	}

	// Main function
	public static void main(String args[]) throws Exception {
		JobConf conf = new JobConf(ProcessSocialNetworkData.class);

		conf.setJobName("max_eletricityunits");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(E_EMapper.class);
		conf.setCombinerClass(E_EReduce.class);
		conf.setReducerClass(E_EReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	public static ArrayList<String> getMessageContentKeysList() {
		ArrayList<String> keyList = new ArrayList<String>();
		keyList.add("\"message\"");
		keyList.add("\"comment\"");
		keyList.add("\"Text\"");
		return keyList;
	}

	public static HashMap<String, String> getInputOutputKeysMapping() {
		final HashMap<String, String> inputOutputKeysMap = new HashMap<String, String>();
		inputOutputKeysMap.put("Bored", "WorkLife Balance");
		inputOutputKeysMap.put("Opportunities", "Better oppurtunities");
		inputOutputKeysMap.put("Skills", "Better oppurtunities");
		inputOutputKeysMap.put("Abilities", "Better oppurtunities");
		inputOutputKeysMap.put("Contribution", "Better oppurtunities");
		inputOutputKeysMap.put("Independence", "Better oppurtunities");
		inputOutputKeysMap.put("Meaningfulness", "Better oppurtunities");
		inputOutputKeysMap.put("financial", "Monetary Benefits");
		inputOutputKeysMap.put("motivation", "Corporate Policy and Culture");
		inputOutputKeysMap.put("appreciation", "Corporate Policy and Culture");
		inputOutputKeysMap.put("recognition", "Corporate Policy and Culture");
		inputOutputKeysMap.put("culture", "Corporate Policy and Culture");
		inputOutputKeysMap.put("compensation", "Monetary Benefits");
		inputOutputKeysMap.put("benefits", "Monetary Benefits");
		inputOutputKeysMap.put("respect", "Corporate Policy and Culture");
		inputOutputKeysMap.put("satisfaction", "Monetary Benefits");
		inputOutputKeysMap.put("seek growth", "Career Growth");
		inputOutputKeysMap.put("career growth", "Career Growth");
		inputOutputKeysMap.put("Bad boss", "WorkLife Balance");
		return inputOutputKeysMap;
	}
}