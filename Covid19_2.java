/*

Use MapReduce paradigm on pseudo-distributed hadoop to count number of deaths 
per country within a date range given the record of number of new deaths per day 
for each country in csv file

*/
import java.io.IOException;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class Covid19_2 
{
	
	public static class Covid_Mapper extends Mapper<Object, Text, Text, LongWritable>
	{
		/*
		Map function that takes in line of csv as input, emits a key value pair 
		of (country_name, num deaths) as output

		Filters the lines of the csv on basis of dates 
		(only gets records within a certain date range) 
		*/
		private Text country = new Text();
		
		// @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split(",");
			Configuration config = context.getConfiguration();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date start = new Date();
			Date end = new Date();	
			Date curr = new Date();

			try
			{
				if (!cols[0].equals("date"))
					{
						start = sdf.parse(config.get("Start"));
						end = sdf.parse(config.get("End"));
						curr = sdf.parse(cols[0]);

						// Dates which are in the start and end range inclusive
						if ((curr.compareTo(start)>=0) && (curr.compareTo(end)<=0))
						{
							Long num_deaths = Long.parseLong(cols[3]);
							country.set(cols[1]);
							LongWritable death_count = new LongWritable(num_deaths);
							context.write(country, death_count);
						}
					}
			}	
			catch (ParseException e)
			{
				System.out.println("Date Parsing failed! Wrong date format specified in start and/or end dates");
				System.exit(1);
			}	
		}
	}


	public static class Covid_Reducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		/*

		Implements Reduce operation to process the key value pairs emitted by 
		map, groups them by country and applies SUM to aggregate records 

		*/
		private LongWritable total_cases = new LongWritable();

		// @Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum=0;

			for (LongWritable tmp: values)
			{
				sum+= tmp.get();
			}

			total_cases.set(sum);
			context.write(key, total_cases);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration config = new Configuration();

		Date start = new Date();
		Date end = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		// for detecting wrong date formats
		start = sdf.parse(args[1]);
		end = sdf.parse(args[2]);

		System.out.println("Start "+args[1]+" "+args[2]);

		config.set("Start", args[1]);
		config.set("End", args[2]);

		Job covid_job = Job.getInstance(config, "Covid Num Deaths by Country");
		covid_job.setJarByClass(Covid19_2.class);
		covid_job.setMapperClass(Covid_Mapper.class);
		covid_job.setReducerClass(Covid_Reducer.class);
		covid_job.setOutputKeyClass(Text.class);
		covid_job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(covid_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(covid_job, new Path(args[3]));
		System.exit(covid_job.waitForCompletion(true)? 0:1);	
	}

}
