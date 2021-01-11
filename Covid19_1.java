/*

Use MapReduce paradigm on pseudo-distributed hadoop to count number of cases 
per country until a certain date given the record of number of cases per day 
for each country in csv

The output contains cumulative records until a certain date. (2020-04-08)

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

public class Covid19_1 
{
	
	public static class Covid_Mapper extends Mapper<Object, Text, Text, LongWritable>
	{
		/*

		Map function that takes in line of csv as input, emits a key value pair 
		of (country_name, case_count) as output

		Filters the lines of the csv on basis of dates 
		(only gets records before a certain date) and whether or not the count 
		of cases for the entire world should be included in the final result. 

		*/

		private Text country = new Text();
		
		// @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] cols = value.toString().split(",");
			Configuration config = context.getConfiguration();
			String world = config.get("World");
			Boolean wld = Boolean.parseBoolean(world);

			// If user chose not to include records for entire world, ignore.
			if (!wld)
				{
					if (cols[1].equals("World"))
					{
						System.out.println("World detected when false");
						return;
					}
				}

			if (!cols[0].equals("date"))
			{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

				try
				{
					Date date1 = new Date();
					Date date = new Date();

					date1 = sdf.parse(cols[0]);
					date = sdf.parse("2020-04-08");

					if ((date1.getYear()+1900==2020) && (date1.before(date)))
					{


						country.set(cols[1]);

						Long count = Long.parseLong(cols[2]);

						LongWritable case_count = new LongWritable(count);
						// Emit key value pair
						context.write(country, case_count);
					}
				}

				catch (ParseException e)
				{
					System.out.println("Date Parse exception!!");
				}
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
		// User chooses to include records for total case count in the world
		config.set("World", args[1]);

		// Set mapper and reducer
		Job covid_job = Job.getInstance(config, "Covid Num Cases by Country");
		covid_job.setJarByClass(Covid19_1.class);
		covid_job.setMapperClass(Covid_Mapper.class);
		covid_job.setReducerClass(Covid_Reducer.class);

		covid_job.setOutputKeyClass(Text.class);
		covid_job.setOutputValueClass(LongWritable.class);

		// CSV file and output file path
		FileInputFormat.addInputPath(covid_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(covid_job, new Path(args[2]));
		System.exit(covid_job.waitForCompletion(true)? 0:1);	
	}

}
