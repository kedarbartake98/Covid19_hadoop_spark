/*

Use MapReduce paradigm on pseudo-distributed hadoop to count the number of cases per million 
people country-wise. 

Input:

- CSV containing population details of each country
- CSV containing daily record of new cases and deaths country-wise

Demonstrates the use of 'setup' function in reduce operation.
Since, the population data is required by each reducer, it does not make sense
to store the population data on just one node and have the workers access it.
Therefore, we parse it into a HashMap and store it with each reduce worker.

*/

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.lang.*;
import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.text.SimpleDateFormat;
import java.text.ParseException;

public class Covid19_3 
{
	
	public static class Covid_Mapper extends Mapper<Object, Text, Text, LongWritable>
	{

		/*

		Map function that takes in line of csv as input, emits a key value pair 
		of (country_name, case_count) as output

		*/

		private Text country = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			try
			{
				String[] cols = value.toString().split(",");
				System.out.println("Cols :"+cols);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date date = new Date();
				date = sdf.parse(cols[0]);

				if ((date.getYear()+1900)==2020)
				{
					country.set(cols[1]);
					Long count = Long.parseLong(cols[2]);

					LongWritable case_count = new LongWritable(count);
					context.write(country, case_count);
				}

			}
			catch (ParseException e)
			{
				System.out.println("Error in parsing date");
			}		
		}
	}


	public static class Covid_Reducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
	{
		/*

		Here, the reducer has two components:

		- a 'setup' function
		Parses the population data and stores it in a HashMap for faster access
		and each worker gets a copy of that HashMap locally. 

		- a reduce function

		Implements Reduce operation to process the key value pairs emitted by 
		map, groups them by country and applies SUM to aggregate records 
		
		*/
		private DoubleWritable cases_per_million = new DoubleWritable();
		Map<String,Double> country_pop = new HashMap();

		public void setup(Context context) throws IOException, InterruptedException
		{
			System.out.println("Setup function called in Reducer");		
			String line = "";
			URI[] cached = context.getCacheFiles();

			if (cached!=null && cached.length>0)
			{
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path fpath = new Path(cached[0].toString());

				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fpath)));

				while ((line=br.readLine())!=null)
				{
					if (line.contains("\""))
					{
						line = line.replaceAll("\"([^\"]*)\"", "@@");
						System.out.println("Repalced line "+line);
					}
					
					String[] fields = line.toString().split(",");
					System.out.println("Line: \n"+line+"\n"+"Fields : \n"+fields[fields.length-1]);

					if (!fields[0].equals("countriesAndTerritories"))
					{
						try
						{
							double population = Double.parseDouble(fields[fields.length-1]);
							country_pop.put(fields[1], population);
						}
						catch (Exception e)
						{
							System.out.println("Error reading population");
						}
						
					}
				}

			}
		}
		// @Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum=0;

			for (LongWritable tmp: values)
			{
				sum+= tmp.get();
			}
			try
			{
				System.out.println("key "+key.toString());
				System.out.println("CountryMap:\n"+country_pop+"\n");
				String countryname = key.toString();
				double curr_pop = country_pop.get(countryname);
				System.out.println("Population fetched : "+curr_pop);
				cases_per_million.set((sum/curr_pop)*1000000);
				context.write(key, cases_per_million);
			}
			catch (Exception e)
			{
				System.out.println("Error in fetching population");
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration config = new Configuration();


		Job covid_job = Job.getInstance(config, "Covid cases per million");
		covid_job.addCacheFile(new Path(args[1]).toUri());

		covid_job.setJarByClass(Covid19_3.class);
		covid_job.setMapperClass(Covid_Mapper.class);
		covid_job.setReducerClass(Covid_Reducer.class);
		covid_job.setOutputKeyClass(Text.class);
		covid_job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(covid_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(covid_job, new Path(args[2]));
		System.exit(covid_job.waitForCompletion(true)? 0:1);	
	}

}
