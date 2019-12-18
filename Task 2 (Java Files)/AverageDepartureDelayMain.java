import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
//import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.Scanner;
import java.io.File;

//https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/batch/index.html
//https://data-flair.training/blogs/apache-flink-application/
// Author: Karan Goda

public class AverageDepartureDelayMain {
	@SuppressWarnings("unused")
	private static Scanner input;
	private static String targetYear;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		targetYear = "2007";
		// default year, add a system input scanner to get year input by user
		input = new Scanner(System.in);
		System.out.println(
				"What year would you like to obtain the avg, min and max delays of the United States airlines?");
		 targetYear = input.nextLine(); // comment out this line when working
		// with random for test
		
		// set default year from 1995 to 2008 at random
		// default output file name
		String outputFileName = "ct2_test3_cluster_massive.txt";

		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String airlinesFilePath = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv";
		String flightsFilePath = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_massive.csv";
		String outputDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/bmar9979/task2/" + outputFileName;

		/// String airlinesFilePath =
		/// "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_airlines.csv";
		/// String flightsFilePath =
		 ///"/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_flights_massive.csv";
		 ///String outputDir = "/home/bmar9979/data3404_testOutputFiles/task2/" + outputFileName;
		// SELECT clause
		// filter columns first
		// Obtain a data set from the flights tiny file, include the required
		// fields for
		// the task, in this case carrier code, flight date, scheduled
		// department time
		// and actual department time

		// retrieve flight data from file:
		// <airline_code, airline_name, airline_country>
		DataSet<Tuple3<String, String, String>> airlines = env.readCsvFile(airlinesFilePath).includeFields("111")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);

		// retrieve airports data from file:
		// <F.carrier_code, F.flight_date, F.scheduled_departure_time,
		// F.actual_departure_time, F.actual_arrival_time>
		DataSet<Tuple5<String, String, String, String, String>> flights = env.readCsvFile(flightsFilePath)
				.includeFields("010100010110").ignoreFirstLine().ignoreInvalidLines()
				.types(String.class, String.class, String.class, String.class, String.class);
		// System.out.println(flights.count()); // Good 803668

		// WHERE clause
		// Reduce by filtering here
		// get all US airlines: <airline_code, airline_name>
		DataSet<Tuple2<String, String>> usAirlines = airlines.reduceGroup(new usAirlinesReducer());
		// System.out.println(usAirline.count()); // Good 322

		// get all flight delays: <airline_code, delay>
		DataSet<Tuple2<String, Double>> flightDelays = flights.reduceGroup(new yearAndDelayReducer(targetYear));
		// System.out.println(flightDelays.count()); // Good 36058

		// JOIN clause
		// Join the result from "usAirlines" and "flightDelays"
		// To get airline flight delays: <airline_code, airline_name, delay in
		// minutes>
		DataSet<Tuple3<String, String, Double>> airlineFlightDelays = usAirlines.join(flightDelays).where(0).equalTo(0)
				.with(new JoinAirLinesFlights());

		// RESULT
		// Group by airline name
		// <airline name, total delays, average delay in minutes, min delay, max
		// delay>
		DataSet<Tuple5<String, Integer, Double, Double, Double>> result = airlineFlightDelays.groupBy(1)
				.reduceGroup(new avgDelay()).sortPartition(0, Order.ASCENDING).setParallelism(1);
		// result.print();
		// store in Hadoop cluster or local directory
		result.writeAsFormattedText(outputDir, WriteMode.OVERWRITE,
				new TextFormatter<Tuple5<String, Integer, Double, Double, Double>>() {
					public String format(Tuple5<String, Integer, Double, Double, Double> t) {
						return t.f0 + "\t" + t.f1 + "\t" + t.f2 + "\t" + t.f3 + "\t" + t.f4;
					}
				});
		//

		env.execute("Task 2 executed.");
		long end = System.currentTimeMillis();
		
		System.out.println((end - start) + " ms");
		// System.out.println(result.count());

		// save to local
		// storeResults(result, outputFileName);
		Thread.sleep(20000);
	}

	// private static class AircraftMapping implements
	// FlatMapFunction<Tuple3<String, String, Double>, Tuple5<String, Integer,
	// Double, Double, Double>> {
	// @Override
	// public void flatMap(Tuple3<String, String, Double> input,
	// Collector<Tuple5<String, Integer, Double, Double, Double>> output) {
	// output.collect(new Tuple5<String, Integer, Double, Double,
	// Double>(input.f1, 1, input.f2, 1.0, 1.0));
	// }
	// }

	@SuppressWarnings("serial")
	private static class JoinAirLinesFlights
			implements JoinFunction<Tuple2<String, String>, Tuple2<String, Double>, Tuple3<String, String, Double>> {
		@Override
		public Tuple3<String, String, Double> join(Tuple2<String, String> airline, Tuple2<String, Double> flight) {
			return new Tuple3<String, String, Double>(airline.f0, airline.f1, flight.f1);
		}
	}

	@SuppressWarnings("serial")
	public static class usAirlinesReducer
			implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>> {
		@Override
		public void reduce(Iterable<Tuple3<String, String, String>> records, Collector<Tuple2<String, String>> out)
				throws Exception {
			for (Tuple3<String, String, String> airline : records) {
				if (!airline.f2.equals("United States"))
					continue;
				out.collect(new Tuple2<String, String>(airline.f0, airline.f1));
			}
		}
	}

	@SuppressWarnings("serial")
	public static class yearAndDelayReducer
			implements GroupReduceFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Double>> {
		// target year
		private String year;

		// constructor
		yearAndDelayReducer(String year) {
			this.year = year;
		}

		// Returns results as carrier_code, delay
		@Override
		public void reduce(Iterable<Tuple5<String, String, String, String, String>> records,
				Collector<Tuple2<String, Double>> out) throws Exception {
			for (Tuple5<String, String, String, String, String> flight : records) {
				if (!flight.f1.contains(year))
					continue;
				if (flight.f2.equals("") || flight.f3.equals("") || flight.f4.equals(""))
					continue;

				DateFormat formatter = new SimpleDateFormat("HH:mm:ss");

				Date scheduledDepart = (Date) formatter.parse(flight.f2);
				Date actualDepart = (Date) formatter.parse(flight.f3);

				double departDelay = actualDepart.getTime() - scheduledDepart.getTime();

				// Check that actual departure > scheduled departure
				if (departDelay <= 0)
					continue;

				Double delay = departDelay;
				out.collect(new Tuple2<String, Double>(flight.f0, delay));
			}
		}
	}

	// This function will group by airport
	// and store
	// [ airline_name \t num_delays \t average_delay \t min_delay \t max_delay ]
	@SuppressWarnings("serial")
	public static class avgDelay implements
			GroupReduceFunction<Tuple3<String, String, Double>, Tuple5<String, Integer, Double, Double, Double>> {
		@Override
		public void reduce(Iterable<Tuple3<String, String, Double>> records,
				Collector<Tuple5<String, Integer, Double, Double, Double>> out) throws Exception {
			String airport = null;
			double minDelay = 500000000;
			double maxDelay = 0;
			double totalDelay = 0;
			int totalFlightsByAirline = 0;

			for (Tuple3<String, String, Double> r : records) {
				airport = r.f1;
				totalDelay += r.f2;
				totalFlightsByAirline++;

				maxDelay = r.f2 > maxDelay ? r.f2 : maxDelay; // If max delay is
																// greater than
																// existing max,
																// replace
				minDelay = r.f2 < minDelay ? r.f2 : minDelay; // If min delay is
																// less than
																// existing min,
																// replace
			}

			// Divide the values by 60 seconds (60,000 milliseconds)
			double avgDelay = totalDelay / totalFlightsByAirline / 60000.0;
			maxDelay = maxDelay / 60000.0;
			minDelay = minDelay / 60000.0;

			out.collect(new Tuple5<String, Integer, Double, Double, Double>(airport, totalFlightsByAirline, avgDelay,
					minDelay, maxDelay));
		}
	}

	@SuppressWarnings("unused")
	private static void checkIfFileExists(String fpath) {
		File tempFile = new File(fpath);
		boolean exists = tempFile.exists();
		System.out.print(tempFile.getAbsolutePath());
		System.out.println(exists);
	}

}