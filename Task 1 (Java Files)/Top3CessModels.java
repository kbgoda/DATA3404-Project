import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class Top3CessModels {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String outputFileName = "ct1_test3_cluster_massive.txt";
		
//		 String aircraftsDir =
//		 "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_aircrafts.csv";
//		 String flightsDir =
//		 "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_flights_medium.csv";
//		 String outputDir = "/home/bmar9979/data3404_testOutputFiles/task1/" +
//		 outputFileName;
//		 DataSet<Tuple1<String>> flights =
//		 env.readCsvFile(flightsDir).includeFields("00000010000").ignoreFirstLine().ignoreInvalidLines().types(String.class);
//		 DataSet<Tuple3<String, String, String>> aircrafts =
//		 env.readCsvFile(aircraftsDir).includeFields("101010000").ignoreFirstLine().ignoreInvalidLines().types(String.class,
//		 String.class, String.class);

		DataSet<Tuple1<String>> flights = env
				.readCsvFile(
						"hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_massive.csv")
				.includeFields("00000010000").ignoreFirstLine().ignoreInvalidLines().types(String.class);
		DataSet<Tuple3<String, String, String>> aircrafts = env
				.readCsvFile(
						"hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_aircrafts.csv")
				.includeFields("101010000").ignoreFirstLine().ignoreInvalidLines()
				.types(String.class, String.class, String.class);

		String outputDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/bmar9979/task1/" + outputFileName;

		DataSet<Tuple3<String, String, String>> filtaircrafts = aircrafts
				.filter(new FilterFunction<Tuple3<String, String, String>>() {

					public boolean filter(Tuple3<String, String, String> t) {
						return t.f1.contains("CESSNA");
					}
				});

		DataSet<Tuple1<String>> joined_results = flights.join(filtaircrafts).where(0).equalTo(0).projectSecond(2);

		DataSet<Tuple2<String, Integer>> total_results = joined_results.flatMap(new AircraftMapping()).groupBy(0).sum(1)
				.sortPartition(1, Order.DESCENDING).setParallelism(1).first(3);

		total_results.writeAsFormattedText(outputDir, WriteMode.OVERWRITE,
				new TextFormatter<Tuple2<String, Integer>>() {
					public String format(Tuple2<String, Integer> t) {
						return "Cessna " + t.f0 + " \t " + t.f1;

					}
				});

		env.execute("Task 1 executed.");
		long end = System.currentTimeMillis();
		Thread.sleep(20000);
		System.out.println((end - start) + " ms");

	}

	@SuppressWarnings("serial")
	private static class AircraftMapping implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {
		@Override
		public void flatMap(Tuple1<String> input, Collector<Tuple2<String, Integer>> output) {
			output.collect(new Tuple2<String, Integer>(input.f0.substring(0, 3), 1));
		}
	}

}