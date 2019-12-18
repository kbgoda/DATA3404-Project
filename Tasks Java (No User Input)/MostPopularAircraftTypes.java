import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.util.Scanner;

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
 
// Author: Karan Goda
public class MostPopularAircraftTypes {
    @SuppressWarnings("unused")
    private static Scanner input;
    private static String country;
 
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        //input = new Scanner(System.in);
        long start = System.currentTimeMillis();;
        System.out.println("Which country's top 5 used aircraft types for all airlines do you wish to obtain?");
//        country = input.nextLine();
        country = "United States";
   
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
   
        String outputFileName = "lt3_test3.txt";
       
        // load files from cluster
         String flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_large.csv";
         String airlineDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv";
         String aircraftDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_aircrafts.csv";
      // Default file path
         String outputDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/bmar9979/task3/" + outputFileName;
         
//        String flightDataDir = "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_flights_medium.csv";
//        String airlineDataDir = "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_airlines.csv";
//        String aircraftDataDir = "/home/bmar9979/Documents/data3404_files/Assignment/assignment_data_files/ontimeperformance_aircrafts.csv";
//        String outputDir = "/home/bmar9979/data3404_testOutputFiles/task3/" + outputFileName;
     
       
        // retrieve airline data from CSV
        // resulting in airline_code, airline_name, country
        DataSet<Tuple3<String, String, String>> airlines = env.readCsvFile(airlineDataDir).includeFields("111")
        .ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
 
        // retrieve flight data from CSV
        // resulting in airline_code, tail_number
        DataSet<Tuple2<String, String>> flights = env.readCsvFile(flightDataDir).includeFields("010000100000")
        .ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class);
 
        // retrieve aircraft data from CSV
        // resulting in tailnum, manufacturer, model
        DataSet<Tuple3<String, String, String>> aircraftDetails = env.readCsvFile(aircraftDataDir).includeFields("101010000")
        .ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
 
        // get all airlines associated with the country input,
        // resulting in airline_code, airline_name
        DataSet<Tuple2<String, String>> countryAirline = airlines.reduceGroup(new countryAirlineReducer(country));
 
        // join the result from "usAirline" and "flights"
        // resulting in airline_name, tail_number
        DataSet<Tuple2<String, String>> airlinesAndTailNumbers = countryAirline.join(flights).where(0)
        .equalTo(0)
        .with(new AirlineFlightJoin());
 
        // join the result from "airlineTailNumbers" and "aircraftDetails"
        // resulting in airline_name, tail_number, manufacturer, model
        DataSet<Tuple4<String, String, String, String>> airlinesAndAircraftDetails = airlinesAndTailNumbers
        .join(aircraftDetails).where(1)
        .equalTo(0)
        .with(new ATNumberAndAircraftJoin());
 
        // Filter the dataset in descending order of the most used tail numbers per airline
        // resulting in airline_name, tail_number, manufacturer, model, count
        DataSet<Tuple5<String, String, String, String, Integer>> aircraftUsedCount = airlinesAndAircraftDetails
        .groupBy(1)
        .reduceGroup(new TailnumberCounter())
        .sortPartition(0, Order.ASCENDING).setParallelism(1)
        .sortPartition(4, Order.DESCENDING);
 
        // Apply reduction so that only the 5 most used tailnumbers for each airline is
        // recorded
        // Creates new data set of Tuples with fields: <airline_name, ArrayList
        // <Tuple<manufacturer, model>>>
        DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>> aircraftUsedCountFive = aircraftUsedCount
        .reduceGroup(new FiveMostUsedReducer());
       
        aircraftUsedCountFive.print();
 
        // store in hadoop cluster
        aircraftUsedCountFive.writeAsFormattedText(outputDir, WriteMode.OVERWRITE,
                new TextFormatter<Tuple2<String, ArrayList<Tuple2<String, String>>>>() {
                    public String format(Tuple2<String, ArrayList<Tuple2<String, String>>> t) {
                        String outputString = "";
                        outputString += t.f0 + "\t[";
                        for (int j = 0; j < t.f1.size(); j++) {
                            outputString += t.f1.get(j).f0 + " " + t.f1.get(j).f1;
                            if (!(j == (t.f1.size()) - 1))
                                outputString += ", ";
                        }
                        outputString += "]";
 
                        return outputString;
                    }
                });
       
        // Output results to file
//        outputResults(aircraftUsedCountFive, outputFileName);
        env.execute("Task 3 executed.");
        long end = System.currentTimeMillis();
        System.out.println((end - start) + " ms");
       
        // print results
        // aircraftUsedCount.print();
    }
 
    //@SuppressWarnings("serial")
    public static class TailnumberCounter implements
            GroupReduceFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple4<String, String, String, String>> records,
                Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
            String airline = null;
            String tailnumber = null;
            String manufacturer = null;
            String model = null;
            int cnt = 0;
            for (Tuple4<String, String, String, String> flight : records) {
                airline = flight.f0;
                tailnumber = flight.f1;
                manufacturer = flight.f2;
                model = flight.f3;
                cnt++;
            }
            out.collect(
                    new Tuple5<String, String, String, String, Integer>(airline, tailnumber, manufacturer, model, cnt));
        }
    }
 
    //@SuppressWarnings("serial")
    private static class AirlineFlightJoin
            implements JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> join(Tuple2<String, String> airline, Tuple2<String, String> flight) throws Exception {
            return new Tuple2<String, String>(airline.f1, flight.f1);
        }
    }
 
    //@SuppressWarnings("serial")
    private static class ATNumberAndAircraftJoin implements
            JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>> {
        @Override
        public Tuple4<String, String, String, String> join(Tuple2<String, String> airlineTailNumbers,
                Tuple3<String, String, String> aircraft) throws Exception {
            return new Tuple4<String, String, String, String>(airlineTailNumbers.f0, airlineTailNumbers.f1, aircraft.f1,
                    aircraft.f2);
        }
    }
    // reduce by given country
    //@SuppressWarnings("serial")
    public static class countryAirlineReducer
            implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>> {
		private String country;

		// constructor
		countryAirlineReducer(String country) {
			this.country = country;
		}
        @Override
        public void reduce(Iterable<Tuple3<String, String, String>> records, Collector<Tuple2<String, String>> out)
                throws Exception {
            for (Tuple3<String, String, String> airline : records) {
            if(airline != null){
            	
                if (airline.f2 == null || country == null || !country.equals(airline.f2))
                    continue;
                out.collect(new Tuple2<String, String>(airline.f0, airline.f1));
            }}
            
        }
    }
 
    //@SuppressWarnings("serial")
    public static class FiveMostUsedReducer implements
            GroupReduceFunction<Tuple5<String, String, String, String, Integer>, Tuple2<String, ArrayList<Tuple2<String, String>>>> {
        @Override
        public void reduce(Iterable<Tuple5<String, String, String, String, Integer>> records,
                Collector<Tuple2<String, ArrayList<Tuple2<String, String>>>> out) throws Exception {
            String airlineName = "";
            ArrayList<String> modelMostUsed = new ArrayList<String>();
            ArrayList<Tuple2<String, String>> mostUsedList = new ArrayList<Tuple2<String, String>>();
            int counter = 0; // Counter to limit output tuples to 5 per airline
            for (Tuple5<String, String, String, String, Integer> flight : records) {
                if (airlineName.equals(""))
                    airlineName = flight.f0;
                if (counter == 5 || !(flight.f0.equals(airlineName))) {
                    if (flight.f0.equals(airlineName))
                        continue;
                    out.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(airlineName, mostUsedList));
                    mostUsedList.clear();
                    modelMostUsed.clear();
                    counter = 0;
                    airlineName = flight.f0;
                }
                if (modelMostUsed.contains(flight.f3))
                    continue;
                counter++;
                modelMostUsed.add(flight.f3);
                mostUsedList.add(new Tuple2<String, String>(flight.f2, flight.f3));
            }
            out.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(airlineName, mostUsedList));
        }
    }
 

 
}