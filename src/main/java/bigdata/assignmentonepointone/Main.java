package bigdata.assignmentonepointone;

import java.io.IOException;

public class Main {
	public static void main(String[] args) throws IOException {

		String ipAddress = "178.154.179.250";
		Long startTime = System.currentTimeMillis();
		LogParser log0 = new LogParser("Site0-access.log", ipAddress);
		Thread thread0 = new Thread(log0);
		thread0.start();
		LogParser log1 = new LogParser("Site1-access.log", ipAddress);
		Thread thread1 = new Thread(log1);
		thread1.start();
		LogParser log2 = new LogParser("Site2-access.log", ipAddress);
		Thread thread2 = new Thread(log2);
		thread2.start();
		LogParser log3 = new LogParser("Site3-access.log", ipAddress);
		Thread thread3 = new Thread(log3);
		thread3.start();
		LogParser log4 = new LogParser("Site4-access.log", ipAddress);
		Thread thread4 = new Thread(log4);
		thread4.start();
		LogParser log5 = new LogParser("Site5-access.log", ipAddress);
		Thread thread5 = new Thread(log5);
		thread5.start();
		try {
			thread0.join();
			thread1.join();
			thread2.join();
			thread3.join();
			thread4.join();
			thread5.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("File 0 results");
		ResultMap map0 = new ResultMap(log0.getResultsMap());
		// map0.printMap();
		System.out.println(map0.getStatusPercentages());

		System.out.println("File 1 results");
		ResultMap map1 = new ResultMap(log1.getResultsMap());
		// map1.printMap();
		System.out.println(map1.getStatusPercentages());

		System.out.println("File 2 results");
		ResultMap map2 = new ResultMap(log2.getResultsMap());
		// map2.printMap();
		System.out.println(map2.getStatusPercentages());

		System.out.println("File 3 results");
		ResultMap map3 = new ResultMap(log3.getResultsMap());
		// map3.printMap();
		System.out.println(map3.getStatusPercentages());

		System.out.println("File 4 results");
		ResultMap map4 = new ResultMap(log4.getResultsMap());
		// map4.printMap();
		System.out.println(map4.getStatusPercentages());

		System.out.println("File 5 results");
		ResultMap map5 = new ResultMap(log5.getResultsMap());
		// map5.printMap();
		System.out.println(map5.getStatusPercentages());

		ResultMap mergedMap = new ResultMap();
		mergedMap.setMap(ResultMap.mergeAndAdd(map0.getMap(), map1.getMap(),
				map2.getMap(), map3.getMap(), map4.getMap(), map5.getMap()));

		//System.out.println("Merged Map:");
		//mergedMap.printMap();

		System.out.println("Total Bytes: " + mergedMap.totalBytes());

		System.out.println("Merged Map Percentages");
		System.out.println(mergedMap.getStatusPercentages());

		Long endTime = System.currentTimeMillis();
		System.out.println("Duration: " + (endTime - startTime + "ms"));

	}

}
