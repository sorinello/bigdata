package bigdata;
import java.io.IOException;
public class Main {
	public static void main(String[] args) throws IOException {
		LogParser log0 = new LogParser("Site0-access.log", "::1");
		Thread thread0 = new Thread(log0);
		thread0.start();
		LogParser log1 = new LogParser("Site1-access.log", "178.162.193.33");
		Thread thread1 = new Thread(log1);
		thread1.start();
		LogParser log2 = new LogParser("Site2-access.log", "46.229.166.195");
		Thread thread2 = new Thread(log2);
		thread2.start();
		LogParser log3 = new LogParser("Site3-access.log", "220.181.108.157");
		Thread thread3 = new Thread(log3);
		thread3.start();
		LogParser log4 = new LogParser("Site4-access.log", "66.249.74.147");
		Thread thread4 = new Thread(log4);
		thread4.start();
		LogParser log5 = new LogParser("Site5-access.log", "89.121.227.146");
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
		System.out.println("Thread 0 sum: " + log0.getSumOfBytes());
		System.out.println("Thread 1 sum: " + log1.getSumOfBytes());
		System.out.println("Thread 2 sum: " + log2.getSumOfBytes());
		System.out.println("Thread 3 sum: " + log3.getSumOfBytes());
		System.out.println("Thread 4 sum: " + log4.getSumOfBytes());
		System.out.println("Thread 5 sum: " + log5.getSumOfBytes());
	}
}