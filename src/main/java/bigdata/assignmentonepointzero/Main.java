package bigdata.assignmentonepointzero;

import java.io.IOException;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException {
        
        String ipAddress = "178.154.179.250";
        Date startDate = new Date();
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
        Date endDate = new Date();
        System.out.println("Duration: " + (endDate.getTime()-startDate.getTime() + "ms"));
        System.out.println("Thread 0 sum: " + log0.getSumOfBytes());
        System.out.println("Thread 1 sum: " + log1.getSumOfBytes());
        System.out.println("Thread 2 sum: " + log2.getSumOfBytes());
        System.out.println("Thread 3 sum: " + log3.getSumOfBytes());
        System.out.println("Thread 4 sum: " + log4.getSumOfBytes());
        System.out.println("Thread 5 sum: " + log5.getSumOfBytes());
        
        //Long finalSum = 
        System.out.println("TOTAL: " + (log0.getSumOfBytes() + log1.getSumOfBytes() + log2.getSumOfBytes()
            + log3.getSumOfBytes() + log4.getSumOfBytes() + log5.getSumOfBytes()));
    }
}
