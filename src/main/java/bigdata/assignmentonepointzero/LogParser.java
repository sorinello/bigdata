package bigdata.assignmentonepointzero;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class LogParser implements Runnable {
	private String fileName;
	private String ipAddress;
	private Long sumOfBytes = 0L;
	public LogParser(String fileName, String ipAddress) throws IOException {
		this.fileName = fileName;
		this.ipAddress = ipAddress;
	}
	private void parseLog() throws IOException, URISyntaxException {
		File log = new File(this.getClass().getResource("/" + fileName).toURI());
		if (log.exists()) {
			System.out.println("Found file " + log.getCanonicalPath());
		}
		FileReader logFileReader = new FileReader(log);
		try (BufferedReader br = new BufferedReader(logFileReader)) {
			String regex = String.format(
					"^%s - - (.*) (\\d{3}) (\\d+) \"(.*)$", this.ipAddress);
			Pattern pattern = Pattern.compile(regex);
			String line;
			int result = 0;
			while ((line = br.readLine()) != null) {
				// System.out.println(line);
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					//System.out.print("Result " + result + "###");
					//System.out.println(line + matcher.group(3));
					sumOfBytes += Integer.parseInt(matcher.group(3));
					result++;
				}
			}
		}
		// logFileReader.close();
	}
	public Long getSumOfBytes() {
		return this.sumOfBytes;
	}
	@Override
	public void run() {
		try {
			System.out.println("STARTING THREAD");
			parseLog();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}