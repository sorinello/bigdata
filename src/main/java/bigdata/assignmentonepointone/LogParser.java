package bigdata.assignmentonepointone;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class LogParser implements Runnable {
	private String fileName;
	private String ipAddress;
	private TreeMap<Integer, Long> results = new TreeMap<Integer, Long>();
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

			while ((line = br.readLine()) != null) {
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					Integer statusCode = Integer.parseInt(matcher.group(2));
					Long newBytes = Long.valueOf(matcher.group(3));
					Long existingBytes = (Long) results.get(statusCode);

					if (existingBytes == null) {
						results.put(statusCode, newBytes);
					} else {
						results.put(statusCode, existingBytes + newBytes);
					}
				}
			}
		}
	}

	public TreeMap<Integer, Long> getResultsMap() {
		return this.results;

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