package bigdata.assignmentonepointwo;

import java.math.BigDecimal;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ResultMap {
	private TreeMap<Integer, Long> map;

	public TreeMap<Integer, Long> getMap() {
		return map;
	}

	public void setMap(TreeMap<Integer, Long> map) {
		this.map = map;
	}

	public ResultMap(TreeMap<Integer, Long> map) {
		this.map = map;
	}

	public ResultMap() {
		this.map = new TreeMap<Integer, Long>();
	}

	public Long totalBytes() {
		Long sum = 0L;
		for (Long value : map.values()) {
			sum += value;
		}
		return sum;
	}

	@SafeVarargs
	public static TreeMap<Integer, Long> mergeAndAdd(
			TreeMap<Integer, Long>... maps) {
		TreeMap<Integer, Long> result = new TreeMap<>();
		for (TreeMap<Integer, Long> map : maps) {
			for (Entry<Integer, Long> entry : map.entrySet()) {
				Integer key = entry.getKey();
				Long current = result.get(key);
				result.put(key,
						current == null ? entry.getValue() : entry.getValue()
								+ current);
			}
		}
		return result;
	}

	public void printMap() {
		for (Integer key : map.keySet()) {
			String http = String.format("%12s", "HTTP Status: " + key);
			String bytes = String.format("%-15s", " bytes: " + map.get(key));
			System.out.println(http + "|" + bytes);
		}
	}

	public String getStatusPercentages() {

		StringBuilder sb = new StringBuilder();
		for (Entry<Integer, Long> entry : map.entrySet()) {
			BigDecimal numerator = new BigDecimal((entry.getValue() * 100));
			BigDecimal denominator = new BigDecimal(totalBytes());
			BigDecimal percentageBigFinal = numerator.divide(denominator, 6, 1);

			String http = String.format("%12s",
					"HTTP Status: " + entry.getKey());
			String bytes = String
					.format("%-20s", "bytes: " + entry.getValue());
			String percentage = String.format("%-15s", "percentage: "
					+ percentageBigFinal.toString() + "%");

			sb.append(http + "|" + bytes + "|" + percentage).append("\n");
		}
		return sb.toString();

	}
}
