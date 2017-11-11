package com.lpy.spark_applog;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 数据生成器
 * @author 柳培岳
 *
 */
public class DataGenerator {
	public static void main(String[] args) {
		Random random = new Random();
		StringBuffer buffer = new StringBuffer();
		List<String> deviceIDs = new ArrayList<String>();
		for (int i = 0; i < 100; i++) {
			deviceIDs.add(getRandomUUID());
		}

		for (int i = 0; i < 1000; i++) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(new Date());
			calendar.add(Calendar.MINUTE, -random.nextInt(600));
			long timeStamp = calendar.getTime().getTime();

			String deviceID = deviceIDs.get(random.nextInt(100));

			long upTraffic = random.nextInt(100000);

			long downTraffic = random.nextInt(100000);

			buffer.append(timeStamp).append("\t").append(deviceID).append("\t").append(upTraffic).append("\t")
					.append(downTraffic).append("\n");
			PrintWriter pw = null;

			try {
				pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("F:\\app_log.txt")));
				pw.write(buffer.toString());

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}finally {
				pw.close();//不关闭流的话导致一部分数据不能写入到文件中
			}
		}

	}

	public static String getRandomUUID() {
		return UUID.randomUUID().toString();
	}
}
