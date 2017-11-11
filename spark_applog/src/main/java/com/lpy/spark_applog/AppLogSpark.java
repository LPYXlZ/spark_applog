package com.lpy.spark_applog;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AppLogSpark {
	public static void main(String[] args) {
		
	    DBHelper db = new DBHelper();
		//创建spark配置文件和上下文对象
		SparkConf conf = new SparkConf().setAppName("AppLogSpark").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//读取日志文件，并创建一个RDD，使用SparkContext的TextFile()方法，可以读取本地磁盘或者hdfs上的文件
		JavaRDD<String> accessLogRDD  = sc.textFile("F:\\app_log.txt");
		
		//将RDD映射为key - value 格式,为后面的ruduceByKey做准备
		JavaPairRDD<String, AccessLogInfo> accesslogpairRDD = mapAccessLogRDD2Pair(accessLogRDD);
		
		//将按照deviceID聚合的key映射为二次排序的key，value映射为deviceID
		JavaPairRDD<AccessLogSortKey, String> accesslogSortRDD = mapRddKey2SortKey(accesslogpairRDD);
		//实现降序排序获取前top10
		JavaPairRDD<AccessLogSortKey, String> sortedAccessLogRDD = accesslogSortRDD.sortByKey(false);
		List<Tuple2<AccessLogSortKey, String>> top10DataList = sortedAccessLogRDD.take(10);
	    //sortedAccessLogRDD.saveAsTextFile(path);  //存储到本地或者hdfs上
		/*//打印前10个数据
		for (Tuple2<AccessLogSortKey, String> tuple2 : top10DataList) {
			System.out.println(tuple2._2 +" "+tuple2._1.getUpTraffic()+" "+tuple2._1.getDownTraffic());
		}*/
		
		//把数据存储到数据库中。
		for (Tuple2<AccessLogSortKey, String> tuple2 : top10DataList) {
			db.insetData(tuple2._2, tuple2._1.getUpTraffic()+"", tuple2._1.getDownTraffic()+"", tuple2._1.getTimestamp()+"");
		}
		sc.close();//关闭上下文
	}
	
    /**
     * 将日志的RDD映射为key - value 格式
     * @param accessLogRDD
     * @return
     */
	private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(JavaRDD<String> accessLogRDD){
		return accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
			
			private static final long serialVersionUID = 1L;

			public Tuple2<String, AccessLogInfo> call(String accessLog) throws Exception {
				//根据\t对日志进行切分
				
				String[] accessLogSplited = accessLog.split("\t");
				//获取4个字段
				long timestamp = Long.valueOf(accessLogSplited[0]);
				String deviceID = accessLogSplited[1];
				long upTraffic = Long.valueOf(accessLogSplited[2]);
				long downTraffic = Long.valueOf(accessLogSplited[3]);
				
				//将时间戳，上行流量，下行流量封装为自定义的可序列化对象
				AccessLogInfo accessLogInfo=new AccessLogInfo(timestamp,upTraffic,downTraffic);
				
				//直接返回一个Tuple<String ,AccessLoginfo>
				
				return new Tuple2<String, AccessLogInfo>(deviceID, accessLogInfo);
			}
		});
	}
	
	/**
	 * 将RDD的key映射为二次排序的key
	 * @return
	 */
	private static JavaPairRDD<AccessLogSortKey, String> mapRddKey2SortKey(JavaPairRDD<String, AccessLogInfo> accesslogpairRDD){
		return accesslogpairRDD.mapToPair(new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> tuple) throws Exception {

				String deviceID=tuple._1;
				AccessLogInfo accessLogInfo = tuple._2;
				AccessLogSortKey accessLogSortKey = new AccessLogSortKey(accessLogInfo.getTimestamp(),accessLogInfo.getUpTraffic(),accessLogInfo.getDownTraffic());
				
				return new Tuple2<AccessLogSortKey, String>(accessLogSortKey, deviceID);
			}
		});
	}
}
