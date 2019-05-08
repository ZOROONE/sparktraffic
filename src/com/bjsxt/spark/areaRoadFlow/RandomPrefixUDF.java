package com.bjsxt.spark.areaRoadFlow;

import java.util.Random;

import org.apache.spark.sql.api.java.UDF2;

/**
 * 指定一个数，随机生成前缀
 * @author ZORO
 *
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String>{
	private static final long serialVersionUID = 1L;

	@Override
	public String call(String area_name_road_id, Integer ranNum) throws Exception {
		Random random = new Random();
		int prefix = random.nextInt(ranNum);
		return prefix+"_"+area_name_road_id;
	}

}
