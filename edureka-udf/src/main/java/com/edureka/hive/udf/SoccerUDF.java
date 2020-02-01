package com.edureka.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 
 * @author vivek
 *
 */
public class SoccerUDF extends UDF {
	
	public boolean evaluate(int homeTeamGoals) {
		return homeTeamGoals>0;
	}	

}
