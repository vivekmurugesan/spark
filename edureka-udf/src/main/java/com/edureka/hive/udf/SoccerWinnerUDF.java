package com.edureka.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 
 * @author vivek
 *
 */
public class SoccerWinnerUDF extends UDF {

	public int evaluate(int homeTeamId, int awayTeamId, int homeTeamGoals, int awayTeamGoals) {
		if(homeTeamGoals == awayTeamGoals) 
			return -1;
		return ((homeTeamGoals>awayTeamGoals)?homeTeamId:awayTeamId);
	}
}
