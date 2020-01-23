package com.edureka.hbase.youtube.data.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.edureka.hbase.youtube.data.model.VideoDetails;

/**
 * 
 * @author vivek
 *
 */
public class HBaseInsertData {
	
	public HBaseInsertData() {
	}
	
	private String videoStatTable = "Youtube_Video_Stats";
	
	private String metaDataCF = "meta_data";
	private String statsCF = "stats";
	private String flagsCF = "flags";

	public HTable createTableHandle(String tableName) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		
		conf.clear();
        conf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal");
        conf.set("hbase.zookeeper.property.clientPort", "2181");


		HTable hTable = new HTable(conf, tableName);
		
		return hTable;
	}
	
	public HTable createTableHandle() throws IOException {
		return this.createTableHandle(videoStatTable);
	}
	
	public void closeTableHandle(HTable tableHandle) throws IOException {
		tableHandle.close();
	}
	
	public void insertData(VideoDetails videoDetails, HTable hTable) throws IOException {
		
		Put p = new Put(Bytes.toBytes(videoDetails.getVideoId()));
		
		// add values using add() method
		
		//MetaData CF
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("region_id"),Bytes.toBytes(videoDetails.getRegionId()));
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("title"),Bytes.toBytes(videoDetails.getTitle()));
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("channel_title"),Bytes.toBytes(videoDetails.getChannelTitle()));
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("trendig_date"),Bytes.toBytes(videoDetails.getTrendingDate()));
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("category_id"),Bytes.toBytes(videoDetails.getCategoryId()));
		p.add(Bytes.toBytes(metaDataCF),
				Bytes.toBytes("publish_time"),Bytes.toBytes(videoDetails.getPublishTime()));
		
		
		//Stats CF
		p.add(Bytes.toBytes(statsCF),
				Bytes.toBytes("view_count"),Bytes.toBytes(videoDetails.getViewCount()));
		p.add(Bytes.toBytes(statsCF),
				Bytes.toBytes("like_count"),Bytes.toBytes(videoDetails.getLikeCount()));
		p.add(Bytes.toBytes(statsCF),
				Bytes.toBytes("dislike_count"),Bytes.toBytes(videoDetails.getDislikeCount()));
		p.add(Bytes.toBytes(statsCF),
				Bytes.toBytes("comment_count"),Bytes.toBytes(videoDetails.getCommentCount()));
		
		
		//Flags CF
		p.add(Bytes.toBytes(flagsCF),
				Bytes.toBytes("comments_disabled"),Bytes.toBytes(videoDetails.getCommentsDisabled()));
		p.add(Bytes.toBytes(flagsCF),
				Bytes.toBytes("ratings_disabled"),Bytes.toBytes(videoDetails.getRatingsDisabled()));
		p.add(Bytes.toBytes(flagsCF),
				Bytes.toBytes("video_err_removed"),Bytes.toBytes(videoDetails.getVideoErrOrRemoved()));
		
		hTable.put(p);
		
	}
	
	
}
