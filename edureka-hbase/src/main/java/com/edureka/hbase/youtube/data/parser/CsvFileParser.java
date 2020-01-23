package com.edureka.hbase.youtube.data.parser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.client.HTable;

import com.edureka.hbase.youtube.data.hbase.HBaseInsertData;
import com.edureka.hbase.youtube.data.model.VideoDetails;

/**
 * 
 * @author vivek
 *
 */
public class CsvFileParser {
	
	/**
	 * video_id=0, trending_date=1, title=2, channel_title=3, category_id=4, 
	 * publish_time=5, tags=6, views=7, likes=8, dislikes=9, comment_count=10, 
	 * thumbnail_link=11, comments_disabled=12, ratings_disabled=13, video_error_or_removed=14, description=15
	 */

	private static final String VideoIdCol = "video_id";
	
	private static final String TitleCol = "title";
	private static final String ChannelTitleCol = "channel_title";
	private static final String TrendingDateCol = "trending_date";
	private static final String CatIdCol = "category_id";
	private static final String PublishTimeCol = "publish_time";
	
	private static final String ViewCountCol = "views";
	private static final String LikeCountCol = "likes";
	private static final String DislikeCountCol = "dislikes";
	private static final String CommentCountCol = "comment_count";
	
	private static final String CommentsDisabledCol = "comments_disabled";
	private static final String RatingsDisabledCol = "ratings_disabled";
	private static final String VideoErrOrRemovedCol = "video_error_or_removed";
	
	private static final String[] csvFiles = {
			"CAvideos.csv",  "DEvideos.csv",  "FRvideos.csv",  "GBvideos.csv",  "INvideos.csv",  
			"JPvideos.csv",  "KRvideos.csv",  "MXvideos.csv",  "RUvideos.csv",  "USvideos.csv"
	};
	
	private String directory="/mnt/bigdatapgp/edureka_549997/datasets/youtube";
	
	public static void main(String[] args) throws IOException {
		CsvFileParser parser = new CsvFileParser();
		//parser.parseFile("src/main/resources/sample-video-stat.csv");
		parser.processAllFiles();
	}
	
	public CsvFileParser() {
	}
	
	public CsvFileParser(String directory) {
		this.directory = directory;
	}
	
	public void processAllFiles() {
		String fileName = this.directory + "/";
		String regionName;
		
		for(String csvFileName : csvFiles) {
			fileName = fileName + csvFileName;
			regionName = csvFileName.substring(0, 2);
			try {
				parseAndProcessFile(fileName, regionName);
			} catch (IOException e) {
				System.out.println(".. Not able to process file: " + fileName);
				e.printStackTrace();
			}
		}
	}
	
	public void parseAndProcessFile(String fileName, String regionName) throws IOException {
		File csvFile = new File(fileName);
		CSVParser parser = 
				CSVFormat.DEFAULT.withFirstRecordAsHeader().withQuote(null).withDelimiter(',')
			.parse(new FileReader(csvFile));
		/*
		 * CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_16BE,
		 * CSVFormat.POSTGRESQL_CSV);
		 */ 
		
		Map<String, Integer> headerMap = 
				parser.getHeaderMap();
		
		System.out.println("Headers..\n" + headerMap);
		
		List<CSVRecord> csvRecords = parser.getRecords();
		
		HBaseInsertData hbaseInserter = new HBaseInsertData();
		
		HTable htable = hbaseInserter.createTableHandle();
		
		int i = 0;
		for(CSVRecord record : csvRecords) {
			System.out.printf(".. Processing Record....:%d",i++);
			
			VideoDetails videoDetails = new VideoDetails();
			
			videoDetails.setVideoId(record.get(headerMap.get(VideoIdCol)));
			
			videoDetails.setRegionId(regionName);
			
			videoDetails.setTitle(record.get(headerMap.get(TitleCol)));
			videoDetails.setChannelTitle(record.get(headerMap.get(ChannelTitleCol)));
			videoDetails.setTrendingDate(record.get(headerMap.get(TrendingDateCol)));
			videoDetails.setCategoryId(record.get(headerMap.get(CatIdCol)));
			videoDetails.setPublishTime(record.get(headerMap.get(PublishTimeCol)));
			
			videoDetails.setViewCount(Long.parseLong(record.get(headerMap.get(ViewCountCol))));
			videoDetails.setLikeCount(Long.parseLong(record.get(headerMap.get(LikeCountCol))));
			videoDetails.setDislikeCount(Long.parseLong(record.get(headerMap.get(DislikeCountCol))));
			videoDetails.setCommentCount(Long.parseLong(record.get(headerMap.get(CommentCountCol))));
			
			videoDetails.setCommentsDisabled(record.get(headerMap.get(CommentsDisabledCol)));
			videoDetails.setRatingsDisabled(record.get(headerMap.get(RatingsDisabledCol)));
			videoDetails.setVideoErrOrRemoved(record.get(headerMap.get(VideoErrOrRemovedCol)));
			
			hbaseInserter.insertData(videoDetails, htable);
			
			/*
			 * for(Iterator<String> iter = record.iterator();iter.hasNext();) { String val =
			 * iter.next(); System.out.println("Col:" + val); }
			 */
			System.out.println("........End...........");
		}
		
		
		hbaseInserter.closeTableHandle(htable);
		
	}

}
