package com.edureka.hbase.youtube.data.model;

/**
 * 
 * @author vivek
 *
 */
public class VideoDetails {

	private String videoId;
	
	/**
	 * Meta-data of the video.
	 */
	private String regionId;
	private String title;
	private String channelTitle;
	private String trendingDate;
	private String categoryId;
	private String publishTime;
	
	/**
	 * Stats -- likes, dislikes, comments_count
	 */
	private String viewCount;
	private String likeCount;
	private String dislikeCount;
	private String commentCount;
	
	/**
	 * Flags.
	 */
	private String commentsDisabled;
	private String ratingsDisabled;
	private String videoErrOrRemoved;
	
	
	public VideoDetails() {
	}
	
	public String getVideoId() {
		return videoId;
	}
	public void setVideoId(String videoId) {
		this.videoId = videoId;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getChannelTitle() {
		return channelTitle;
	}
	public void setChannelTitle(String channelTitle) {
		this.channelTitle = channelTitle;
	}
	public String getTrendingDate() {
		return trendingDate;
	}
	public void setTrendingDate(String trendingDate) {
		this.trendingDate = trendingDate;
	}
	public String getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}
	public String getPublishTime() {
		return publishTime;
	}
	public void setPublishTime(String publishTime) {
		this.publishTime = publishTime;
	}
	public String getViewCount() {
		return viewCount;
	}
	public void setViewCount(String viewCount) {
		this.viewCount = viewCount;
	}
	public String getLikeCount() {
		return likeCount;
	}
	public void setLikeCount(String likeCount) {
		this.likeCount = likeCount;
	}
	public String getDislikeCount() {
		return dislikeCount;
	}
	public void setDislikeCount(String dislikeCount) {
		this.dislikeCount = dislikeCount;
	}
	public String getCommentCount() {
		return commentCount;
	}
	public void setCommentCount(String commentCount) {
		this.commentCount = commentCount;
	}
	public String getCommentsDisabled() {
		return commentsDisabled;
	}
	public void setCommentsDisabled(String commentsDisabled) {
		this.commentsDisabled = commentsDisabled;
	}
	public String getRatingsDisabled() {
		return ratingsDisabled;
	}
	public void setRatingsDisabled(String ratingsDisabled) {
		this.ratingsDisabled = ratingsDisabled;
	}
	public String getVideoErrOrRemoved() {
		return videoErrOrRemoved;
	}
	public void setVideoErrOrRemoved(String videoErrOrRemoved) {
		this.videoErrOrRemoved = videoErrOrRemoved;
	}

	public String getRegionId() {
		return regionId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}
	
	
}
