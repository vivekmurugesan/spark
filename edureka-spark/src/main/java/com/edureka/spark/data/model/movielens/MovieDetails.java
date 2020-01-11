package com.edureka.spark.data.model.movielens;

import java.io.Serializable;
import java.util.Map;

/**
 * 
 * @author vivek
 *
 */
public class MovieDetails implements Serializable {

	
	
	// True/False
	private boolean adultMovie;
	/* Format: {'id': 10194, 'name': 'Toy Story Collection',
	  'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 
	  'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'} */
	private String belongsTo;
	// 30000000
	private long budget;
	/*
	 * [{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, 
	 * {'id': 10751, 'name': 'Family'}]
	 */
	private Map<Integer, String> genres;
	/*
	 * http://toystory.disney.com/toy-story
	 */
	private String homePage;
	// 862
	private int id;
	// tt0114709
	private String imdbId;
	// en
	private String originalLanguage;
	// Toy Story
	private String originalTitle;
	/*
	 * Led by Woody, Andy's toys live happily in
 	 his room until Andy's birthday brings Buzz Lightyear onto the scene.
 	 Afraid of losing his place in Andy's heart, Woody plots against Buzz. 
 	 But when circumstances separate Buzz and Woody
 	 from their owner, the duo eventually learns to put aside their differences.
	 */
	private String overview;
	// 21.95
	private double popularity;
	// /rhIRbceoE9lR4veEXuwCC2wARtG.jpg
	private String posterPath;
	/*
	 * [{'name': 'Pixar Animation Studios', 'id': 3}]
	 */
	private String productionCompanies;
	/*
	 * [{'iso_3166_1':'US', 'name': 'United States of America'}]
	 */
	private String productionCountries;
	// 1995-10-30
	private String releaseDate;
	// 373554033
	private long revenue;
	// 81.0
	private double runtime;
	/*
	 * [{'iso_639_1': 'en', 'name': 'English'}]
	 */
	private String spokenLanguages;
	// Released
	private String status;
	/*
	 * Roll the dice and unleash the excitement!
	 */
	private String tagline;
	// Toy Story
	private String title;
	// True/False
	private boolean video;
	// 7.7
	private double voteAverage;
	// 5415
	private long voteCount;
	
	public MovieDetails() {
	}

	public boolean isAdultMovie() {
		return adultMovie;
	}

	public void setAdultMovie(boolean adultMovie) {
		this.adultMovie = adultMovie;
	}

	public String getBelongsTo() {
		return belongsTo;
	}

	public void setBelongsTo(String belongsTo) {
		this.belongsTo = belongsTo;
	}

	public long getBudget() {
		return budget;
	}

	public void setBudget(long budget) {
		this.budget = budget;
	}

	public Map<Integer, String> getGenres() {
		return genres;
	}

	public void setGenres(Map<Integer, String> genres) {
		this.genres = genres;
	}

	public String getHomePage() {
		return homePage;
	}

	public void setHomePage(String homePage) {
		this.homePage = homePage;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getImdbId() {
		return imdbId;
	}

	public void setImdbId(String imdbId) {
		this.imdbId = imdbId;
	}

	public String getOriginalLanguage() {
		return originalLanguage;
	}

	public void setOriginalLanguage(String originalLanguage) {
		this.originalLanguage = originalLanguage;
	}

	public String getOriginalTitle() {
		return originalTitle;
	}

	public void setOriginalTitle(String originalTitle) {
		this.originalTitle = originalTitle;
	}

	public String getOverview() {
		return overview;
	}

	public void setOverview(String overview) {
		this.overview = overview;
	}

	public double getPopularity() {
		return popularity;
	}

	public void setPopularity(double popularity) {
		this.popularity = popularity;
	}

	public String getPosterPath() {
		return posterPath;
	}

	public void setPosterPath(String posterPath) {
		this.posterPath = posterPath;
	}

	public String getProductionCompanies() {
		return productionCompanies;
	}

	public void setProductionCompanies(String productionCompanies) {
		this.productionCompanies = productionCompanies;
	}

	public String getProductionCountries() {
		return productionCountries;
	}

	public void setProductionCountries(String productionCountries) {
		this.productionCountries = productionCountries;
	}

	public String getReleaseDate() {
		return releaseDate;
	}

	public void setReleaseDate(String releaseDate) {
		this.releaseDate = releaseDate;
	}

	public long getRevenue() {
		return revenue;
	}

	public void setRevenue(long revenue) {
		this.revenue = revenue;
	}

	public double getRuntime() {
		return runtime;
	}

	public void setRuntime(double runtime) {
		this.runtime = runtime;
	}

	public String getSpokenLanguages() {
		return spokenLanguages;
	}

	public void setSpokenLanguages(String spokenLanguages) {
		this.spokenLanguages = spokenLanguages;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getTagline() {
		return tagline;
	}

	public void setTagline(String tagline) {
		this.tagline = tagline;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public boolean isVideo() {
		return video;
	}

	public void setVideo(boolean video) {
		this.video = video;
	}

	public double getVoteAverage() {
		return voteAverage;
	}

	public void setVoteAverage(double voteAverage) {
		this.voteAverage = voteAverage;
	}

	public long getVoteCount() {
		return voteCount;
	}

	public void setVoteCount(long voteCount) {
		this.voteCount = voteCount;
	}

	@Override
	public String toString() {
		return "MovieDetails [adultMovie=" + adultMovie + ", belongsTo=" + belongsTo + ", budget=" + budget
				+ ", genres=" + genres + ", homePage=" + homePage + ", id=" + id + ", imdbId=" + imdbId
				+ ", originalLanguage=" + originalLanguage + ", originalTitle=" + originalTitle + ", overview="
				+ overview + ", popularity=" + popularity + ", posterPath=" + posterPath + ", productionCompanies="
				+ productionCompanies + ", productionCountries=" + productionCountries + ", releaseDate=" + releaseDate
				+ ", revenue=" + revenue + ", runtime=" + runtime + ", spokenLanguages=" + spokenLanguages + ", status="
				+ status + ", tagline=" + tagline + ", title=" + title + ", video=" + video + ", voteAverage="
				+ voteAverage + ", voteCount=" + voteCount + "]";
	}
	
	
	
}
