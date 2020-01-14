package com.edureka.spark.movielens;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.spark_project.guava.base.Splitter;

import com.edureka.spark.data.model.movielens.MovieDetails;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * 
 * @author vivek
 *
 */
public class MovieDetailsParser implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// Format
	private final static String FORMAT_STR =
			"adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,"
					+ "original_language,original_title,overview,popularity,poster_path,"
					+ "production_companies,production_countries,release_date,reve" + 
					"nue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count";

	private final static String delim = ",";
	/*
	 * False,"{'id': 10194, 'name': 'Toy Story Collection', 
	 * 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 
	 * 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",
	 * 30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, 
	 * {'id': 10751, 'name': 'Family'}]
	 * ",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,
	 * "Led by Woody, Andy's toys live happily in his room until Andy's 
	 * birthday brings Buzz Lightyear onto the scene. Afraid of losing 
	 * his place in Andy's heart, Woody plots against Buzz. 
	 * But when circumstances separate Buzz and Woody 
	 * from their owner, the duo eventually learns to put aside their differences."
	 * ,21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg
	 * ,"[{'name': 'Pixar Animation Studios', 'id': 3}]"
	 * ,"[{'iso_3166_1': 'US', 'name': 'United States of America'}]"
	 * ,1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]"
	 * ,Released,,Toy Story,False,7.7,5415
	 */

	public static void main(String[] args) {
		String movie1Str = "False,\"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}\",30000000,\"[{'id': 16, 'name'" + 
				": 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]\",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,\"Led by Woody, Andy's toys live happily in" + 
				" his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody" + 
				" from their owner, the duo eventually learns to put aside their differences.\",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,\"[{'name': 'Pixar Animation Studios', 'id': 3}]\",\"[{'iso_3166_1':" + 
				" 'US', 'name': 'United States of America'}]\",1995-10-30,373554033,81.0,\"[{'iso_639_1': 'en', 'name': 'English'}]\",Released,,Toy Story,False,7.7,5415";
		MovieDetails movie1 = parse(movie1Str);
		
		System.out.println("Movie1 Details::\n" + 
				movie1);
		
		String movie2Str = "False,,0,[],,290157,tt0110217,en,Jupiter's Wife,\"Michel Negroponte, a documentary filmmaker, meets Maggie one day in Central Park. Maggie claims to be married to the god Jupiter and the " + 
				"daughter of actor Robert Ryan. Michel gets to know Maggie over the next couple of years, and attempts to use her often outlandish stories as clues to reconstruct her past." + 
				" - Written by James Meek\",0.001178,/uUi23HjvDFYGfuVlCBGozUY1Ab4.jpg,[],[],1995-01-01,0,87.0,[],Released,A Haunting Real Life Mystery,Jupiter's Wife,False,0.0,0";
	
		MovieDetails movie2 = parse(movie2Str);
		
		System.out.println("Movie2 Details::\n" + 
				movie2);
	}

	public static MovieDetails parse(String input) {
		
		List<String> tokensList = new ArrayList<>();
		
		Splitter.on(
				Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
				.split(input).forEach(x -> tokensList.add(x));;
		
		MovieDetails movie = new MovieDetails();
		//String[] tokens = input.split(delim);
		
		String[] tokens = tokensList.toArray(new String[0]);
		
		for(int i=0;i<tokens.length;i++) {
			System.out.printf("Token: %d before unqote: %s \n",i, tokens[i]);
			tokens[i] = unquote(tokens[i]);
			System.out.printf("Token: %d after unqote: %s \n",i, tokens[i]);
		}
		movie.setAdultMovie(Boolean.parseBoolean(tokens[0]));
		movie.setBelongsTo(tokens[1]);
		movie.setBudget(Long.parseLong(tokens[2]));
		String genreString = tokens[3];
		movie.setGenres(parseGenreStr(genreString));
		movie.setHomePage(tokens[4]);
		movie.setId(Integer.parseInt(tokens[5]));
		movie.setImdbId(tokens[6]);
		movie.setOriginalLanguage(tokens[7]);
		movie.setOriginalTitle(tokens[8]);
		movie.setOverview(tokens[9]);
		movie.setPopularity(Double.parseDouble(tokens[10]));
		movie.setPosterPath(tokens[11]);
		movie.setProductionCompanies(tokens[12]);
		movie.setProductionCountries(tokens[13]);
		movie.setReleaseDate(tokens[14]);
		movie.setRevenue(Long.parseLong(tokens[15]));
		movie.setRuntime(Double.parseDouble(tokens[16]));
		movie.setSpokenLanguages(tokens[17]);
		movie.setStatus(tokens[18]);
		movie.setTagline(tokens[19]);
		movie.setTitle(tokens[20]);
		movie.setVideo(Boolean.parseBoolean(tokens[21]));
		movie.setVoteAverage(Double.parseDouble(tokens[22]));
		movie.setVoteCount(Long.parseLong(tokens[23]));

		return movie;
	}
	
	private static String unquote(String input) {
		if(input.startsWith("\"") && input.endsWith("\""))
			return input.substring(1, input.length()-1);
		else
			return input;
	}

	private static Map<Integer, String> parseGenreStr(String genreStr){
		//genreStr = unquote(genreStr);
		System.out.println(genreStr);
		Map<Integer, String> genres = new HashMap<>();
		Gson gson = new Gson();
		JsonArray arr = 
				gson.fromJson(genreStr, JsonArray.class);
		int size = arr.size();
		for(int i=0;i<size;i++) {
			JsonElement element = arr.get(i);
			JsonObject obj = element.getAsJsonObject();
			int id = obj.get("id").getAsInt();
			String name = obj.get("name").getAsString();
			genres.put(id, name);
		}

		return genres;
	}
}
