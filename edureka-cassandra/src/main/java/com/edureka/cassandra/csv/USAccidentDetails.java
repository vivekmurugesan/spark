package com.edureka.cassandra.csv;

/**
 * 
 * @author vivek
 *
 */
public class USAccidentDetails {

	/**
	 * ID,Source,TMC,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,
	 * Distance(mi),Description,Number,Street,Side,City,County,State,
	 * Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,
	 * Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),
	 * Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,
	 * Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,
	 * Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,
	 * Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
	 */
	
	
	private String id;
	
	// Time details
	private String startTime;
	private String endTime;
	
	// Position details
	private String startLat;
	private String startLng;
	private String endLat;
	private String endLng;
	
	// Address details
	private String number;
	private String street;
	private String side;
	private String city;
	private String county;
	private String state;
	private String zip;
	private String country;
	
	// Weather details
	private String weatherTimestamp;
	private String temparature;
	private String windChill;
	private String humidity;
	private String pressure;
	private String visibility;
	private String windDirection;
	private String windSpeed;
	private String precipitation;
	private String weatherCondition;
	
	public USAccidentDetails() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getStartLat() {
		return startLat;
	}

	public void setStartLat(String startLat) {
		this.startLat = startLat;
	}

	public String getStartLng() {
		return startLng;
	}

	public void setStartLng(String startLng) {
		this.startLng = startLng;
	}

	public String getEndLat() {
		return endLat;
	}

	public void setEndLat(String endLat) {
		this.endLat = endLat;
	}

	public String getEndLng() {
		return endLng;
	}

	public void setEndLng(String endLng) {
		this.endLng = endLng;
	}

	public String getNumber() {
		return number;
	}

	public void setNumber(String number) {
		this.number = number;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getSide() {
		return side;
	}

	public void setSide(String side) {
		this.side = side;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCounty() {
		return county;
	}

	public void setCounty(String county) {
		this.county = county;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getWeatherTimestamp() {
		return weatherTimestamp;
	}

	public void setWeatherTimestamp(String weatherTimestamp) {
		this.weatherTimestamp = weatherTimestamp;
	}

	public String getTemparature() {
		return temparature;
	}

	public void setTemparature(String temparature) {
		this.temparature = temparature;
	}

	public String getWindChill() {
		return windChill;
	}

	public void setWindChill(String windChill) {
		this.windChill = windChill;
	}

	public String getHumidity() {
		return humidity;
	}

	public void setHumidity(String humidity) {
		this.humidity = humidity;
	}

	public String getPressure() {
		return pressure;
	}

	public void setPressure(String pressure) {
		this.pressure = pressure;
	}

	public String getVisibility() {
		return visibility;
	}

	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}

	public String getWindDirection() {
		return windDirection;
	}

	public void setWindDirection(String windDirection) {
		this.windDirection = windDirection;
	}

	public String getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(String windSpeed) {
		this.windSpeed = windSpeed;
	}

	public String getPrecipitation() {
		return precipitation;
	}

	public void setPrecipitation(String precipitation) {
		this.precipitation = precipitation;
	}

	public String getWeatherCondition() {
		return weatherCondition;
	}

	public void setWeatherCondition(String weatherCondition) {
		this.weatherCondition = weatherCondition;
	}
	
}
