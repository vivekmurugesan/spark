package com.edureka.spark.data.model;

import java.io.Serializable;

/**
 * 
 * @author vivek
 *
 */
public class ODPairSummary implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int pickupCommunity;
	private int dropoffCommunity;
	
	private int tripCount;
	
	private int totalMins;
	private double totalMiles;
	private double totalAmount;
	
	private double avgMins;
	private double avgMiles;
	private double avgAmount;
	
	
	public ODPairSummary() {
	}

	public int getPickupCommunity() {
		return pickupCommunity;
	}

	public void setPickupCommunity(int pickupCommunity) {
		this.pickupCommunity = pickupCommunity;
	}

	public int getDropoffCommunity() {
		return dropoffCommunity;
	}

	public void setDropoffCommunity(int dropoffCommunity) {
		this.dropoffCommunity = dropoffCommunity;
	}

	public int getTripCount() {
		return tripCount;
	}

	public void setTripCount(int tripCount) {
		this.tripCount = tripCount;
	}

	public int getTotalMins() {
		return totalMins;
	}

	public void setTotalMins(int totalMins) {
		this.totalMins = totalMins;
	}

	public double getTotalMiles() {
		return totalMiles;
	}

	public void setTotalMiles(double totalMiles) {
		this.totalMiles = totalMiles;
	}

	public double getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(double totalAmount) {
		this.totalAmount = totalAmount;
	}

	public double getAvgMins() {
		return avgMins;
	}

	public void setAvgMins(double avgMins) {
		this.avgMins = avgMins;
	}

	public double getAvgMiles() {
		return avgMiles;
	}

	public void setAvgMiles(double avgMiles) {
		this.avgMiles = avgMiles;
	}

	public double getAvgAmount() {
		return avgAmount;
	}

	public void setAvgAmount(double avgAmount) {
		this.avgAmount = avgAmount;
	}

	@Override
	public String toString() {
		String result =  pickupCommunity + "," + dropoffCommunity + "," + tripCount + "," + totalMins + "," + totalMiles
				+ "," + totalAmount + "," + avgMins + "," + avgMiles + ","
				+ avgAmount ;
		
		return result;
	}
	
	
}
