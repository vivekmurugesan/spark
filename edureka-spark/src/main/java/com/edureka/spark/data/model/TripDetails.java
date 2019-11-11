package com.edureka.spark.data.model;

import java.io.Serializable;

/**
 * 
 * @author vivek
 *
 */
public class TripDetails implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private int pikcupCommunity;
	private int dropoffCommunity;
	private double tripTotalAmt;
	private double tripMiles;
	private int tripSeconds;
	
	public TripDetails(int pikcupCommunity, int dropoffCommunity, double tripTotalAmt, double tripMiles,
			int tripSeconds) {
		super();
		this.pikcupCommunity = pikcupCommunity;
		this.dropoffCommunity = dropoffCommunity;
		this.tripTotalAmt = tripTotalAmt;
		this.tripMiles = tripMiles;
		this.tripSeconds = tripSeconds;
	}

	public TripDetails() {
	}

	public int getPikcupCommunity() {
		return pikcupCommunity;
	}

	public void setPikcupCommunity(int pikcupCommunity) {
		this.pikcupCommunity = pikcupCommunity;
	}

	public int getDropoffCommunity() {
		return dropoffCommunity;
	}

	public void setDropoffCommunity(int dropoffCommunity) {
		this.dropoffCommunity = dropoffCommunity;
	}

	public double getTripTotalAmt() {
		return tripTotalAmt;
	}

	public void setTripTotalAmt(double tripTotalAmt) {
		this.tripTotalAmt = tripTotalAmt;
	}

	public double getTripMiles() {
		return tripMiles;
	}

	public void setTripMiles(double tripMiles) {
		this.tripMiles = tripMiles;
	}

	public int getTripSeconds() {
		return tripSeconds;
	}

	public void setTripSeconds(int tripSeconds) {
		this.tripSeconds = tripSeconds;
	}
	
}
