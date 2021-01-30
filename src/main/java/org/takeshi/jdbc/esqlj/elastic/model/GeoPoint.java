package org.takeshi.jdbc.esqlj.elastic.model;

public class GeoPoint {
	private double latitude;
	private double longitude;
	
	public GeoPoint(double latitude, double longitude) {
		super();
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}
	
}
