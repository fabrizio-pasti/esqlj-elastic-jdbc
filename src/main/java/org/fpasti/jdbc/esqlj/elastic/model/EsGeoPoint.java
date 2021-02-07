package org.fpasti.jdbc.esqlj.elastic.model;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class EsGeoPoint {
	private double latitude;
	private double longitude;
	
	public EsGeoPoint(double latitude, double longitude) {
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
