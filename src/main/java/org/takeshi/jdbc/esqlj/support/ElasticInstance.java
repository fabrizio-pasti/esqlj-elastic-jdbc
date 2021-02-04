package org.takeshi.jdbc.esqlj.support;

/**
* @author  Fabrizio Pasti - fabrizio.pasti@gmail.com
*/

public class ElasticInstance {
	public enum HttpProtocol {
		http, https
	}

	public ElasticInstance(HttpProtocol protocol, String server, int port) {
		this.protocol = protocol;
		this.server = server;
		this.port = port;
	}

	private String server;
	private HttpProtocol protocol;
	private int port;

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public HttpProtocol getProtocol() {
		return protocol;
	}

	public void setProtocol(HttpProtocol protocol) {
		this.protocol = protocol;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public String getUrl() {
		return String.format("%s://%s:%d", getProtocol(), getServer(), getPort());
	}

}
