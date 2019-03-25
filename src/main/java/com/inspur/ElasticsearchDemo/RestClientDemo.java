package com.inspur.ElasticsearchDemo;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class RestClientDemo {

	
	public static RestClient getRestClientWithoutSearchGuard(String ip,int port) {
		RestClient restClient = RestClient.builder(
		        new HttpHost(ip, port, "http"))				
				.build();
		System.out.println("restClient连接成功");
		return restClient;
	}
	
	/**
	 * @param ip
	 * @param port
	 * @param userName
	 * @param password
	 * @return
	 */
	public static RestClient getRestClientWithSearchGuard(String ip,int port,String userName,String password) {
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
		        new UsernamePasswordCredentials(userName, password));
		RestClient restClient = RestClient.builder(
		        new HttpHost(ip, port, "http"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
		            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
		                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		            }})
				.build();
		System.out.println("restClient连接成功");
		return restClient;
	}
}
