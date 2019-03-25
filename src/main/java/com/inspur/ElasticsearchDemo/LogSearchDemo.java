package com.inspur.ElasticsearchDemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
/*日志文件检索demo*/
public class LogSearchDemo {
	public static void main(String[] args) {
		RestClient restClient = RestClient.builder(
		        new HttpHost("10.110.13.50", 9200, "http"))				
				.build();
		RestHighLevelClient restHighLevelClient =  new RestHighLevelClient(restClient);
		//查询服务类型为hdfs的日志
		searchLogByService(restHighLevelClient,"serviceslog-2018.11","hdfs");
		//查询服务类型为hdfs且level为error的日志
		searchLogByServiceAndLevel(restHighLevelClient,"serviceslog-2018.11","hdfs","ERROR");
		//日期范围
		searchLogByDateRange(restHighLevelClient,"serviceslog-2018.11","2018-11-08 06:16:32,395","2018-11-08 06:17:32,395");
		try {
			restClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void searchLogByService(RestHighLevelClient client,String indexName,String service) {
		SearchRequest searchRequest = new SearchRequest(); 
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		//精确匹配
		//QueryBuilder termQueryBuilder = QueryBuilders.termQuery("fields.service.keyword", service);
		//全文检索
		QueryBuilder termQueryBuilder = QueryBuilders.matchPhraseQuery("fields.service", service);
		searchSourceBuilder.query(termQueryBuilder);
		//searchSourceBuilder.from(0);
		//searchSourceBuilder.sort(name);
		searchSourceBuilder.size(15);//查询数据量，默认为10
		searchRequest.source(searchSourceBuilder).indices(indexName);
		try {
			System.out.println("执行search");
			SearchResponse searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			System.out.println(searchResponse);
			SearchHit[] hitsArr = hits.getHits();
			System.out.println("返回结果数："+hitsArr.length);
			for(SearchHit hit:hitsArr) {
				Map<String, Object> source = hit.getSource();
				for(String field:source.keySet()) {
					//System.out.println(field);
					Object value =  source.get(field);
					//System.out.println(value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void searchLogByServiceAndLevel(RestHighLevelClient client,String indexName,String service,String level) {
		SearchRequest searchRequest = new SearchRequest(); 
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		//QueryBuilder serviceQueryBuilder = QueryBuilders.termQuery("fields.service.keyword", service);
		//QueryBuilder levelQueryBuilder = QueryBuilders.termQuery("log_level.keyword", level);
		QueryBuilder serviceQueryBuilder = QueryBuilders.matchPhraseQuery("fields.service", service);
		QueryBuilder levelQueryBuilder = QueryBuilders.matchPhraseQuery("log_level", level);
		
		List<QueryBuilder> lq = new ArrayList<QueryBuilder>();
		lq.add(serviceQueryBuilder);
		lq.add(levelQueryBuilder);
		
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(serviceQueryBuilder).must(levelQueryBuilder));
		searchSourceBuilder.size(15);
		searchRequest.source(searchSourceBuilder).indices(indexName);
		try {
			System.out.println("执行search");
			SearchResponse searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			System.out.println(searchResponse);
			SearchHit[] hitsArr = hits.getHits();
			System.out.println("返回结果数："+hitsArr.length);
			for(SearchHit hit:hitsArr) {
				Map<String, Object> source = hit.getSource();
				for(String field:source.keySet()) {
					//System.out.println(field);
					Object value =  source.get(field);
					//System.out.println(value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void searchLogByDateRange(RestHighLevelClient client,String indexName,String from,String to) {
		SearchRequest searchRequest = new SearchRequest(); 
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("log_time.keyword").from(from).to(to);
		searchSourceBuilder.query(rangeQueryBuilder);
		searchRequest.source(searchSourceBuilder).indices(indexName);
		try {
			System.out.println("执行search");
			SearchResponse searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			System.out.println(searchResponse);
			SearchHit[] hitsArr = hits.getHits();
			System.out.println("返回结果数："+hitsArr.length);
			for(SearchHit hit:hitsArr) {
				Map<String, Object> source = hit.getSource();
				for(String field:source.keySet()) {
					//System.out.println(field);
					Object value =  source.get(field);
					//System.out.println(value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
