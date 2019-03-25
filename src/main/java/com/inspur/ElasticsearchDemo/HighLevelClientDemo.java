package com.inspur.ElasticsearchDemo;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/*HighLevelClient开发demo */
public class HighLevelClientDemo {
	private static RestClient restClient = null;
	public static void main(String [] args) {
		System.out.println("----开始");
		
		String ip = "10.111.24.95";//es节点ip
		int port = 9200;//es http服务端口
		String userName = "tenant1234-master";//用户名为仓库平台登陆用户（要添加realm后缀）
		String password = "tenant1234";//密码为仓库平台登陆密码

		//获取RestClient
		RestClient restClient =  getRestClientWithSearchGuard( ip, port, userName, password) ;
		//获取RestHighLevelClient
		RestHighLevelClient restHighLevelClient =  new RestHighLevelClient(restClient);		
		String indexName = "index4";//索引名称
		
		//索引一条数据
		Map<String, Object> jsonMap = new HashMap<String,Object>();
		jsonMap = new HashMap<String, Object>();
		jsonMap.put("user", "tom");
		jsonMap.put("postDate", new Date());
		jsonMap.put("message", "trying out Elasticsearch");
		//index(restHighLevelClient,indexName,typeName,jsonMap);
		
		//检索indexName索引中user字段值为tom的记录
		//search( restHighLevelClient, indexName, "user", "tom");
		try {
			restClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("----结束");
	}
	
	/**索引一条数据
	 * @param client
	 * @param index
	 * @param type
	 * @param jsonMap
	 */
	public static void index(RestHighLevelClient client,String index,String type,Map<String, Object> jsonMap) {
		IndexRequest indexRequest = new IndexRequest(index,type)
		        .source(jsonMap);
		try {
		    IndexResponse indexResponse = client.index(indexRequest);
		    System.out.println(indexResponse.toString());
		} catch(ElasticsearchException e) {
		   e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**指定id索引一条数据
	 * @param client
	 * @param index
	 * @param type
	 * @param id
	 * @param jsonMap
	 */
	public static void index(RestHighLevelClient client,String index,String type,String id,Map<String, Object> jsonMap) {
		IndexRequest indexRequest = new IndexRequest(index,type,id)
		        .source(jsonMap);
		try {
		    IndexResponse indexResponse = client.index(indexRequest);

		} catch(ElasticsearchException e) {
		    if (e.status() == RestStatus.CONFLICT) {
		        
		    }
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	
	/**检索数据
	 * @param client 客户端
	 * @param index 索引名
	 * @param fieldName1  查询条件：字段名
	 * @param fieldValue1 查询字段值
	 */
	public static void search(RestHighLevelClient client,String index,String fieldName1,String fieldValue1) {
		SearchRequest searchRequest = new SearchRequest(); 
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
		QueryBuilders.fuzzyQuery(fieldName1, fieldValue1);//模糊查询
		searchSourceBuilder.from(0); 
		searchSourceBuilder.size(5);
		
		//QueryBuilders.termQuery(fieldName, fieldName);//精确查询
		//QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(fieldName, fieldName));//布尔查询
		searchRequest.source(searchSourceBuilder);
		searchRequest.indices(index);
		try {
			System.out.println("执行search");
			SearchResponse searchResponse = client.search(searchRequest);
			SearchHits hits = searchResponse.getHits();
			System.out.println(searchResponse);
			SearchHit[] hitsArr = hits.getHits();
			for(SearchHit hit:hitsArr) {
				Map<String, Object> source = hit.getSource();
				for(String field:source.keySet()) {
					System.out.println(field);
					Object value =  source.get(field);
					System.out.println(value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static RestHighLevelClient getHighLevelClientWithSearchGuard(String esip,int port,String userName,String password) {	
		RestHighLevelClient client =
			    new RestHighLevelClient(getRestClientWithSearchGuard(esip,port,userName,password)); 
		return client;		
	}
	
	public static RestClient getRestClientWithSearchGuard(String esip,int port,String userName,String password) {
		if(restClient==null) {
			restClient =RestClientDemo.getRestClientWithSearchGuard(esip,port,userName,password);
		}
		return restClient;
	}
}
