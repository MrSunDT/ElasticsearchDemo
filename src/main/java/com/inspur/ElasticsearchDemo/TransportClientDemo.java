package com.inspur.ElasticsearchDemo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
/*在启用searchguard安全的仓库平台es环境下，无法使用*/
public class TransportClientDemo {
	
	public static void main(String[] args) throws UnknownHostException {
		//创建客户端
		TransportClient client = getClientWithNoXpack("10.110.13.53","es");
		
		//创建索引
		//createIndexWithMapping(client, "index2", "type");
		
		//查看所有索引
		//listIndex(client);
		
		//索引一条数据
		//Map<String,String> source = new HashMap<String,String>();
		//source.put("name", "李四");
		//source.put("age", "20");		
		//insert(client, "index2", "type", source);
		
		//查询
		//search(client, null, "index2", "type", 10);
		
		//更新
		//Map<String,String> newMap = new HashMap<String,String>();
		//newMap.put("age","21");//将age更新为21
		//updateWithScript(client, "index2", "type", "AWSNI3QNRpDTLBoqCD98", newMap);
		
		//最大值、最小值、和、平均值
		//metricsAggregations(client, "index2", "type", "age");
		
		//分组统计
		//termsAggregation(client, "index2", "type", "age", 10);
		
		//先分组，后求和
		//sumByGroup(client, "index2", "type");
		
		//删除索引
		//deleteIndex(client,"index2");
	}
	
	/**先分桶，后求和
	 * @param client
	 * @param index
	 * @param type
	 */
	private static void sumByGroup(TransportClient client, String index, String type) {
		TermsAggregationBuilder aggregation = 
				//指定分桶字段
				AggregationBuilders.terms("aggName1").field("field1").size(2000)
				//指定桶内求和字段
				.subAggregation(AggregationBuilders.sum("aggName2").field("field2"));
		SearchResponse sr = client.prepareSearch().setIndices(index).setTypes(type)
				// .setQuery( /* your query */ )
				.addAggregation(aggregation).execute().actionGet();
		Terms agg1 = sr.getAggregations().get("aggName1");
		for(Bucket bucket:agg1.getBuckets()) {
			String term = bucket.getKeyAsString();
			Sum sum = bucket.getAggregations().get("aggName2");
			Double sumDouble = sum.getValue();
			System.out.println("桶： "+term+"，和："+sumDouble);
		}
	}

	/**创建TranportClient
	 * @param esips es节点ip
	 * @param clusterName es集群名称
	 * @return
	 * @throws UnknownHostException
	 */
	public static TransportClient getClientWithNoXpack(String esips, String clusterName) throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true)// 自动把集群下的机器添加到列表中
				.build();
		TransportClient client = new PreBuiltTransportClient(settings);
		String[] ips = esips.split(",");
		for (String ip : ips) {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
		}
		return client;
	}
	
	/**创建索引并指定Mapping
	 * @param client 客户端
	 * @param index  索引
	 * @param type  类型
	 */
	public static void createIndexWithMapping(TransportClient client, String index, String type) {
		try {
			XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties")
					.startObject("name").field("type", "keyword").endObject()
					.startObject("age").field("type", "integer").endObject().
					//.endObject().startObject("born").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss")
					endObject().endObject();
			CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate(index);
			CreateIndexResponse response = prepareCreate.addMapping(type, mapping).execute().actionGet();
			System.out.println(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 查看所有索引
	 * 
	 * @throws UnknownHostException
	 */
	public static void listIndex(TransportClient client) {
		ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
		// 获取所有索引
		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		// response.getState().getMetaData().
		System.out.println(response.getClusterName());
		for (String index : indexs) {
			System.out.println(index);
		}
	}
	
	/**索引一条数据
	 * @param client 客户端
	 * @param index  索引
	 * @param type  类型
	 * @param source 数据
	 */
	public static void insert(TransportClient client, String index, String type, Map source) {
		IndexResponse response = client.prepareIndex(index, type).setSource(source).get();
		//response.getId()
		System.out.println(response.toString());
	}
	
	/**查询
	 * @param client
	 * @param map 查询条件，为null时全查
	 * @param index 索引
	 * @param type 类型
	 * @param size 查询记录数
	 * @return
	 */
	public static List<Map<String, Object>> search(TransportClient client, Map<String, String> map, String index,
			String type, int size) {
		List<Map<String, Object>> lm = new ArrayList<Map<String, Object>>();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		if (map != null) {
			Set<String> set = map.keySet();
			for (String str : set) {
				boolQueryBuilder.filter(QueryBuilders.termQuery(str, map.get(str)));
			}
		}

		SearchResponse response = client.prepareSearch().setIndices(index).setTypes(type).setSize(size)
				//.setFetchSource(includesArr, excludesArr)//includesArr可以指定返回哪些字段		
				.setQuery(boolQueryBuilder).get();
		System.out.println(response);
		SearchHits hits = response.getHits();
		long count = hits.getTotalHits();
		SearchHit[] hit = hits.getHits();
		for (int i = 0; i < hit.length; i++) {
			String id = hit[i].getId();
			lm.add(hit[i].getSource());
			System.out.println(hit[i].getSource());
		}
		return lm;
	}
	
	/**更新
	 * @param client 客户端
	 * @param index 索引
	 * @param type 类型
	 * @param id 文档主键
	 * @param map 跟新数据
	 */
	public static void updateWithScript(TransportClient client, String index, String type, String id,
			Map<String, String> map) {
		UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, id);
		for (String fieldName : map.keySet()) {
			updateRequestBuilder.setScript(new Script("ctx._source." + fieldName + "= \"" + map.get(fieldName) + "\""));
		}
		UpdateResponse response = updateRequestBuilder.get();
		System.out.println(response);
	}
	
	/**度量聚合（最小值、最大值、求和、平均值）
	 * @param client 客户端
	 * @param index 索引
	 * @param type 类型
	 * @param field 统计字段
	 */
	public static void metricsAggregations(TransportClient client, String index, String type, String field) {
		MinAggregationBuilder minAggregation = AggregationBuilders.min("min").field(field);
		MaxAggregationBuilder maxAggregation = AggregationBuilders.max("max").field(field);
		SumAggregationBuilder sumAggregation = AggregationBuilders.sum("sum").field(field);
		AvgAggregationBuilder avgAggregation = AggregationBuilders.avg("avg").field(field);
		SearchResponse sr = client.prepareSearch().setIndices(index).setTypes(type)
				// .setQuery( /* your query */ )可以基于查询结果进行聚合
				.addAggregation(minAggregation).addAggregation(maxAggregation).addAggregation(sumAggregation)
				.addAggregation(avgAggregation).execute().actionGet();
		Min minAgg = sr.getAggregations().get("min");
		Max maxAgg = sr.getAggregations().get("max");
		Sum sumAgg = sr.getAggregations().get("sum");
		Avg avgAgg = sr.getAggregations().get("avg");
		double minValue = minAgg.getValue();
		double maxValue = maxAgg.getValue();
		double sumValue = sumAgg.getValue();
		double avgValue = avgAgg.getValue();
		SearchHits hits = sr.getHits();
		SearchHit[] hit = hits.getHits();
		for (int i = 0; i < hit.length; i++) {
			// lm.add(hit[i].getSource());
			System.out.println(hit[i].getSource());
		}
		System.out.println("最小值: " + minValue);
		System.out.println("最大值: " + maxValue);
		System.out.println("和: " + sumValue);
		System.out.println("平均值: " + avgValue);
	}
	
	/**分组计数
	 * @param client 客户端
	 * @param index  索引
	 * @param type   类型
	 * @param field  用于判断分组的字段
	 * @param topSize 返回结果数
	 * @return
	 */
	public static Map<String, Long> termsAggregation(TransportClient client, String index, String type, String field,int topSize) {
		Map<String, Long> result = new HashMap<String, Long>();
		AggregationBuilder aggregation = AggregationBuilders.terms("aggName").field(field).size(topSize);
		SearchResponse sr = client.prepareSearch().setIndices(index).setTypes(type)
				// .setQuery( /* your query */ )
				.addAggregation(aggregation).execute().actionGet();
		Terms terms = sr.getAggregations().get("aggName");
		int size = terms.getBuckets().size();
		// For each entry
		for (Terms.Bucket entry : terms.getBuckets()) {
			result.put(entry.getKeyAsString(), entry.getDocCount());
			System.out.println("term:" + entry.getKeyAsString() + ", Doc count" + entry.getDocCount());
		}
		System.out.println(field + "字段，分桶数：" + size);
		return result;
	}
	
	/**删除索引
	 * @param client 客户端
	 * @param index 索引
	 * @return
	 */
	public static boolean deleteIndex(TransportClient client, String index) {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).execute().actionGet();
		return response.isAcknowledged();
	}
}
