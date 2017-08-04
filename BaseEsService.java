package com.elasticsearch.es.service;

import com.alibaba.fastjson.JSONObject;
import com.elasticsearch.common.config.Config;
import com.elasticsearch.common.exception.BEException;
import com.elasticsearch.es.dto.BaseEsDto;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoDistanceRangeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.geoDistanceRangeQuery;

/**
 * @Author: Vachel Wang
 * @Date: 2017/6/1下午3:54
 * @Version: V1.0
 * @Description:
 */
public class BaseEsService {

    private static final Logger LOG = LoggerFactory.getLogger(BaseEsService.class);
    // 类型
    private String type ;
    // 索引
    private String index ;

    public BaseEsService(String index , String type){
        this.type = type ;
        this.index = index ;
    }

    /**
     * 获取客户端连接，获取后注意关闭
     * @return
     */
    protected Client getClient(){
//        LOG.info("Config.ES_HOST = {} , Config.ES_PORT = {}",Config.ES_HOST,Config.ES_PORT);
        Client client = null;
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Config.ES_HOST), Config.ES_PORT));
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage(),e);
            throw BEException.me("获取ES链接失败");
        }
        return client ;
    }

    /**
     * 索引一条数据
     * @param baseEsDto
     */
    public String indexOne(BaseEsDto baseEsDto){
        Client client = getClient() ;
        IndexResponse response = client.prepareIndex(index, type, baseEsDto.getId()==null?null:baseEsDto.getId())
                .setSource(baseEsDto.toJsonString())
                .get();

        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();
        LOG.info("_index：" + _index + ",_type：" + _type + ",_id：" + _id + ",created：" + created);
        client.close();
       return _id ;
    }

    /**
     * 索引多条数据，id可程序指定，为空时由ES自动生成
     * @param baseEsDtoList
     */
    public <T extends BaseEsDto> void indexList(List<T> baseEsDtoList){
        Client client = getClient() ;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(BaseEsDto baseEsDto:baseEsDtoList) {
            System.out.println(baseEsDto.toJsonString());
            bulkRequest.add(client.prepareIndex(index, type,baseEsDto.getId()).setSource(baseEsDto.toJsonString()));
        }
        bulkRequest.execute().actionGet();
        client.close();
    }


    /**
     * 根据id获取
     * @param id
     * @param tClass
     */
    public <T extends BaseEsDto>T getById(String id,Class<T> tClass){
        Client client = getClient() ;
        GetResponse response = client.prepareGet(index, type, id).get();
        T t = JSONObject.parseObject(response.getSourceAsString(), tClass);
        client.close();
        return t;
    }

    /**
     * 查询所有
     */
    protected SearchHit[] queryAll(QueryBuilder qb){
        Client client = getClient() ;
        SearchResponse searchResponse = client.prepareSearch().setIndices(index).setTypes(type).setQuery(qb).setSize(10000).execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        client.close();
        return hits;
    }
    /**
     * 查询所有并排序
     */
    protected SearchHit[] queryAllAndSort(QueryBuilder qb,List<SortBuilder> sortBuilderList){
        Client client = getClient() ;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(qb).setSize(10000);
        if(null!=sortBuilderList){
            for(SortBuilder sortBuilder: sortBuilderList){
                searchRequestBuilder.addSort(sortBuilder);
            }
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        client.close();
        return hits;
    }
    /**
     * 聚合查询
     */
    protected Aggregations aggregationQuery(QueryBuilder qb, AggregationBuilder aggregationBuilder){
        Client client = getClient() ;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(qb).addAggregation(aggregationBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        Aggregations aggregations = searchResponse.getAggregations();
        client.close();
        return aggregations;
    }

    /**
     * 分页查询并排序
     * @param qb 查询对象
     * @param sortBuilderList 排序
     * @param from 开始坐标
     * @param size 分页大小
     * @return
     */
    protected SearchHits pageQueryAndSort(QueryBuilder qb, List<SortBuilder> sortBuilderList, int from , int size){
        Client client = getClient() ;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(qb).setFrom(from).setSize(size);
        if(null!=sortBuilderList){
            for(SortBuilder sortBuilder: sortBuilderList){
                searchRequestBuilder.addSort(sortBuilder);
            }
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        client.close();
        return hits;
    }

    /**
     * 根据id获取多条数据
     * @param tClass
     */
    protected <T extends BaseEsDto>List<T> getByIds(Class<T> tClass,String ... ids){
        Client client = getClient() ;
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add(index, type, ids)
                .get();
        List<T> resultList = new ArrayList<>() ;
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                T t = JSONObject.parseObject(response.getSourceAsString(), tClass);
                resultList.add(t);
            }
        }
        client.close();
        return resultList;
    }

    /**
     * 根据id删除单条数据
     * @param id
     * @return
     */
    public boolean deleteById(String id){
        Client client = getClient() ;
        DeleteResponse response = client.prepareDelete(index, type, id).get();
        client.close();
        return response.getShardInfo().getSuccessful()==1 ;
    }
    /**
     * 根据id删除单条数据
     * @param index
     * @param type
     * @return
     */
    protected boolean deleteAll(String index,String type){
        Client client = getClient() ;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setExplain(true).execute().actionGet();
        for(SearchHit hit : response.getHits()){
            String id = hit.getId();
            bulkRequest.add(client.prepareDelete(index, type, id).request());
        }
        BulkResponse bulkResponse = bulkRequest.get();
        client.close();
        return bulkResponse.hasFailures()==true?false:true;
    }


    /**
     * 通过id更新
     * @param id
     * @param baseEsDto
     * @return
     */
    public boolean updateById(String id,BaseEsDto baseEsDto){
        Client client = getClient() ;
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(baseEsDto.toJsonString());
        boolean success = true ;
        try {
            UpdateResponse updateResponse = client.update(updateRequest).get();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(),e);
            throw BEException.me("ES执行更新失败");
        } catch (ExecutionException e) {
            LOG.error(e.getMessage(),e);
            throw BEException.me("ES执行更新失败");
        }finally {
            client.close();
        }
        return success ;
    }

    /**
     * 根据给定的坐标和距离查询附近的信息
     * @param geoField 字段
     * @param lon 经度
     * @param lat 纬度
     * @param distance 距离
     * @param query 查询
     * @return
     */
    protected SearchHit[] listGeoByPoint(String geoField , Double lon,Double lat,Double distance,QueryBuilder query){
        Client client = getClient() ;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(query).setMinScore(0.1f).setSize(10000);
        GeoDistanceRangeQueryBuilder qb = null;
        GeoDistanceSortBuilder sort = null ;
        if(lon!=null && lat!=null) {
            qb = geoDistanceRangeQuery(geoField)
                    .point(lat, lon)
                    .includeLower(true)
                    .includeUpper(false)
                    .optimizeBbox("memory")
                    .geoDistance(GeoDistance.ARC);
            if(null!=distance){
                qb.from("0km").to(distance + "km");
            }

            sort = new GeoDistanceSortBuilder(geoField);
            sort.unit(DistanceUnit.KILOMETERS);
            sort.order(SortOrder.ASC);
            sort.point(lat, lon);
            sort.geoDistance(GeoDistance.ARC);
            searchRequestBuilder.setPostFilter(qb);
            searchRequestBuilder.addSort(sort);
        }

        SearchResponse response = searchRequestBuilder.execute().actionGet();

        SearchHit[] hits = response.getHits().getHits();
        client.close();
        return hits ;

    }

}
