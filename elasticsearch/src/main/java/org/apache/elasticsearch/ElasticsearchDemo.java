package org.apache.elasticsearch;


import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/11/30.
 *
 * Elasticsearch-API
 *
 * ES的操作分为两类:
 *      1、索引库的操作
 *          client.admin().indices().prepareXXX()
 *      2、索引数据的操作
 *          client.prepareXXX()
 *
 * 爬区数据命令:  wget.log需要提前存在
 *  wget -o /tmp/wget.log -P /root/data --no-parent --no-verbose -m -D www.cctv.com,news.cctv.com -N --convert-links --random-wait -A shtml,SHTML http://news.cctv.com
 */
public class ElasticsearchDemo {

    private TransportClient client = null;

    @Before
    public void init() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "mycluster").build();


        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node01"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node02"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node03"), 9300));

        System.out.println("client:"+ client);

    }


    /**
     * 创建索引库
     * @throws UnknownHostException
     */
    @Test
    public void createIndexRepository() throws UnknownHostException {
        HashMap<String,Object> set = new HashMap<>();
        set.put("number_of_shards",4);


        IndicesExistsResponse mycluster = client.admin().indices().prepareExists("mycluster").execute().actionGet();

        //如果索引库存在,就删除
        if(mycluster.isExists()){
            client.admin().indices().prepareDelete("mycluster").execute();
        }

        client.admin().indices().prepareCreate("mycluster").setSettings(set).execute();
        System.out.println("创建索引库成功！");
    }


    @Test
    public void createDocument() throws UnknownHostException {
        HashMap<String,Object> document = new HashMap<>();
        document.put("name","神奇女侠");
        document.put("desc","美国大片");
        document.put("time",120);

        IndexResponse indexResponse = client.prepareIndex("mycluster", "video").setSource(document).execute().actionGet();

        System.out.println(indexResponse.getId());

    }


    @Test
    public void createIndex() throws Exception {


        IndicesExistsResponse mycluster = client.admin().indices().prepareExists("mycluster").execute().actionGet();

        //如果索引库存在,就删除
        if(mycluster.isExists()){
            client.admin().indices().prepareDelete("mycluster").execute();
        }

        client.admin().indices().prepareCreate("mycluster").execute();

        new XContentFactory();

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("htmlbean").startObject("propertise")
                .startObject("title").field("type", "string")
                .field("store", "yes").field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word").endObject()
                .startObject("content").field("type", "string")
                .field("store", "yes").field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word").endObject()
                .endObject().endObject().endObject();

        PutMappingRequest mapping = Requests.putMappingRequest("mycluster").type("htmlbean").source(builder);
        client.admin().indices().putMapping(mapping).actionGet();
    }

    @Test
    public void writeIndex(){
        readData(new File(ExtractText.DATA_DIR));
    }


    public void readData(File file){
        if(file.isDirectory()){
            for (File f : file.listFiles()) {
                readData(f);
            }
        }else{
            try {
                Map<String, String> htmlData = ExtractText.parserHtml(file);

                client.prepareIndex("mycluster", "htmlbean").setSource(htmlData).execute().actionGet();

            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }

        }

    }



    public void search(String keyword,int pageNum,int pagesize){

        MultiMatchQueryBuilder q = new MultiMatchQueryBuilder(keyword,new String[]{"title","content"});

        SearchResponse searchResponse = client.prepareSearch("mycluster")
                .setTypes("htmlbean")
                .setQuery(q)
                .setHighlighterPreTags("<font color=\"red\">")
                .setHighlighterPostTags("</font>")
                .addHighlightedField("title")
                .addHighlightedField("content")
                .setHighlighterFragmentSize(30)//结果中显示的片段最多包括多少个字符
                .setHighlighterNumOfFragments(3)//结果中显示的片段总数，最多为3
                .setFrom(0)//每页从第几个结果开始返回
                .setSize(10)
                .execute().actionGet();

        SearchHits hits = searchResponse.getHits();

        System.out.println("总共查询到--" + hits.getTotalHits() + "--个结果");

        int i = 1;
        for (SearchHit hit : hits) {

            HighlightField ht = hit.getHighlightFields().get("title");

            String title = ht == null ? hit.getSource().get("title").toString() : ht.toString();//如果title中没有包含keyword则返回空

            HighlightField hc = hit.getHighlightFields().get("content");

            String content = hc == null ? hit.getSource().get("content").toString() : hc.toString();

            String url = hit.getSource().get("url").toString();

            System.out.println(title);
            System.out.println(content);
            System.out.println(url);
            System.out.println("================="+ i +"==================");
            i++;
        }


    }

}
