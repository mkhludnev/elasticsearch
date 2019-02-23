package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class DocValuesSearchTest extends ESSingleNodeTestCase {
    
    @SuppressWarnings("unchecked")
    public void testCopyToFieldsParsing() throws Exception {
        
        client().admin().indices()
        .prepareDeleteTemplate("random-soft-deletes-template").get();
        
        client().admin().indices()
        .preparePutTemplate("random-soft-deletes-template")
        .setPatterns(Collections.singletonList("*"))
        .setOrder(0)
        .setSettings(Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)
        ).get();
        
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type1")
                //.field("dynamic","strict")
                .startObject("properties")
                    /*
                    .startObject("dv_field")
                    .field("type", "integer")
                    .field("doc_values", true)
                    .field("index", false)
                    .field("store", false)
                    .endObject()
                    */
                    .startObject("name")
                    .field("type", "text")
                    .field("doc_values", false)
                    .field("index", true)
                    .field("store", true)
                    .endObject()
               .endObject()
               .startObject("_source")
                  .field("enabled", false)
               .endObject()
               .startArray("dynamic_templates")
                  .startObject()
                   .startObject("subscr_columns")
                        .field("match", "dv_*")
                             .startObject("mapping")
                                 .field("type", "integer")
                                 .field("index", false)
                                 .field("store", false)
                                 .field("doc_values", true)
                             .endObject()
                   .endObject()
                  .endObject()  
               .endArray()
         .endObject().endObject());

        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test")
                .setType("type1").setSource(mapping, XContentType.JSON).get();
        
        client().prepareIndex("test", "type1", "0").setSource(
                XContentFactory.jsonBuilder().startObject().field("dv_field", 100).field("name", "one hundred").endObject()
                ).execute().actionGet();
        client().prepareIndex("test", "type1", "1").setSource(
                XContentFactory.jsonBuilder().startObject().field("dv_field", 200).field("name", "two hundred").endObject()
                ).execute().actionGet();
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().actionGet();
        assertThat(refreshResponse.getSuccessfulShards(), greaterThanOrEqualTo(1)); // at least one shard should be successful when refreshing
        
        int numDocs = 0;
        int numSegs = 0;
        {
          final IndicesSegmentResponse segments = client().admin().indices().prepareSegments("test").execute().actionGet();
          for(org.elasticsearch.action.admin.indices.segments.ShardSegments sgmnts: 
              segments.getIndices().get("test").getShards().values().iterator().next().getShards()) {
              final List<Segment> segments2 = sgmnts.getSegments();
              System.out.print(segments2);
          }
          assertEquals(1, segments.getIndices().get("test").getShards().values().iterator().next().getShards().length);
          for(Segment seg : segments.getIndices().get("test").getShards().values().iterator().next().getShards()[0].getSegments()) {
              numDocs += seg.getNumDocs();
              numSegs ++;
          }
        }
        
        {
        SearchResponse rsp = client().prepareSearch("test").setQuery(
                QueryBuilders.rangeQuery("dv_field").from(100).to(100)
                ).get();
        
        assertThat(rsp.getHits().getTotalHits().value, equalTo(1L));
        assertThat(rsp.getHits().getAt(0).getId(), equalTo("0"));
        }
        {
            SearchResponse rsp = client().prepareSearch("test").setQuery(
                    QueryBuilders.rangeQuery("dv_field").from(200).to(200)
                    ).get();
            
            assertThat(rsp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(rsp.getHits().getAt(0).getId(), equalTo("1"));
        }
        {
            GetFieldMappingsResponse fieldMappings = client().admin().indices()
                    .prepareGetFieldMappings("test").setTypes("type1").setFields("dv_field").get();
            Object map = fieldMappings.fieldMappings("test", "type1", "dv_field").sourceAsMap()
                    .get("dv_field");
            assertFalse((boolean) ((Map<String,Object>) map).get("index"));
        }
        
        {
           // final UpdateResponse updRsp =
                    /*client().prepareUpdate("test", "type1", "0").setDoc(
                    XContentFactory.jsonBuilder().startObject().
                        field("dv_field", 1009)
                    .endObject()
            ).execute().actionGet();
            
                        assertEquals(""+updRsp.getResult()+
                    ": "+updRsp,200, updRsp.status().getStatus());

            */
            
            final BulkResponse updRsp = client().prepareBulk().add(
                    client().prepareUpdate("test", "type1", "0").setDoc(
                            XContentFactory.jsonBuilder().startObject().
                            field("dv_field", 1009)
                        .endObject()
                )
            ).execute().actionGet();
            assertEquals(updRsp.getItems()[0].getFailureMessage(),200, updRsp.status().getStatus());
             
            {
                client().admin().indices().refresh(new RefreshRequest("test")).get();
            }

            {
                final IndicesSegmentResponse segments = client().admin().indices().prepareSegments("test").execute().actionGet();
                for(org.elasticsearch.action.admin.indices.segments.ShardSegments sgmnts: 
                    segments.getIndices().get("test").getShards().values().iterator().next().getShards()) {
                    final List<Segment> segments2 = sgmnts.getSegments();
                    System.out.print(segments2);
                }
                assertEquals(1, segments.getIndices().get("test").getShards().values().iterator().next().getShards().length);
                int docs=0;
                int segs=0;
                for(Segment seg : segments.getIndices().get("test").getShards().values().iterator().next().getShards()[0].getSegments()) {
                    docs += seg.getNumDocs();
                    segs ++;
                }
                assertEquals(numDocs, docs);
                assertEquals(numSegs, segs);
            }
            {
            SearchResponse rsp = client().prepareSearch("test").setQuery(
                        QueryBuilders.rangeQuery("dv_field").from(1009).to(1009)
                        ).get();
                
            assertThat(rsp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(rsp.getHits().getAt(0).getId(), equalTo("0"));
            }
            
            /*{
                SearchResponse rsp = client().prepareSearch("test").setQuery(
                        QueryBuilders.termQuery("dv_field",1000)
                        ).get();
                
                assertThat(rsp.getHits().getTotalHits().value, equalTo(1L));
                assertThat(rsp.getHits().getAt(0).getId(), equalTo("0"));
            }*/

        }
    }
    
    /**
     * InternalEngine$AssertingIndexWriter(IndexWriter).updateDocument(Node<?>, Iterable<IndexableField>) line: 1591    

     * */
}
