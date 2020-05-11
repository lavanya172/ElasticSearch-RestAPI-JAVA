package Employee.Controller;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping(path = "/employees")
public class ESService {

    static RestHighLevelClient client;

    public static RestHighLevelClient configureElasticSearch(){

        if(client != null)  return client;

        final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("username", "password"));
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http")).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        client = new RestHighLevelClient(builder);

        return client;
    }

    public CreateIndexResponse CreateIndex(String reqName) throws IOException {

        //creating an index
        CreateIndexRequest request = new CreateIndexRequest(reqName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2)
        );
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("empId", message);
        properties.put("name", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);
        CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.index());
        return indexResponse;
    }

    @PostMapping(path= "/", consumes = "application/json", produces = "application/json")
    public ResponseEntity CreateEmployee( @RequestBody Employee emp) throws IOException {
        configureElasticSearch();
        String IndexName = "employee";
        IndexRequest request = new IndexRequest(IndexName);
        request.id(emp.getId());
        Map<String, Object> fields = new HashMap<>();
        fields.put("empId", emp.getId());
        fields.put("name", emp.getName());
        request.source(fields);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.getId());

        return new ResponseEntity(indexResponse, HttpStatus.CREATED);
    }

    @PutMapping(path= "/{id}", consumes = "application/json", produces = "application/json")
    public IndexResponse UpdateEmployee( @PathVariable("id") String Id , @RequestBody Employee emp) throws IOException {
        configureElasticSearch();
        String IndexName = "employee";
        IndexRequest request = new IndexRequest(IndexName);
        request.id(Id);
        Map<String, Object> fields = new HashMap<>();
        fields.put("empId", Id);
        fields.put("name", emp.getName());
        request.source(fields);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.getId());
        return indexResponse;
    }

    @DeleteMapping(path= "/{id}", produces = "application/json")
    public void DeleteEmployee( @PathVariable("id") String Id) throws IOException {
        String IndexName = "employee";
        DeleteRequest request = new DeleteRequest(IndexName,Id);
        DeleteResponse deleteResponse = client.delete(request,RequestOptions.DEFAULT);
    }

    @DeleteMapping(path= "/", produces = "application/json")
    public void DeleteAllEmployee() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("employee");
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        CreateIndex("employee");
    }

    public SearchResponse SearchDocument(String IndexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(IndexName);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        return searchResponse;
    }

    @GetMapping(path="/{id}", produces = "application/json")
    public Map<String, Object> GetEmployeeDetails(@PathVariable("id") String empId) throws IOException {
        configureElasticSearch();
        SearchRequest searchRequest = new SearchRequest("employee");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("empId", empId));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);

        SearchHit[] hits = searchResponse.getHits().getHits();

        if(hits.length >0)
        return  searchResponse.getHits().getHits()[0].getSourceAsMap();

        return null;
    }

    @GetMapping(path="/", produces = "application/json")
    public ResponseEntity GetAllEmployees() throws IOException {

//        @RequestHeader("Authorization") String Authorization
//        if(!ValidateUser(Authorization)){
//            return new ResponseEntity("Invalid User", HttpStatus.UNAUTHORIZED);
//        }

        configureElasticSearch();
        ESService ts = new ESService();
        SearchResponse searchResponse = ts.SearchDocument("employee");
        SearchHit[] hits = searchResponse.getHits().getHits();

        if(hits.length ==0)
            return null;
        Map<String, Object>[] map = new HashMap[hits.length];
        for(int i=0; i<hits.length; i++){
            map[i] = searchResponse.getHits().getHits()[i].getSourceAsMap();
        }
        return new ResponseEntity(map, HttpStatus.OK);
    }


    private Boolean ValidateUser(String authorization){

        if (authorization != null && authorization.toLowerCase().startsWith("basic")) {
            // Authorization: Basic base64credentials
            String base64Credentials = authorization.substring("Basic".length()).trim();
            byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
            String credentials = new String(credDecoded, StandardCharsets.UTF_8);
            // credentials = username:password
            final String[] values = credentials.split(":", 2);
            if(values[0].equals("lavanya") && values[1].equals("password") )
                return true;
        }

        return false;
    }
    public static void main(String[] args) throws IOException {
        ESService ts = new ESService();
        RestHighLevelClient client = configureElasticSearch();
        //ts.CreateIndex("employee");
        Employee emp = new Employee("104","TestEmployee");
        ts.CreateEmployee( emp );
        //ts.DeleteDocument("employee","101");
        //System.out.println(ts.GetEmployeeDetails("2001").getHits().getHits().toString());
       // ts.SearchDocument("employee");
        //System.out.println("Closing...");
        ts.DeleteAllEmployee();
        client.close();
    }
}
