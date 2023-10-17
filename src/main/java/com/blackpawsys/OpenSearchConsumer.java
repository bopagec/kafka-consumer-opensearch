package com.blackpawsys;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        // create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create our Kafka Client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // gracefully shutting down the consumerg
        Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                kafkaConsumer.wakeup();
                try {
                    currentThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // we need to create the index on OpenSearch if it does not exist
        try(openSearchClient; kafkaConsumer){
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created.");
            }

            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true){
                // timeout set to 3000 millis, if no data this line will get blocked for 3000 millis, nothing to do with committing offsets here.
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCounts = consumerRecords.count();
                log.info("Received: " + recordCounts + " record(s)");

                // let's use BulkRequest to
                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : consumerRecords){
                    // send the record into OpenSearch
                    // make our consumer idempotent as per Delivery Semantics: At least Once.
                    // we should practice idempotency with more exercises.

                    // explain how idempotent works here...
                    /*
                    This consumer has following configs
                        auto.commit.interval.ms = 5000;
                        enable.auto.commit = true;
                    means, this consumer automatically commit the offsets in every 5 seconds when call .poll() method.
                    in our example if our consumer crashes in 4th seconds and it will miss a commit for all the messages that has been written to the OpenSearch database.
                    when the consumer comes back live, it will poll once again from the old offset and will process the same data.
                    in this way the same data will have their unique id which will be used to write on OpenSearch database.
                    OpenSearch database is clever enough to overwrite the old data with the new data without duplicating a new record.
                    * */
                    String id = extractId(record.value());

                    try{
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id); // if it's duplicate, elastic search will update the one with id.

                       // IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);

                       // log.info("Inserted 1 document into OpenSearch id: " + indexResponse.getId());
                    }catch (Exception e){

                    }

                }

                if(bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s)");
                    Thread.sleep(1000);
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is starting to shutdown gracefully...");
        }
        catch (Exception e) {
            log.info("Exception occurred!", e.getMessage());

        } finally {
            kafkaConsumer.close();
            log.info("Gracefully closed the Kafka-OpenSearch-Consumer");
        }

    }

    private static String extractId(String record) {
        return JsonParser.parseString(record)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);

    }

    public static RestHighLevelClient createOpenSearchClient(){
        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();

        if(userInfo == null){
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }

        return restHighLevelClient;
    }
}

