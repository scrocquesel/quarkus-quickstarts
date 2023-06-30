package org.acme.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@ApplicationScoped
public class FruitEnhancedAsyncService extends AbstractService {

    @Inject
    private DynamoDbAsyncTable<Fruit> fruitTable;

    @Inject
    FruitEnhancedAsyncService(DynamoDbEnhancedAsyncClient dynamoEnhancedAsyncClient) {
        fruitTable = dynamoEnhancedAsyncClient.table(FRUIT_TABLE_NAME,
            TableSchema.fromClass(Fruit.class));
    }

    public Uni<List<Fruit>> findAll() {
        return Multi.createFrom().publisher(AdaptersToFlow.publisher(fruitTable.scan().items())).collect().asList();
    }

    public Uni<Void> add(Fruit fruit){
        return Uni.createFrom().completionStage(() -> fruitTable.putItem(fruit));
}

    public Uni<Fruit> get(String name) {
        Key partitionKey = Key.builder().partitionValue(name).build();
        return Uni.createFrom().completionStage(() -> fruitTable.getItem(partitionKey));
    }
}