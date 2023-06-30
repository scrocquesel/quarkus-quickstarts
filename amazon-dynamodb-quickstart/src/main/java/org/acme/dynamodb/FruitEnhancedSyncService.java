package org.acme.dynamodb;

import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@ApplicationScoped
public class FruitEnhancedSyncService extends AbstractService {

    private DynamoDbTable<Fruit> fruitTable;

    @Inject
    FruitEnhancedSyncService(DynamoDbEnhancedClient dynamoEnhancedClient) {
        fruitTable = dynamoEnhancedClient.table(FRUIT_TABLE_NAME,
            TableSchema.fromClass(Fruit.class));
    }

    public List<Fruit> findAll() {
        return fruitTable.scan().items().stream().collect(Collectors.toList());
    }

    public List<Fruit> add(Fruit fruit) {
        fruitTable.putItem(fruit);
        return findAll();
    }

    public Fruit get(String name) {
        Key partitionKey = Key.builder().partitionValue(name).build();
        return fruitTable.getItem(partitionKey);
    }
}