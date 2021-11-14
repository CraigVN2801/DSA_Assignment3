import ballerinax/kafka;
import ballerina/log;
import ballerina/io;
import ballerina/lang.value;

kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "group-id",
    // Subscribes to the topic `test-kafka-topic`.
    topics: ["writeFile", "requestFileWithKey"],

    pollingInterval: 1,
    // Sets the `autoCommit` to `false` so that the records should be committed manually.
    autoCommit: false
};

string DEFAULT_URL = "localhost:9092";

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

listener kafka:Listener kafkaListener =
        new (kafka:DEFAULT_URL, consumerConfigs);

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:ConsumerRecord[] records) returns error? {
        // The set of Kafka records received by the service are processed one by one.
        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
        }

        // Commits offsets of the returned records by marking them as consumed.
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the " +
                "offsets for the consumer ", 'error = commitResult);
        }
    }
}

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) returns error? {
    // The value should be a `byte[]` since the byte[] deserializer is used
    // for the value.
    io:println("Timestamp ", kafkaRecord.timestamp);
    io:println("Topic ", kafkaRecord.offset.partition.topic);
    //io:println("Timestamp ", kafkaRecord);

    byte[] value = kafkaRecord.value;

    // Converts the `byte[]` to a `string`.
    string messageContent = check string:fromBytes(value);

    if(kafkaRecord.offset.partition.topic=="writeFile"){

        log:printInfo("Recieve file object: "+messageContent);

        json fileObject = check value:fromJsonString(messageContent);

        if(fileObject is json){
            string path = check fileObject.courseCode;
            
            string jsonFilePath = "./Service/CourseOutline/"+path+".json";
            check io :fileWriteJson(jsonFilePath, fileObject);
    }
}
if(kafkaRecord.offset.partition.topic=="requestFileWithKey"){
            string filepath = "./Service/CourseOutline/"+messageContent+".json";
            
            io:println("**********************************************\n\n\n");
            io:println("Message Content: ", filepath);
            
            json readJson = check io:fileReadJson(filepath);
            io:println("**********************************************\n\n\n");
            io:println("View Course Outline: ",readJson);
            io:println("**********************************************\n\n\n");
            check kafkaProducer->send({
                topic: "readFileResponse",
                value: readJson.toJsonString().toBytes()
            });

            check kafkaProducer->'flush();
    }
}