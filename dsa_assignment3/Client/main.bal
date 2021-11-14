import ballerinax/kafka;
import ballerina/io;
import ballerina/lang.value;

#NB Create and edit topics
string DEFAULT_URL = "localhost: 9092";
string [] topics = ["writeFile", "requestFileWithKey"];// change

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",
    offsetReset: "earliest",
    topics: ["readFileResponse"] //change

};
kafka:Consumer consumer = check new (kafka:DEFAULT_URL, consumerConfiguration);

public function main() {

    io:println("***Account Type***");
    io:println("1. HOD");
    io:println("2. Lecturer");
    io:println("3. Learner");
    io:println("\n");
    string option = io:readln("Select an option");

    match option{
        "1"=>{
            error? w = HODPriv();
            }
        "2"=>{
            error? r = LecturerPriv();
        }
        "3"=>{
            error? l = LearnerPriv();
        }
    }
}

public function HODPriv()returns error?{
    io:println("**********Select Operation************");
    io:println("1. Edit Course Outline");
    io:println("2. View Course Outlines");
    io:println("3. Approve Course Outline");
    string option = io:readln("Select an option");

    match option{
        "1"=>{
            error? e = createCO();
        }
        "2"=>{
            error? v = viewCO();
        }
        "3"=>{
            error? e = approveCO();
        }
    }
}

public function LecturerPriv()returns error?{
    io:println("**********Select Operation************");
    io:println("1. Edit Course Outline");
    io:println("2. View Course Outlines");
    io:println("3. Create new course Outline");
    string option = io:readln("Select an option");

    match option{
        "1"=>{
            error? e = createCO();
        }
        "2"=>{
            error? v = viewCO();
        }
        "3"=>{
            error? c = createCO();
        }
    }
}

public function LearnerPriv()returns error?{
    io:println("**********Select Operation************");
    io:println("1. View Course Outlines");
    io:println("2. Acknowledge Course Outline");
    string option = io:readln("Select an option");

    match option{
        "1"=>{
            error? v = viewCO();
        }
        "2"=>{
            error? a = acknowledge();
        }
    }
}

public function createCO()returns error?{
    string message = "Creating new course outline";
       string courseCode = io:readln("Enter Course Code: ");
       string courseTitle = io:readln("Enter Course Name: ");
       string lectDetails = io:readln("Enter Lecturer Details: ");
       string courseCont = io:readln("Enter course content: ");
       string assInfo = io:readln("Enter Assessment information: ");
       string isSigned = "true";
       string isApproved ="False";
       string acknowledged = "False";
       
       json fileObject = {courseCode, courseTitle, lectDetails, courseCont, assInfo, isSigned, isApproved, acknowledged};
       io:println(fileObject);

       check kafkaProducer->send({
                               topic: topics[0],
                               value: fileObject.toJsonString().toBytes() });

       check kafkaProducer->'flush();
       io:println("\n");
       main();
}

//public function editCO()returns error?{
//   error? v = viewCO();
//   error? c = createCO();
//}


public function approveCO()returns error?{
    string message = "Approve course outline";
    string isApproved = "True";
}

public function acknowledge()returns error?{
    string message = "Acknowledge course outline";
    string acknowledge = "True";
}

public function viewCO()returns error?{
       string courseCode = io:readln("Enter Course Code name: ");

       check kafkaProducer->send({
                                    topic: topics[1],
                                    value: courseCode.toBytes()
                                    });
        check kafkaProducer->'flush();



kafka:ConsumerRecord[] records = check consumer->poll(1);
int i=0;
foreach var kafkaRecord in records {
    byte[] messageContent = kafkaRecord.value;
    string message = check string:fromBytes(messageContent);
    io:println("Kafka ",i+1);

    json fileObject = check value:fromJsonString(message);
    io:println("\n\n\n");
    if(fileObject.courseCode == courseCode){
        io:println("Object recieved: ",fileObject);

        break;
    }
}
       main();
}