import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.daslab.czy.Utils.MongoDBUtils.getDataFromMongoDB;

public class MongoDBUtilsTest {
    @Test
    public void testMongo(){
        List<Document> records = getDataFromMongoDB("localhost", "Chinanews", new String[]{"201901"});
        System.out.println(records.size());
        System.out.println(records.get(0));
    }
}
