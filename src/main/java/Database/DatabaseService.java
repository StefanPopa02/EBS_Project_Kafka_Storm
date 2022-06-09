package Database;

import java.util.List;

public class DatabaseService {
    private static DatabaseService instance;
    private MyMongoClient myMongoClient;
    private DatabaseService(){
        myMongoClient = new MyMongoClient();
    }

    public static DatabaseService getInstance() {
        if(instance == null){
            instance = new DatabaseService();
        }
        return instance;
    }

    public void addSub(String source, String sub){
        myMongoClient.storeSubscription(source, sub);
    }

    public List<RoutingTableEntry> getAllEntries(){
        return myMongoClient.getAllEntries();
    }
}
