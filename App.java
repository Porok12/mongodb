import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;

class App {
    public static void main(String[] args) throws ParseException {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("linie_lotnicze");

        System.out.println(database.getName());

        for (String name : database.listCollectionNames()) {
            System.out.println(name);
        }

        MongoCollection<Document> pilot = database.getCollection("Pasazer");

        MongoCursor<Document> cursor = pilot.find().cursor();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }


        System.out.println("========================--------------========================");
        MongoCollection<Document> pasazer1 = database.getCollection("Pasazer");

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1900);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        Date from = cal.getTime();

        cal.set(Calendar.YEAR, 2100);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        Date to = cal.getTime();

        FindIterable<Document> findIterable = pasazer1.find(
                and(
                        gt("data_urodzenia", from),
                        lt("data_urodzenia", to)
                )
        ).projection(
                fields(
                        include(
                                "imie", "nazwisko", "dataUrodzenia", "data_urodzenia"
                        ),
                        excludeId()
                )
        );

        MongoCursor<Document> cursor1 = findIterable.cursor();

        while (cursor1.hasNext()) {
            System.out.println(cursor1.next());
        }

        System.out.println("========================--------------========================");
        MongoCollection<Document> pasazer = database.getCollection("Pasazer");

        AggregateIterable<Document> aggregate = pasazer.aggregate(
                asList(
                        Aggregates.lookup("Bilet", "PasazerId", "ObjectId", "Bilet")
                )
        );

        cursor = aggregate.cursor();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("========================--------------========================");
        MongoCollection<Document> collection = database.getCollection("Lot");
        aggregate = collection.aggregate(
                Arrays.asList(
                        match(
                                exists("liniaLotnicza", true)
                        ),
                        group(
                                "$liniaLotnicza", Accumulators.sum("count", 1)
                        )
                )
        );


        cursor = aggregate.cursor();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }
}