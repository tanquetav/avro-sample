package com.soujava.collabtime.avro;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class GenerateV1
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void generatev1() throws IOException {
        User user1 = User.newBuilder()
                .setName("George")
                .setFavoriteColor("blue")
                .setFavoriteNumber(3)
                .build();

        User user2 = User.newBuilder()
                .setName("Max")
                .setFavoriteColor("green")
                .setFavoriteNumber(4)
                .build();

        User user3 = User.newBuilder()
                .setName("Igor")
                .setFavoriteColor("grey")
                .setFavoriteNumber(10)
                .build();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("/tmp/users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

    }


    @Test
    public void readv1() throws IOException {
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("/tmp/users.avro"), userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
        }

        dataFileReader.close();

    }
}
