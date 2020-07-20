package com.barry.kafka.serializer;

import com.barry.kafka.bean.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author boruiShao
 * Date 2020/7/19 7:06 PM
 * Version 1.0
 * Describe TODO
 **/

public class CompanyDeserialier implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received is shorter than expect.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name , address ;
        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }
        return new Company(name, address);
    }

    @Override
    public Company deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic,data);
    }

    @Override
    public void close() {

    }
}
