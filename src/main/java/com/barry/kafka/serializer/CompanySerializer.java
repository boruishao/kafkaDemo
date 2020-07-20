package com.barry.kafka.serializer;

import com.barry.kafka.bean.Company;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author boruiShao
 * Date 2020/7/18 3:02 PM
 * Version 1.0
 * Describe TODO
 **/

public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if(data == null ){
            return null;
        }
        byte[] name,address ;
        try {
            if (data.getName()!=null){
                name = data.getName().getBytes("UTF-8");
            }else {
                name = new byte[0];
            }

            if (data.getAddress()!=null){
                address = data.getAddress().getBytes("UTF-8");
            }else {
                address = new byte[0];
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Company data) {
        return serialize(topic,data);

    }

    @Override
    public void close() {

    }
}
