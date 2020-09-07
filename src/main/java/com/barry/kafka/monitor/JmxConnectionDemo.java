package com.barry.kafka.monitor;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/9/2 9:08 AM
 * Version 1.0
 * Describe 通过jmx获取kafka的监控指标，前提是kafka要开启jmx监控端口
 *  JMX_PORT=9999 bin/kafka-server-start.sh -daemon config/server0.properties
 *  通过jconsole，找到MBEAN，通过不同的ObjName和attribute即可查看不同的指标，这里列举了
 *      OneMinuteRate 这个属性
 **/

public class JmxConnectionDemo {
    private MBeanServerConnection conn;
    private String jmxUrl;
    private String ipAndPort;

    public JmxConnectionDemo(String ipAndPort) {
        this.ipAndPort = ipAndPort;
    }

    public boolean init() {
        //注意这个协议的有三条线
        jmxUrl = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        try {
            JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);
            JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);
            conn = connector.getMBeanServerConnection();
            if (conn == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public double getMsgInPerSec() {
//        String objectName = "kafka.controller:type=KafkaController," + "name=ActiveControllerCount";
        String objectName = "kafka.server:type=BrokerTopicMetrics," + "name=MessagesInPerSec";
//        Object val = getAttribute(objectName, "Value");
        Object val = getAttribute(objectName, "OneMinuteRate");
        if (val != null) {
            return (double) (Double) val;
        }
        return 0.0;
    }

    private Object getAttribute(String objName, String objAttr) {
        ObjectName objectName;

        try {
            objectName = new ObjectName(objName);
            return conn.getAttribute(objectName, objAttr);
        } catch (MBeanException | AttributeNotFoundException |
                InstanceNotFoundException | ReflectionException
                | IOException | MalformedObjectNameException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) {
        JmxConnectionDemo jmxConnectionDemo = new JmxConnectionDemo("localhost:9999");
        jmxConnectionDemo.init();
        System.out.println(jmxConnectionDemo.getMsgInPerSec());
    }
}
