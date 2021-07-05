package com.redislabs.redconn;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;
import java.util.Calendar;
//javac src/main/java/com/redislabs/redconn/DNSTest.java
//java -classpath ./src/main/java com.redislabs.redconn.DNSTest redis-12000.internal.k1.demo.redislabs.com
public class DNSTest {
    private static final String DNS_CACHE_TTL = "networkaddress.cache.ttl";
    private static final String DNS_CACHE_NEGATIVE_TTL = "networkaddress.cache.negative.ttl";

    public static void main(String[] args) {
        String hostname = args[0];
        Security.setProperty(DNS_CACHE_NEGATIVE_TTL, "0");
        Security.setProperty(DNS_CACHE_TTL, "0");

       while(true) {
           try {
               System.out.println(java.time.LocalDateTime.now() + " : " + getHostAddress(hostname));
               Thread.sleep(1000);
           } catch (Exception e) {
               //ignore
           }
       }

    }

    private static String getHostAddress(String host) {
        try {

            InetAddress inetHost = InetAddress.getByName(host);
            return inetHost.getHostAddress();
        } catch (UnknownHostException ex) {
            return "Unknown Host";
        }
    }
}
