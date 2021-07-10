package com.example.learningjava;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class MainTest {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String str = "{\"track_no\":\"SF1324433376117\"}";
        System.out.println(URLEncoder.encode(str, "UTF-8"));
    }
}
