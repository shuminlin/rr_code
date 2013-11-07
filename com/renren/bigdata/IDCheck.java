package com.renren.bigdata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-9-9
 * Time: 下午4:55
 * To change this template use File | Settings | File Templates.
 */
public class IDCheck {
    private static String qqMailCheck = "^[0-9]*@qq.com$";
    private static String mailCheck = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
    private static String mobileCheck = "^(13[4,5,6,7,8,9]|15[0,8,9,1,7]|18[8,7,6,5])\\d{8}$";
//    private static String digCheck = "^\\d*$";  //^([0-9].*)$
private static String digCheck = "^([0-9]*)$";  //

    public static Pattern qqPattern = Pattern.compile(qqMailCheck);
    public static Pattern mailPattern = Pattern.compile(mailCheck);
    public static Pattern mobilePattern = Pattern.compile(mobileCheck);
    public static Pattern digPattern = Pattern.compile(digCheck);

    public static boolean isMobileMail(String input){
        boolean isMobileMail = false;
        if(input.contains("139.com")||input.contains("189.cn")){
            isMobileMail = true;
        }
        return isMobileMail;
    }

    public static boolean isDig(String input){
        boolean isDig = false;
        Matcher matcher = digPattern.matcher(input);
        if(matcher.find()) isDig = true;
        return isDig;
    }

    public static boolean isMobile(String input){
        boolean isMobile = false;
        Matcher matcher = mobilePattern.matcher(input);
        if(matcher.find()) isMobile = true;
        return isMobile;
    }
    public static boolean isMail(String input){
        boolean isMail = false;
        Matcher matcher = mailPattern.matcher(input);
//        if(matcher.find()) isMail = true;
        if(input.contains("@")) isMail = true;

        return isMail;
    }
    public static boolean isQQMail(String input){
        boolean isQQ  = false;
        Matcher matcher = qqPattern.matcher(input);

//        if(matcher.find()) isQQ = true;
        if(input.contains("qq.com")) isQQ = true;

        return isQQ;
    }
    public static void main(String[] args)throws Exception{
        String val  = "shuminlin@renren.com";
        System.out.println("check value = "+val);
        System.out.println("isQQMail = " + isQQMail(val));
        System.out.println("isMail = "+isMail(val));
        System.out.println("isMobile = "+isMobile(val));
        System.out.println("isMobileMail = "+isMobileMail(val));
        System.out.println("isDig = "+isDig(val));


    }
}
