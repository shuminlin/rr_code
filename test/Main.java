package test;
import java.util.Map;

import static java.lang.System.out;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-10-5
 * Time: 下午7:42
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main(String[] args){

         out.println("abc");
        Map a = System.getenv();
            out.println(a.toString());
    }
}
