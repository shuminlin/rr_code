package com.renren.bigdata;
//import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import java.lang.Math;
import java.util.*;


/**
 * User: Shumin.lin@renren-inc.com
 * Date: 13-6-13
 * Time: 上午11:09
 * To change this template use File | Settings | File Templates.
 */
public class CalcUtil {

    public static double getCTROutlier(double[] data){
        double[] noneZero = getNoneZero(data);
        double ctr = getExtremeOutlier(noneZero);

        return ctr;
    }

    public static double[] getNoneZero(double[] data)throws  IllegalArgumentException{
        int count = 0;
        for(double d:data){
            if( d > 0){
                count++;
            }
        }

        if(count==0) throw new IllegalArgumentException("All zero encountered!!");

        double[] noneZero = new double[count];
        int index = 0;
        for(int i=0;i<data.length;i++){
            if( data[i] > 0){
                noneZero[index] = data[i];
                index++;
            }
        }

        return noneZero;
    }

    public static int getCCROutlierCount(double[] data){
        int MAX_CLICK = 1024*1024;
        double P_VALUE = 0.01;
        int count = 0;
        double thres = 1.0;
        double[] nonZero =  getNoneZero(data);
        double ccrMedian =  getQuantile(nonZero,0.5);
        while(thres > P_VALUE && count < MAX_CLICK){
            thres = thres*(1-ccrMedian);
            count++;
        }

        return count;
    }


    public static double getMean(double[] data){
        double res;
        double sum = 0;
        for(double d:data){
            sum+=d;
        }
        res = sum / data.length;
        return res;
    }

    public static double getSD(double[] data){
        double sum = 0;
        double res;
        double mean;
        mean = getMean(data);
        for(double d:data){
            sum+=(d-mean)*(d-mean);
        }
        sum = sum / (data.length - 1);
        res = Math.sqrt(sum);

        return res;
    }



//    public static double getQuantile(double[] data, double p) throws IllegalArgumentException{
//        if (data.length < 3) throw new IllegalArgumentException("Illegal input: Data vector length must > 3");
//        Percentile instance = new Percentile();
//        double res = instance.evaluate(data,p*100);
//        return res;
//
//    }


    public static double getQuantile(double[] data, double p) throws IllegalArgumentException{
        if(p >=1 || p<0){
           throw new IllegalArgumentException("Input p should be [0,1)");
        }
        LinkedList<Double> val = new LinkedList<Double>();
        int index = (int)(Math.floor(p*data.length));

        for(double d:data){
           val.add(new Double(d));
        }
        Collections.sort(val);
//        System.out.println("index = "+index);
        return val.get(index);

    }

    public static double getOutlier(double[] data){
        double res;
        res = 1.5 * (getQuantile(data,0.75) - getQuantile(data,0.25));

        return res;
    }

    public static double getExtremeOutlier(double[] data){
        double res;
        res = 2.0 * 1.5 * (getQuantile(data,0.75) - getQuantile(data,0.25));

        return res;
    }

    public static double calcAccumulation(int[] data){
        double res = 0.0;
        int sum = 0;
        for(int d:data){
          sum+=d;
        }

        for(int i=0;i<data.length;i++){
           res+= (i+1)*data[i]*1.0/(sum*1.0);
        }

        return res;
    }




    public static void main(String[] args) throws Exception{
        int[] data =  {1,2,2} ;
        double[] tmp =  {1};//34,2,77,12,100,1,2,3,4,5,6,7,8,9,10,}  ;
        double[] ctr =  {0.001,0.002,0.003,0}  ;
        double[] ccr =  {0.005,0.005,0.005}  ;

//        System.out.println("outlier thres= "+getOutlier(tmp));
//        System.out.println("Extreme outlier thres= "+getExtremeOutlier(tmp));
//        System.out.println("Accumulated sum: "+calcAccumulation(data));
        System.out.println("quantile: "+getQuantile(tmp, 0.9));
//        System.out.println("mean: "+getMean(tmp));
//        System.out.println("SD: "+getSD(tmp));
//        System.out.println("ctr error thres: "+getCTROutlier(ctr));
//        System.out.println("ccr click count: "+getCCROutlierCount(ccr));
        double[] test = getNoneZero(tmp);
        System.out.println("quantile: "+getQuantile(test, 0.9));
        for(double t:test) System.out.println(t);


    }

}
