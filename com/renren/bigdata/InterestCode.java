package com.renren.bigdata;

/**
 * Created with IntelliJ IDEA.
 * User: zyshen
 * Date: 13-8-26
 * Time: 下午3:22
 * To change this template use File | Settings | File Templates.
 */
public class InterestCode

{
    public static String getInterest(int index){
        String result="未入库兴趣标签";
        String[] interestTable = new String[]{
                "报刊杂志",
                "财务",
                "参考",
                "导航",
                "工具",
                "健康健美",
                "教育",
                "旅行",
                "商业",
                "社交",
                "摄影与录像",
                "生活",
                "体育",
                "天气",
                "图书",
                "效率",
                "新闻",
                "医疗",
                "音乐",
                "角色扮演游戏",
                "娱乐",
                "商品指南",
                "美食佳饮",
                "动作游戏",
                "探险游戏",
                "街机游戏",
                "桌面游戏",
                "扑克牌游戏",
                "娱乐场游戏",
                "骰子游戏",
                "教育游戏",
                "家庭游戏",
                "儿童游戏",
                "音乐游戏",
                "智力游戏",
                "赛车游戏",
                "模拟游戏",
                "体育游戏",
                "策略游戏",
                "小游戏",
                "文字游戏",
                "游戏",

        };

        if(index<interestTable.length){
            result = interestTable[index - 1];
        }

        return result;
    }
}
