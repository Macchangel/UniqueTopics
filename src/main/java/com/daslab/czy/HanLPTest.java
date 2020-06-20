package com.daslab.czy;






import com.daslab.czy.model.Token;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.document.sentence.Sentence;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class HanLPTest {

    public static void main(String[] args) throws ParseException, IOException {

        String[] testCase = new String[]{"中新网上海新闻月日电 五一小长假后巴士三公司九车队陆续收到了两封点赞路职工的服务态度的感谢信，急乘客所急、想乘客所想。据悉月日中午，孙先生一家四口在金沙江路丰庄西路站乘坐路公交车于天山西路淞虹路下车，当时夫妻两人只顾着两个孩子，匆匆忙忙的将装有证明材料、户口簿、身份证、房产证、出生证明的双肩背包遗落在公交车上，直至进入地铁站安检时才发现双肩包不见了，这可把孙先生夫妻急坏了。孙先生立刻赶到了路终点站甘溪路协和路，当班调度员金晓玮知晓情况后，带着孙先生进调度室认领双肩包。孙先生的双肩包是驾驶员陈骅在一程一检后交给调度员保管，双肩包完好无损的物归原主，孙先生激动地说道：真的太感谢你们了！这些东西非常重要，一旦遗失是相当难以补办。无独有偶，月日的上午路驾驶员徐亚杰师傅在一程一检时，拾到了一个背包并在第一时间利用背包内的信息联系到了赵先生，背包内有身份证、手机、现金等重要物品，背包的完璧归赵让赵先生感激万分。并在回家之后，写下了感谢信，对徐亚杰师傅拾金不昧的行为点赞。公交公司提醒广大乘客：随着企业复工学生复学，公交客流量增大，希望乘客们下车时不要遗忘随身携带的物品，避免造成不必要的损失。(完) 注：请在转载文章内容时务必注明出处!   编辑：王子涛"
        };
        Segment segment = HanLP.newSegment();
        CRFLexicalAnalyzer analyzer = new CRFLexicalAnalyzer();
        for (String sentence : testCase)
        {
            System.out.println(analyzer.analyze(sentence));
            List<Term> termList = CoreStopWordDictionary.apply(segment.seg(sentence));
            System.out.println(termList);
        }


//        String startDate = "2019-05-01";
//        String endDate = "2019-08-25";
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date startDt = sdf.parse(startDate);
//        Date endDt = sdf.parse(endDate);
//        Calendar rightNow = Calendar.getInstance();
//        rightNow.setTime(startDt);
//
//
//        while(rightNow.getTime().compareTo(endDt) < 0){
//            String date1 = sdf.format(rightNow.getTime());
//            System.out.println(date1);
//            rightNow.add(Calendar.MONTH, 1);
//            rightNow.add(Calendar.DAY_OF_MONTH, -1);
//            String date2 = sdf.format(rightNow.getTime());
//            System.out.println(date2);
//            rightNow.add(Calendar.DAY_OF_MONTH, 1);
//            System.out.println();
//        }

//        String s = "233,333,333,";
//        String[] ss = s.split(",");
//        System.out.println(ss.length);

    }
}
