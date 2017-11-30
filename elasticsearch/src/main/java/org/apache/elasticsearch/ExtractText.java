package org.apache.elasticsearch;

import net.htmlparser.jericho.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/11/30.
 *
 *
 */
public class ExtractText {

    public static final String DATA_DIR = "F:\\data";

    public static Map<String,String> parserHtml(File htmlFile) throws Throwable {
        Map<String,String> htmlData = new HashMap<>();

        String sourceUrlString="data/test.html";
        Source source=new Source(htmlFile);

        // Call fullSequentialParse manually as most of the source will be parsed.
        source.fullSequentialParse();

        String title=getTitle(source);

        htmlData.put("title",title);

        String description=getMetaValue(source,"description");
        description=description==null ? "(none)" : description;

        if(title==null){
            htmlData.put("title","null");
        }

        String content = source.getTextExtractor().setIncludeAttributes(true).toString();
        content = description + " " + content;

        htmlData.put("content",content);

        String path = htmlFile.getPath();
        String url = path.substring(DATA_DIR.length(),path.length());

        htmlData.put("url","http://" + url.replaceAll("\\\\","/"));

        return htmlData;
    }

    private static String getTitle(Source source) {
        Element titleElement=source.getFirstElement(HTMLElementName.TITLE);
        if (titleElement==null) return "";
        // TITLE element never contains other tags so just decode it collapsing whitespace:
        return CharacterReference.decodeCollapseWhiteSpace(titleElement.getContent());
    }

    private static String getMetaValue(Source source, String key) {
        for (int pos=0; pos<source.length();) {
            StartTag startTag=source.getNextStartTag(pos,"name",key,false);
            if (startTag==null) return null;
            if (startTag.getName() == HTMLElementName.META)
                return startTag.getAttributeValue("content"); // Attribute values are automatically decoded
            pos=startTag.getEnd();
        }
        return null;
    }
}
