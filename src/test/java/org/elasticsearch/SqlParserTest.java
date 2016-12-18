package org.elasticsearch;

import junit.framework.TestCase;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.ElasticSql2DslParser;
import org.junit.Assert;


public class SqlParserTest extends TestCase {

    public void testParseSelectFieldWithQueryAs() {
        String sql = "select b.id,bookCategory,b.bookStatus,clazz.sortNo,b.clazz.cid from book b where 1 = 1";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertEquals(dslContext.getQueryFieldList().get(0), "id");
        Assert.assertEquals(dslContext.getQueryFieldList().get(1), "bookCategory");
        Assert.assertEquals(dslContext.getQueryFieldList().get(2), "bookStatus");
        Assert.assertEquals(dslContext.getQueryFieldList().get(3), "clazz.sortNo");
        Assert.assertEquals(dslContext.getQueryFieldList().get(4), "clazz.cid");
    }

    public void testParseSelectFieldWithoutQueryAs() {
        String sql = "select b.id,bookCategory,b.bookStatus,clazz.sortNo,clazz.cid from book where 1 = 1";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertEquals(dslContext.getQueryFieldList().get(0), "b.id");
        Assert.assertEquals(dslContext.getQueryFieldList().get(1), "bookCategory");
        Assert.assertEquals(dslContext.getQueryFieldList().get(2), "b.bookStatus");
        Assert.assertEquals(dslContext.getQueryFieldList().get(3), "clazz.sortNo");
        Assert.assertEquals(dslContext.getQueryFieldList().get(4), "clazz.cid");
    }

    public void testParseSelectFieldMatchAll() {
        String sql = "select * from book where 1 = 1";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(dslContext.getQueryFieldList()));
    }

    public void testParseFilterCondition() {
        //String sql = "select * from book b where 1 = 1 and b.status='ONLINE'"; //SQLBinaryOpExpr
        //String sql = "select * from book b where price+1 < 2"; //SQLBinaryOpExpr
        //String sql = "select * from book b where c in (1,2,3)";    //SQLInListExpr
        //String sql = "select * from book b where c not in (1,2,3)"; //SQLInListExpr
        //String sql = "select * from book b where c between 1 and 3"; //SQLBetweenExpr

        String sql = "select * from book bk where a + 1 > b"; //SQLBetweenExpr

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
    }
}
