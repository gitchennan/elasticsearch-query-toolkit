package org.elasticsearch;

import junit.framework.TestCase;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.junit.Assert;


public class SqlParserTest extends TestCase {

    public void testParseSelectFieldWithQueryAs() {
        String sql = "select b.id,bookCategory,b.bookStatus,clazz.sortNo,b.clazz.cid from book b where c = 1";
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
        String sql = "select b.id,bookCategory,b.bookStatus,clazz.sortNo,clazz.cid from book where c = 1";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertEquals(dslContext.getQueryFieldList().get(0), "b.id");
        Assert.assertEquals(dslContext.getQueryFieldList().get(1), "bookCategory");
        Assert.assertEquals(dslContext.getQueryFieldList().get(2), "b.bookStatus");
        Assert.assertEquals(dslContext.getQueryFieldList().get(3), "clazz.sortNo");
        Assert.assertEquals(dslContext.getQueryFieldList().get(4), "clazz.cid");
    }

    public void testParseSelectFieldMatchAll_01() {
        String sql = "select * from book where c = 1";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(dslContext.getQueryFieldList()));
    }

    public void testParseSelectFieldMatchAll_02() {
        String sql = "select b.* from book b";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(dslContext.getQueryFieldList()));
    }

    public void testParseSelectFieldMatchAll_03() {
        String sql = "select b.n.* from book b";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(dslContext.getQueryFieldList().size() == 1);
        Assert.assertTrue("n.*".equals(dslContext.getQueryFieldList().get(0)));
    }

    public void testParseInFilterCondition_01() {
        String sql = "select * from book bk where bk.status in ('ONLINE','DONE') and productCategory='802'";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();
        System.out.println(dslContext.toString());
    }

    public void testParseEqFilterCondition_01() {
        //String sql = "select * from book bk where (bk.price = 10.2 or bk.category not in ('AV', 'H')) and bk.status in ('ONLINE', 'DONE')";

        String sql = "select p.id,p.productCategory,p.displayName from product_v9 p "
                + "where p.productCategory in ('901','902') and p.userGroupArray in ('NEW_USER', 'DEFAULT') and salesArea not in(2,3,4) "
                + " and p.productStatus in ('ONLINE', 'PAUSE', 'TEMPORARY_FULL', 'PREVIEW')"
                + " and p.sourceType = '9' and (p.productType='LOAN_REQUEST' or (p.price > 100 and p.price <= 200))"
                + " and p.price between 1 and 1000";


        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();
        System.out.println(dslContext.toString());
    }

    public void testParseFilterCondition() {
        //String sql = "select * from book b where b.status='ONLINE'"; //SQLBinaryOpExpr
        //String sql = "select * from book b where c in (1,2,3)";    //SQLInListExpr
        //String sql = "select * from book b where c not in (1,2,3)"; //SQLInListExpr
        String sql = "select * from book b where c between 1 and 3"; //SQLBetweenExpr

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
    }

    public void testParseIsNullFilter() {
        String sql = "select * from book b where b.cls is not null"; //SQLBetweenExpr

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
    }

    public void testParseOrderCondition() {
        String sql = "select * from book b where b.status = 'ONLINE' order by nvl(b.sortNo,-1) asc";
        //String sql = "select * from book b where b.status = 'ONLINE' order by b.sortNo asc,b.bbs desc";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
    }
}
