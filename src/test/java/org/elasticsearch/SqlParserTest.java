package org.elasticsearch;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserTest {

    @Test
    public void testParseSelectFieldWithQueryAs() {
        String sql = "select b.id,bookCategory,b.bookStatus,clazz.sortNo,b.clazz.cid from book b where b.type = 'AV'";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertEquals(dslContext.getQueryFieldList().get(0), "id");
        Assert.assertEquals(dslContext.getQueryFieldList().get(1), "bookCategory");
        Assert.assertEquals(dslContext.getQueryFieldList().get(2), "bookStatus");
        Assert.assertEquals(dslContext.getQueryFieldList().get(3), "clazz.sortNo");
        Assert.assertEquals(dslContext.getQueryFieldList().get(4), "clazz.cid");
    }

    @Test
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

    @Test
    public void testParseSelectFieldMatchAll_01() {
        String sql = "select * from lib.av as b where c = 1 and updated_at > date('yyyy-MM-dd', '2016-10-22')";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(dslContext.getQueryFieldList()));
    }

    @Test
    public void testParseSelectFieldMatchAll_02() {
        String sql = "select b.* from book b";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(dslContext.getQueryFieldList()));
    }

    @Test
    public void testParseSelectFieldMatchAll_03() {
        String sql = "select b.n.* from book b";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
        Assert.assertTrue(dslContext.getQueryFieldList().size() == 1);
        Assert.assertTrue("n.*".equals(dslContext.getQueryFieldList().get(0)));
    }

    @Test
    public void testParseInFilterCondition_01() {
        String sql = "select * from book bk where bk.status in ('SALES') and category='b'"
                + " and (bk.bookType in ('AV') or salesArea = '4')"
                + " and requestType=2";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();
        System.out.println(dslContext.toString());
    }

    @Test
    public void testParseNestCondition() {
        String sql = "select id,bookStatus,updatedAt,bookClassifications from book.basic dd where"
                + " bookCategory = '802'"
                + " and bookClassifications.classificationId > 0"
                + " order by id desc,nvl(bookClassifications.sortNo, 999, 'min') asc";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser(sql);
        ElasticDslContext dslContext = sql2DslParser.parse();

        System.out.println(dslContext.toString());
    }
    
}
