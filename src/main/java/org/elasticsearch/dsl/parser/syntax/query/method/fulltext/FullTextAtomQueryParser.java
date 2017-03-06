package org.elasticsearch.dsl.parser.syntax.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.syntax.query.method.AbstractAtomMethodQueryParser;

public class FullTextAtomQueryParser {

    protected ParseActionListener parseActionListener;

    public FullTextAtomQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    public AtomQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        AbstractAtomMethodQueryParser matchAtomQueryParser = getQueryParser(methodQueryExpr);
        return matchAtomQueryParser.parseAtomMethodQuery(methodQueryExpr, queryAs, sqlArgs);
    }

    private AbstractAtomMethodQueryParser getQueryParser(SQLMethodInvokeExpr matchQueryExpr) {
        if (Boolean.TRUE == isMatchQuery(matchQueryExpr)) {
            return new MatchAtomQueryParser(parseActionListener);
        }
        else if (Boolean.TRUE == isMatchPrefixQuery(matchQueryExpr)) {
            return new MultiMatchAtomQueryParser(parseActionListener);
        }
        else if(Boolean.TRUE == isQueryStringQuery(matchQueryExpr)) {
            return new QueryStringAtomQueryParser(parseActionListener);
        }
        else if(Boolean.TRUE == isSimpleQueryStringQuery(matchQueryExpr)) {
            return new SimpleQueryStringAtomQueryParser(parseActionListener);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", matchQueryExpr.getMethodName()));
    }

    private Boolean isMatchQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "match".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private Boolean isMatchPrefixQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "multiMatch".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private Boolean isQueryStringQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "queryString".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private Boolean isSimpleQueryStringQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "simpleQueryString".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }
}
