package org.elasticsearch.dsl.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;

public class FullTextAtomQueryParser {

    protected ParseActionListener parseActionListener;

    public FullTextAtomQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    public static Boolean isFulltextAtomQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return MatchAtomQueryParser.isMatchQuery(methodQueryExpr) || MultiMatchAtomQueryParser.isMultiMatch(methodQueryExpr) ||
                QueryStringAtomQueryParser.isQueryStringQuery(methodQueryExpr) || SimpleQueryStringAtomQueryParser.isSimpleQueryStringQuery(methodQueryExpr);
    }

    public AtomQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        AbstractAtomMethodQueryParser matchAtomQueryParser = getQueryParser(methodQueryExpr);
        return matchAtomQueryParser.parseAtomMethodQuery(methodQueryExpr, queryAs, sqlArgs);
    }

    private AbstractAtomMethodQueryParser getQueryParser(SQLMethodInvokeExpr methodQueryExpr) {
        if (Boolean.TRUE == MatchAtomQueryParser.isMatchQuery(methodQueryExpr)) {
            return new MatchAtomQueryParser(parseActionListener);
        }

        if (Boolean.TRUE == MultiMatchAtomQueryParser.isMultiMatch(methodQueryExpr)) {
            return new MultiMatchAtomQueryParser(parseActionListener);
        }

        if (Boolean.TRUE == QueryStringAtomQueryParser.isQueryStringQuery(methodQueryExpr)) {
            return new QueryStringAtomQueryParser(parseActionListener);
        }

        if (Boolean.TRUE == SimpleQueryStringAtomQueryParser.isSimpleQueryStringQuery(methodQueryExpr)) {
            return new SimpleQueryStringAtomQueryParser(parseActionListener);
        }

        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", methodQueryExpr.getMethodName()));
    }
}
