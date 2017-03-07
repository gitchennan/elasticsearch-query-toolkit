package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;

public class TermLevelAtomQueryParser {

    protected ParseActionListener parseActionListener;

    public TermLevelAtomQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    private static Boolean isPrefixQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "prefix".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private static Boolean isTermQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "term".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private static Boolean isTermsQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "terms".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private static Boolean isWildcardQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "wildcard".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private static Boolean isRegexpQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "regexp".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    private static Boolean isFuzzyQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return "fuzzy".equalsIgnoreCase(methodQueryExpr.getMethodName());
    }

    public static Boolean isTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return isPrefixQuery(methodQueryExpr) || isTermQuery(methodQueryExpr) ||
                isTermsQuery(methodQueryExpr) || isWildcardQuery(methodQueryExpr) ||
                isRegexpQuery(methodQueryExpr) || isFuzzyQuery(methodQueryExpr);
    }

    public AtomQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        AbstractAtomMethodQueryParser matchAtomQueryParser = getQueryParser(methodQueryExpr);
        return matchAtomQueryParser.parseAtomMethodQuery(methodQueryExpr, queryAs, sqlArgs);
    }

    private AbstractAtomMethodQueryParser getQueryParser(SQLMethodInvokeExpr methodQueryExpr) {
        if (Boolean.TRUE == isPrefixQuery(methodQueryExpr)) {
            return new PrefixAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == isTermQuery(methodQueryExpr)) {
            return new TermAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == isTermsQuery(methodQueryExpr)) {
            return new TermsAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == isWildcardQuery(methodQueryExpr)) {
            return new WildcardAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == isRegexpQuery(methodQueryExpr)) {
            return new RegexpAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == isFuzzyQuery(methodQueryExpr)) {
            return new FuzzyAtomQueryParser(parseActionListener);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", methodQueryExpr.getMethodName()));
    }
}
