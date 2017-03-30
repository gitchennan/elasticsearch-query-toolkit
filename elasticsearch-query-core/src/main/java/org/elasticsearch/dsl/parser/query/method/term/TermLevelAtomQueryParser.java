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

    public static Boolean isTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return PrefixAtomQueryParser.isPrefixQuery(methodQueryExpr) || TermAtomQueryParser.isTermQuery(methodQueryExpr) ||
                TermsAtomQueryParser.isTermsQuery(methodQueryExpr) || WildcardAtomQueryParser.isWildcardQuery(methodQueryExpr) ||
                RegexpAtomQueryParser.isRegexpQuery(methodQueryExpr) || FuzzyAtomQueryParser.isFuzzyQuery(methodQueryExpr);
    }

    public AtomQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        AbstractAtomMethodQueryParser matchAtomQueryParser = getQueryParser(methodQueryExpr);
        return matchAtomQueryParser.parseAtomMethodQuery(methodQueryExpr, queryAs, sqlArgs);
    }

    private AbstractAtomMethodQueryParser getQueryParser(SQLMethodInvokeExpr methodQueryExpr) {
        if (Boolean.TRUE == PrefixAtomQueryParser.isPrefixQuery(methodQueryExpr)) {
            return new PrefixAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == TermAtomQueryParser.isTermQuery(methodQueryExpr)) {
            return new TermAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == TermsAtomQueryParser.isTermsQuery(methodQueryExpr)) {
            return new TermsAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == WildcardAtomQueryParser.isWildcardQuery(methodQueryExpr)) {
            return new WildcardAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == RegexpAtomQueryParser.isRegexpQuery(methodQueryExpr)) {
            return new RegexpAtomQueryParser(parseActionListener);
        }
        if (Boolean.TRUE == FuzzyAtomQueryParser.isFuzzyQuery(methodQueryExpr)) {
            return new FuzzyAtomQueryParser(parseActionListener);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", methodQueryExpr.getMethodName()));
    }
}
