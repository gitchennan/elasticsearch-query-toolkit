package org.elasticsearch.dsl.parser.syntax.query.method.term;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.syntax.query.method.AbstractAtomMethodQueryParser;

public class TermLevelAtomMethodQueryParser {

    protected ParseActionListener parseActionListener;

    public TermLevelAtomMethodQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    public AtomQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        AbstractAtomMethodQueryParser matchAtomQueryParser = getQueryParser(methodQueryExpr);
        return matchAtomQueryParser.parseAtomMethodQuery(methodQueryExpr, queryAs, sqlArgs);
    }

    private AbstractAtomMethodQueryParser getQueryParser(SQLMethodInvokeExpr methodQueryExpr) {
        if (Boolean.TRUE == isPrefixQuery(methodQueryExpr)) {
            //todo ..
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", methodQueryExpr.getMethodName()));
    }

    private Boolean isPrefixQuery(SQLMethodInvokeExpr matchQueryExpr) {
        return "prefix".equalsIgnoreCase(matchQueryExpr.getMethodName());
    }
}
