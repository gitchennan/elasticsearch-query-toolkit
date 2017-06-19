package org.es.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.MethodQueryParser;

import java.util.List;

public class TermLevelAtomQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public TermLevelAtomQueryParser(ParseActionListener parseActionListener) {
        methodQueryParsers = ImmutableList.of(
                new PrefixAtomQueryParser(parseActionListener),
                new TermAtomQueryParser(parseActionListener),
                new TermsAtomQueryParser(parseActionListener),
                new WildcardAtomQueryParser(parseActionListener),
                new RegexpAtomQueryParser(parseActionListener),
                new FuzzyAtomQueryParser(parseActionListener)
        );
    }

    public Boolean isTermLevelAtomQuery(MethodInvocation invocation) {
        for (MethodQueryParser methodQueryParserItem : methodQueryParsers) {
            if (methodQueryParserItem.isMatchMethodInvocation(invocation)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.TRUE;
    }

    public AtomQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, SQLArgs SQLArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, SQLArgs);
        MethodQueryParser matchAtomQueryParser = getQueryParser(methodInvocation);
        return matchAtomQueryParser.parseAtomMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation methodInvocation) {
        for (MethodQueryParser methodQueryParserItem : methodQueryParsers) {
            if (methodQueryParserItem.isMatchMethodInvocation(methodInvocation)) {
                return methodQueryParserItem;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support method query expr[%s] condition",
                        methodInvocation.getMethodName()));
    }
}
