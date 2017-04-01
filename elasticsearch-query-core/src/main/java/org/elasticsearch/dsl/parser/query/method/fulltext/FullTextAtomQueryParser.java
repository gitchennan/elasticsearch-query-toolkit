package org.elasticsearch.dsl.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.dsl.parser.query.method.MethodQueryParser;

import java.util.List;
import java.util.function.Predicate;

public class FullTextAtomQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public FullTextAtomQueryParser(ParseActionListener parseActionListener) {
        methodQueryParsers = ImmutableList.of(
                new MatchAtomQueryParser(parseActionListener),
                new MultiMatchAtomQueryParser(),
                new QueryStringAtomQueryParser(),
                new SimpleQueryStringAtomQueryParser()
        );
    }

    public Boolean isFulltextAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(new Predicate<MethodQueryParser>() {
            @Override
            public boolean test(MethodQueryParser methodQueryParser) {
                return methodQueryParser.isMatchMethodInvocation(invocation);
            }
        });
    }

    public AtomQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);
        MethodQueryParser matchAtomQueryParser = getQueryParser(methodInvocation);
        return matchAtomQueryParser.parseAtomMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation invocation) {
        for (MethodQueryParser methodQueryParser : methodQueryParsers) {
            if (methodQueryParser.isMatchMethodInvocation(invocation)) {
                return methodQueryParser;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support method query expr[%s] condition", invocation.getMethodName()));
    }
}
