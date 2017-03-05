package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;

public class FullTextAtomQueryParser extends AbstractAtomFullTextQueryParser {

    public FullTextAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomQuery parseFullTextAtomQuery(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.TRUE == isMatchQuery(matchQueryExpr)) {
            MatchAtomQueryParser matchAtomQueryParser = new MatchAtomQueryParser(parseActionListener);
            return matchAtomQueryParser.parseMatchAtomQuery(matchQueryExpr, queryAs, sqlArgs);
        }
        else if (Boolean.TRUE == isMatchPrefixQuery(matchQueryExpr)) {
            MatchPrefixAtomQueryParser matchPrefixAtomQueryParser = new MatchPrefixAtomQueryParser(parseActionListener);
            return matchPrefixAtomQueryParser.parseMatchAtomQuery(matchQueryExpr, queryAs, sqlArgs);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] Can not support method query expr[%s] condition", matchQueryExpr.getMethodName()));
    }


    private Boolean isMatchQuery(SQLMethodInvokeExpr matchQueryExpr) {
        return "match".equalsIgnoreCase(matchQueryExpr.getMethodName());
    }

    private Boolean isMatchPrefixQuery(SQLMethodInvokeExpr matchQueryExpr) {
        return "matchPrefix".equalsIgnoreCase(matchQueryExpr.getMethodName());
    }
}
