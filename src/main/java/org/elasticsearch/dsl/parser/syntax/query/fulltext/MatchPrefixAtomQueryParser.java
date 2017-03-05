package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;

public class MatchPrefixAtomQueryParser extends AbstractFullTextQueryParser {

    public MatchPrefixAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkMatchQueryMethod(SQLMethodInvokeExpr matchQueryExpr) {

    }

    @Override
    protected AtomQuery parseMatchQueryMethodExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        return null;
    }
}
