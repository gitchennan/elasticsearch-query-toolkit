package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;

public abstract class AbstractFullTextQueryParser extends AbstractAtomFullTextQueryParser {

    protected static final String COMMA = ",";

    protected static final String COLON = ":";

    public AbstractFullTextQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    protected abstract void checkMatchQueryMethod(SQLMethodInvokeExpr matchQueryExpr);

    protected abstract AtomQuery parseMatchQueryMethodExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs);

    public final AtomQuery parseMatchAtomQuery(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        checkMatchQueryMethod(matchQueryExpr);

        return parseMatchQueryMethodExpr(matchQueryExpr, queryAs, sqlArgs);
    }
}
