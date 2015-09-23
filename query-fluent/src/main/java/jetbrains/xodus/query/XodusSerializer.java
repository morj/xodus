package jetbrains.xodus.query;

import com.querydsl.core.types.*;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.*;

public class XodusSerializer implements Visitor<Object, Void> {

    private final QueryEngine queryEngine;

    public XodusSerializer(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public NodeBase handle(Expression<?> expression) {
        if (expression == null) {
            return new GetAll();
        }
        return (NodeBase) expression.accept(this, null);
    }

    @Override
    public Object visit(Constant<?> expr, Void context) {
        /* if (Enum.class.isAssignableFrom(expr.getType())) {
            Constant<? extends Enum<?>> expectedExpr = (Constant<? extends Enum<?>>) expr;
            return expectedExpr.getConstant().name();
        } */
        return expr.getConstant();
    }

    @Override
    public Object visit(FactoryExpression<?> expr, Void context) {
        return null;
    }

    @Override
    public Object visit(Operation<?> expr, Void context) {
        Operator op = expr.getOperator();
        if (op == Ops.EQ) {
            if (expr.getArg(0) instanceof Operation) {
                // Operation<?> lhs = (Operation<?>) expr.getArg(0);
                throw new UnsupportedOperationException("Illegal operation " + expr);
            } else if (isLink(expr, 0)) {
                return new LinkEqual(key(expr, 0), entity(expr, 1));
            } else {
                return new PropertyEqual(key(expr, 0), value(expr, 1));
            }
        } else if (op == Ops.AND) {
            NodeBase lhs = handle(expr.getArg(0));
            NodeBase rhs = handle(expr.getArg(1));
            return new And(lhs, rhs);
        } else if (op == Ops.NOT) {
            return new UnaryNot(handle(expr.getArg(0)));
        }
        return null;
    }

    @Override
    public Object visit(ParamExpression<?> expr, Void context) {
        return null;
    }

    @Override
    public Object visit(Path<?> expr, Void context) {
        PathMetadata metadata = expr.getMetadata();
        if (metadata.getParent() != null) {
            if (metadata.getPathType() == PathType.COLLECTION_ANY) {
                return visit(metadata.getParent(), context);
            } else if (metadata.getParent().getMetadata().getPathType() != PathType.VARIABLE
                    && metadata.getParent().getMetadata().getPathType() != PathType.DELEGATE) {
                String rv = getKeyForPath(expr, metadata);
                return visit(metadata.getParent(), context) + "." + rv;
            }
        }
        return getKeyForPath(expr, metadata);
    }

    @Override
    public Object visit(SubQueryExpression<?> expr, Void context) {
        return null;
    }

    @Override
    public Object visit(TemplateExpression<?> expr, Void context) {
        return null;
    }

    protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {
        return metadata.getElement().toString();
    }

    @SuppressWarnings("SimplifiableIfStatement")
    protected boolean isLink(Operation<?> expr, int exprIndex) {
        Expression<?> arg = expr.getArg(exprIndex);
        if (arg instanceof Path) {
            return isLink((Path<?>) arg);
        } else {
            return false;
        }
    }

    protected boolean isLink(Path<?> arg) {
        // return arg.getAnnotatedElement().isAnnotationPresent(Link.class);
        return false; // TODO: annotation processing?
    }

    private String key(Operation<?> expr, int index) {
        return (String) data(expr, index);
    }

    private Entity entity(Operation<?> expr, int index) {
        return (Entity) data(expr, index);
    }

    private Comparable value(Operation<?> expr, int index) {
        return (Comparable) data(expr, index);
    }

    private Object data(Operation<?> expr, int index) {
        return expr.getArg(index).accept(this, null);
    }
}
