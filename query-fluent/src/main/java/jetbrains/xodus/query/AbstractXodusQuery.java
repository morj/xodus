package jetbrains.xodus.query;

import com.google.common.base.Function;
import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.QueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.*;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.NodeBase;
import jetbrains.exodus.query.QueryEngine;
import jetbrains.exodus.query.SortByProperty;
import jetbrains.exodus.query.TreeKeepingEntityIterable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;

public abstract class AbstractXodusQuery<K, Q extends AbstractXodusQuery<K, Q>> implements SimpleQuery<Q>, Iterable<K> {

    private final QueryEngine queryEngine;
    private final Function<Entity, Object> transformer;
    private final String entityType;
    private final XodusSerializer serializer;
    protected final QueryMixin<Q> queryMixin;

    @SuppressWarnings("unchecked")
    public AbstractXodusQuery(QueryEngine queryEngine, Function<Entity, Object> transformer, String entityType) {
        this.queryEngine = queryEngine;
        this.transformer = transformer;
        this.entityType = entityType;
        this.serializer = new XodusSerializer(queryEngine);
        this.queryMixin = new QueryMixin<>((Q) this, new DefaultQueryMetadata(), false);
    }

    // TODO: don't use mutable QueryMixin/DefaultQueryMetadata approach, be functional (copy-on-write) instead

    @Override
    public Q distinct() {
        return queryMixin.distinct();
    }

    public Q where(Predicate e) {
        return queryMixin.where(e);
    }

    @Override
    public Q where(Predicate... e) {
        return queryMixin.where(e);
    }

    @Override
    public Q limit(long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public Q offset(long offset) {
        return queryMixin.offset(offset);
    }

    @Override
    public Q restrict(QueryModifiers modifiers) {
        return queryMixin.restrict(modifiers);
    }

    @Override
    public Q orderBy(OrderSpecifier<?>... o) {
        return queryMixin.orderBy(o);
    }

    @Override
    public <T> Q set(ParamExpression<T> param, T value) {
        return queryMixin.set(param, value);
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<K> iterator() {
        QueryMetadata metadata = queryMixin.getMetadata();
        Predicate filter = metadata.getWhere();
        NodeBase node = serializer.handle(filter);
        node = applySorts(node, metadata.getOrderBy());
        TreeKeepingEntityIterable tkei = queryEngine.query(entityType, node);
        Expression<K> expr = (Expression<K>) queryMixin.getMetadata().getProjection();
        if (expr instanceof Path<?>) {
            final PathMetadata selected = ((Path) expr).getMetadata();
            switch (selected.getPathType()) {
                case PROPERTY:
                    if (!metadata.isDistinct()) {
                        throw new UnsupportedOperationException("Inefficient select is not supported");
                    }
                    return applyTransformer(queryEngine.selectDistinct(tkei, selected.getName()).iterator());
                default:
                    throw new UnsupportedOperationException("Unsupported path type " + selected.getPathType());
            }
        }
        return applyTransformer(tkei.iterator());
    }

    protected NodeBase applySorts(NodeBase source, List<OrderSpecifier<?>> sorts) {
        // TODO: how to incorporate to visitor
        for (OrderSpecifier<?> sort : sorts) {
            final Order order = sort.getOrder();
            final Expression<? extends Comparable> expr = sort.getTarget();
            if (expr instanceof Path<?>) {
                final PathMetadata ordered = ((Path) expr).getMetadata();
                switch (ordered.getPathType()) {
                    case PROPERTY:
                        source = new SortByProperty(source, ordered.getName(), order == Order.ASC);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported order expression " + ordered.getPathType());
                }
            }
        }
        return source;
    }

    @NotNull
    protected Iterator<K> applyTransformer(final Iterator<Entity> iterator) {
        return new Iterator<K>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @SuppressWarnings("unchecked")
            @Override
            public K next() {
                return (K) transformer.apply(iterator.next());
            }

            @Override
            public void remove() {
            }
        };
    }
}
