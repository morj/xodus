package jetbrains.xodus.query;

import com.google.common.base.Function;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.QueryEngine;

public class XodusQuery<K> extends AbstractXodusQuery<K, XodusQuery<K>> {
    /* private static final Function<Entity, Entity> IDEMPOTENT = new Function<Entity, Entity>() {
        @Nullable
        @Override
        public Entity apply(Entity input) {
            return input;
        }
    }; */

    protected XodusQuery(QueryEngine queryEngine, Function<Entity, Object> transformer, String entityType) {
        super(queryEngine, transformer, entityType);
    }


    @SuppressWarnings("unchecked")
    public <U> XodusQuery<U> selectDistinct(Expression<U> expr) { // TODO: move to some interface
        queryMixin.setProjection(expr);
        queryMixin.distinct();
        return (XodusQuery<U>) this;
    }


    /* public static XodusQuery<Entity> create(QueryEngine queryEngine, EntityPath<Entity> entityPath) {
        return create(queryEngine, IDEMPOTENT, entityPath);
    } */


    public static <K> XodusQuery<K> create(QueryEngine queryEngine, Function<Entity, Object> transformer, EntityPath<K> entityPath) {
        return create(queryEngine, transformer, entityPath.getType());
    }

    public static <K> XodusQuery<K> create(QueryEngine queryEngine, Function<Entity, Object> transformer, Class<? extends K> entityClass) {
        return new XodusQuery<>(queryEngine, transformer, entityClass.getSimpleName());
    }

}
