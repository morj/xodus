package sandbox;

import com.google.common.base.Function;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Path;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.PersistentEntity;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import static sandbox.QBar.bar;
import static sandbox.QMyEnum.myEnum;

public class Factory implements Function<Entity, Object> {
    @Nullable
    @Override
    public Object apply(Entity input) {
        if (input == null) {
            return null;
        }
        final String type = input.getType();
        if (type.equals(type(bar))) {
            final Entity enumValue = getLink(input, QBar.bar.myEnum);
            return new Bar(getProperty(input, QBar.bar.lol), getProperty(input, QBar.bar.s), toEnum(enumValue));
        } else if (type.equals(type(myEnum))) {
            return toEnum(input);
        }
        throw new IllegalStateException("Unknown entity type: " + type);
    }

    @NotNull
    public MyEnum toEnum(Entity input) {
        return getProperty(input, myEnum.number) == 1 ? MyEnum.FIRST : MyEnum.SECOND;
    }

    public Entity save(Bar output, Entity enumValue, PersistentStoreTransaction txn) {
        final PersistentEntity entity = txn.newEntity(type(bar));
        setProperty(entity, bar.lol, output.lol);
        setProperty(entity, bar.s, output.s);
        entity.setLink(name(bar.itself), entity);
        entity.setLink(name(bar.myEnum), enumValue);
        return entity;
    }

    public Entity save(MyEnum output, PersistentStoreTransaction txn) {
        final PersistentEntity entity = txn.newEntity(myEnum.getType().getSimpleName());
        setProperty(entity, myEnum.number, output.number);
        return entity;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable> T getProperty(Entity entity, Path<T> path) {
        return (T) entity.getProperty(name(path));
    }

    protected Entity getLink(Entity input, Path<?> path) {
        return input.getLink(name(path));
    }

    public static <T extends Comparable> boolean setProperty(Entity entity, Path<T> path, T value) {
        if (value == null) {
            return entity.deleteProperty(name(path));
        }
        return entity.setProperty(name(path), value);
    }

    @NotNull
    private static String type(EntityPath clazz) {
        return clazz.getType().getSimpleName();
    }

    @NotNull
    private static String name(Path path) {
        return path.getMetadata().getElement().toString();
    }
}
