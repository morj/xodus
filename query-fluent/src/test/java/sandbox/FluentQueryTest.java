package sandbox;

import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityStoreTestBase;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.query.QueryEngine;
import jetbrains.exodus.query.SortEngine;
import jetbrains.xodus.query.XodusQuery;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static jetbrains.exodus.entitystore.metadata.AssociationEndCardinality._0_1;
import static jetbrains.exodus.entitystore.metadata.MetaBuilder.*;
import static sandbox.QBar.bar;
import static sandbox.QMyEnum.myEnum;

public class FluentQueryTest extends EntityStoreTestBase {

    private QueryEngine queryEngine;
    private final Factory factory = new Factory();

    // TODO: tests for composite queries
    private XodusQuery<Bar> propertyEqual;
    private XodusQuery<Bar> propertyEqualNull;
    private XodusQuery<Bar> propertyNotNull;
    private XodusQuery<Bar> linkEqual;
    private XodusQuery<Bar> linkEqualNull;
    private XodusQuery<Bar> linkNotNull;
    private XodusQuery<Bar> concat;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // TODO: parse model from EntityPathBase inheritors (aka QClasses)
        queryEngine = new QueryEngine(model(
                clazz("Bar").
                        prop("lol", "int").
                        prop("s", "string").
                        link("itself", "Bar", _0_1).
                        // edge("self1", "Bar", _0_1, "self2", _0_1).
                                link("myEnum", "MyEnum", _0_1),
                enumeration("MyEnum").
                        prop("number", "int")
        ), getEntityStore());
        SortEngine sortEngine = new SortEngine();
        queryEngine.setSortEngine(sortEngine);
        sortEngine.setQueryEngine(queryEngine);
        prepare();
    }

    public void testSimple() {
        final Bar first = bars().where(
                bar.lol.eq(9).and(bar.s.eq("value"))
        ).iterator().next();
        assertEquals(9, first.lol);
        assertEquals("value", first.s);
    }

    public void testNot() {
        final Iterator<Bar> itr = bars().where(
                bar.lol.eq(9).not()
        ).iterator();
        final Bar first = itr.next();
        assertEquals(0, first.lol);
        assertEquals(MyEnum.FIRST, first.myEnum);
        final Bar second = itr.next();
        assertEquals(0, second.lol);
        assertEquals(MyEnum.FIRST, second.myEnum);
        assertFalse(itr.hasNext());
    }

    public void testSelectDistinct() {
        final Iterator<MyEnum> itr = bars().selectDistinct(bar.myEnum).iterator();
        assertEquals(MyEnum.FIRST, itr.next());
    }

    public void testSort() {
        final Iterator<MyEnum> itr = enums().orderBy(myEnum.number.desc()).iterator();
        final MyEnum first = itr.next();
        assertEquals(2, first.number);
        final MyEnum second = itr.next();
        assertEquals(1, second.number);
        assertFalse(itr.hasNext());
    }

    private void prepare() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final Entity e1 = factory.save(MyEnum.FIRST, txn);
        final Entity e2 = factory.save(MyEnum.SECOND, txn);

        factory.save(new Bar(0, null, MyEnum.FIRST), e1, txn);
        factory.save(new Bar(0, null, MyEnum.FIRST), e1, txn);
        factory.save(new Bar(9, "value", MyEnum.SECOND), e2, txn);

        propertyEqual = bars().where(bar.s.eq("value"));
        propertyEqualNull = bars().where(bar.s.isNull());
        propertyNotNull = bars().where(bar.s.isNotNull());
        linkEqual = bars().where(bar.myEnum.eq(MyEnum.FIRST));
        linkEqualNull = bars().where(bar.itself.isNull());
        linkNotNull = bars().where(bar.itself.isNotNull());
        // concat = new Concat(propertyEqual, linkNotNull);
    }

    @NotNull
    protected XodusQuery<Bar> bars() {
        return XodusQuery.create(queryEngine, factory, bar);
    }

    @NotNull
    protected XodusQuery<MyEnum> enums() {
        return XodusQuery.create(queryEngine, factory, myEnum);
    }

}
