package sandbox;

import com.querydsl.core.annotations.QueryEntity;

// TODO: move those to separate solution or apply QClass generation for tests
@QueryEntity
public class Bar extends Foo {

    public final int lol;
    public final String s;
    public final MyEnum myEnum;

    public Bar(int lol, String s, MyEnum myEnum) {
        this.lol = lol;
        this.s = s;
        this.myEnum = myEnum;
    }

    public Bar getItself() {
        return this;
    }
}
