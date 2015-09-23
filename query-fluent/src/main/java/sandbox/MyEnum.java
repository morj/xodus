package sandbox;

import com.querydsl.core.annotations.QueryEntity;

// TODO: move those to separate solution or apply QClass generation for tests
@QueryEntity
public enum MyEnum {
    FIRST(1),
    SECOND(2);

    public int number;

    MyEnum(int number) {
        this.number = number;
    }
}
