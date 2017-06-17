package org.es.sql.bean;


public class SQLArgs {

    private Object[] args;

    public SQLArgs(Object[] args) {
        this.args = args;

        ensureAllNotNull();
    }

    private void ensureAllNotNull() {
        if (args == null) {
            throw new IllegalArgumentException("Sql args is null");
        }

        for (int idx = 0; idx < args.length; idx++) {
            if (args[idx] == null) {
                throw new IllegalArgumentException(
                        String.format("The sql arg[%s] is null", idx));
            }
        }
    }

    public Object[] getArgs() {
        return args;
    }

    public int getArgCount() {
        return args.length;
    }

    public Object get(int index) {
        if (index >= getArgCount()) {
            throw new IndexOutOfBoundsException(
                    String.format("Index[%s] Size[%s]", index, getArgCount()));
        }
        return args[index];
    }

    @SuppressWarnings("unchecked")
    public <T> T get(int index, Class<T> argClass) {
        if (index >= getArgCount()) {
            throw new IndexOutOfBoundsException(
                    String.format("Index[%s] Size[%s]", index, getArgCount()));
        }

        if (args[index].getClass().isAssignableFrom(argClass)) {
            return (T) get(index);
        }

        throw new IllegalArgumentException(
                String.format("Arg[%s] can not match type[%s]",
                        index, argClass.getName()));
    }
}
