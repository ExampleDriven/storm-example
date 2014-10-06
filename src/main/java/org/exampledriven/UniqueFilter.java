package org.exampledriven;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Peter_Szanto on 10/3/2014.
 */
public class UniqueFilter extends BaseFilter {

    Set<Object> items = new HashSet<>();

    @Override
    public boolean isKeep(TridentTuple tuple) {
        String item = tuple.getString(0);
        if (items.contains(item)) {
            return false;
        }

        items.add(item);
        return true;
    }
}
