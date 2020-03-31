package org.greenplum.pxf.api.model;

import java.util.Iterator;

/**
 * An iterator for tuples, that can be cleaned up after being consumed
 *
 * @param <T> the tuple type
 */
public interface TupleIterator<T> extends Iterator<T> {

    /**
     * Perform any clean up operations after the iterator has been consumed
     *
     * @throws Exception throws an exception when an error occurs
     */
    void cleanup() throws Exception;
}