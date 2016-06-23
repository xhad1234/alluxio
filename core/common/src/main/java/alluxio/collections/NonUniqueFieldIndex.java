/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An interface extending {@link FieldIndex}, represents a non-unique index. A non-unique index is
 * an index where a index value can map to one or more objects.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
class NonUniqueFieldIndex<T> implements FieldIndex<T> {
  private final IndexDefinition<T> mIndexDefinition;
  private ConcurrentHashMap<Object, ConcurrentHashSet<T>> mIndexMap;

  /**
   * Constructs a new {@link NonUniqueFieldIndex} instance.
   */
  public NonUniqueFieldIndex(IndexDefinition<T> indexDefinition) {
    mIndexMap = new ConcurrentHashMap<Object, ConcurrentHashSet<T>>(8, 0.95f, 8);
    mIndexDefinition = indexDefinition;
  }

  @Override
  public void add(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);

    ConcurrentHashSet<T> objSet = mIndexMap.get(fieldValue);

    // Update the indexes.
    if (objSet == null) {
      mIndexMap.putIfAbsent(fieldValue, new ConcurrentHashSet<T>());
      objSet = mIndexMap.get(fieldValue);
    }

    objSet.add(object);
  }

  @Override
  public boolean remove(T object) {
    Object fieldValue = mIndexDefinition.getFieldValue(object);
    ConcurrentHashSet<T> objSet = mIndexMap.get(fieldValue);
    if (objSet != null) {
      return objSet.remove(object);
    }
    return false;
  }

  @Override
  public boolean contains(Object value) {
    return mIndexMap.containsKey(value);
  }

  @Override
  public Set<T> getByField(Object value) {
    Set<T> set = mIndexMap.get(value);
    return set == null ? new HashSet<T>() : set;
  }

  @Override public T getFirst(Object value) {
    Set<T> all = mIndexMap.get(value);
    try {
      return all == null || !all.iterator().hasNext() ? null : all.iterator().next();
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}