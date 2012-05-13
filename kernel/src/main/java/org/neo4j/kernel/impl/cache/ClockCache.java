/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ClockCache<K, V>
{
    private final Queue<Page<V>> clock = new ConcurrentLinkedQueue<Page<V>>();
    private final ConcurrentHashMap<K, Page<V>> cache = new ConcurrentHashMap<K, Page<V>>();
    private final int maxSize;
    private final AtomicInteger currentSize = new AtomicInteger( 0 );
    private final String name;
    private static final AtomicInteger foo = new AtomicInteger( 0 );

    public ClockCache( String name, int size )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "name cannot be null" );
        }
        if ( size <= 0 )
        {
            throw new IllegalArgumentException( size + " is not > 0" );
        }
        this.name = name;
        this.maxSize = size;
    }

    public synchronized void put( K key, V value )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key not allowed" );
        }
        if ( value == null )
        {
            throw new IllegalArgumentException( "null value not allowed" );
        }
        Page<V> theValue = cache.get( key );
        if ( theValue == null )
        {
//            synchronized ( this )
            {
                theValue = new Page<V>();
                if ( cache.putIfAbsent( key, theValue ) == null )
                {
                    clock.offer( theValue );
                    System.out.println( "Size is " + foo.incrementAndGet() );
                }
                else
                {
                    System.out.println( "Ouch, for key " + key );
                }
            }
        }
        V myValue = null;
        while ( !theValue.value.compareAndSet( myValue, value ) )
        {
            // System.out.println( "impressive: " + myValue + " vs " +
            // theValue.value.get() );
            myValue = theValue.value.get();
        }
        if ( myValue == null )
        {
//            synchronized ( this )
            {
                if ( currentSize.incrementAndGet() > maxSize )
                {
                    evict();
                }
                assert currentSize.get() <= maxSize : "put: " + currentSize.get();
            }
        }
        theValue.flag.set( true );
    }

    public V get( K key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "cannot get null key" );
        }
        Page<V> theElement = cache.get( key );
        if ( theElement == null || theElement.value.get() == null )
        {
            return null;
        }
        V theValue = theElement.value.get();
        theElement.flag.set( true );
        if ( theValue == null )
        {
            System.out.println( "hey, candy!" );
            theElement.flag.set( false );
        }
        return theValue;
    }

    private void checkSize()
    {
        while ( currentSize.get() > maxSize )
        {
            evict();
        }
    }

    private void evict()
    {
        if ( currentSize.get() > maxSize + 1 )
        {
            System.out.println( Thread.currentThread().getName() + " getting to evict, count is " + currentSize.get() );
        }
        int countBefore = 0;
        for ( Page<V> page : clock )
        {
            // System.out.println( "\t" + page );
            countBefore++;
        }
        Page<V> theElement = null;/*clock.peek();
                                  if ( theElement == null )
                                  {
                                  System.out.println( "wanted to evict but queue was empty: " + currentSize.get() );
                                  return;
                                  }
                                  int pageStartedAt = theElement.instanceNumber;
                                  clock.offer( clock.poll() );*/
        int maxMet = -1;
        while ( ( theElement = clock.poll() ) != null && currentSize.get() > maxSize /*&& theElement.instanceNumber != pageStartedAt*/)
        {
            if ( maxMet < theElement.instanceNumber )
            {
                maxMet = theElement.instanceNumber;
                // System.out.println( "just met " + maxMet );
            }
            if ( !theElement.flag.compareAndSet( true, false ) )
            {
                int myCurrentSize = currentSize.get();
                assert !theElement.flag.get();
                V valueCleaned = theElement.value.get();
                if ( valueCleaned == null)
                {
                    // System.out.println( "was already gone" );
                    theElement.flag.set( false );
                }
                else if ( !theElement.value.compareAndSet( valueCleaned, null ) )
                {
                    System.out.println( "missed a chance" );
                    // clock.offer( theElement );
                }
                else
                {
                    assert theElement.value.get() == null;
                    // Page<V> fromQueue = findValueInClock(
                    // theElement.instanceNumber );
                    // assert fromQueue != null : "for instance number " +
                    // theElement.instanceNumber;
                    // assert !fromQueue.flag.get();
                    // assert fromQueue.value.get() == null;
                    elementCleaned( valueCleaned );
                    int myCurrentSizeNow = currentSize.get();
                    assert myCurrentSize == myCurrentSizeNow : myCurrentSize + " vs " + myCurrentSizeNow;
                    currentSize.decrementAndGet();
                    myCurrentSizeNow = currentSize.get();
                    assert myCurrentSizeNow == myCurrentSize - 1 : myCurrentSize + " vs " + myCurrentSizeNow + " 2";
                    // System.out.println( "I am " +
                    // Thread.currentThread().getName() + " and my count is "
                    // + myCurrentSizeNow );
                    // return;
                    // clock.offer( theElement );
                }
            }
            if ( !clock.offer( theElement ) )
            {
                System.out.println( "What the hell?" );
            }
        }
        if ( theElement != null )
        {
            clock.offer( theElement );
        }
        assert currentSize.get() <= maxSize : "evict: " + currentSize.get();
        // System.out.println( Thread.currentThread().getName() +
        // " done evicting, count is " + currentSize.get() );
        int countAfter = 0;
        for ( Page<V> page : clock )
        {
            // System.out.println( "\t" + page );
            countAfter++;
        }
        if ( countBefore != countAfter )
        {
            System.out.println( "Hey, wrong count, " + countBefore + " vs " + countAfter );
            System.exit( 0 );
        }
    }

    protected void elementCleaned( V element )
    {
        // System.out.println( "Cleared " + element );
    }

    public Set<K> keySet()
    {
        return cache.keySet();
    }

    public Collection<V> values()
    {
        Set<V> toReturn = new HashSet<V>();
        for ( Page<V> page : cache.values() )
        {
            if ( page.value.get() != null )
            {
                toReturn.add( page.value.get() );
            }
        }
        return toReturn;
    }

    public synchronized Set<Map.Entry<K, V>> entrySet()
    {
        Map<K, V> temp = new HashMap<K, V>();
        int counter = 0;
        for ( K key : cache.keySet() )
        {
            Page<V> value = cache.get( key );
            // Page<V> fromQueue = findValueInClock( value.instanceNumber );
            if ( value.value.get() != null )
            {
                // assert fromQueue.instanceNumber == value.instanceNumber;
                // assert fromQueue != null : "Got null from queue for " +
                // value.instanceNumber;
                // assert value == fromQueue;
                // assert value.value.get().equals( fromQueue.value.get() );
                temp.put( key, value.value.get() );
                counter++;
                // if ( counter > maxSize )
                // {
                // System.out.println( "Trouble in paradise: " );
                // for ( Page<V> page : clock )
                // {
                // System.out.println( "\t" + page );
                // }
                // }
            }
        }
        // System.out.println( "Trouble in paradise: " );
        // for ( Page<V> page : clock )
        // {
        // System.out.println( "\t" + page );
        // }
        return temp.entrySet();
    }

    public V remove( K key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "cannot remove null key" );
        }
        Page<V> toRemove = cache.remove( key );
        if ( toRemove == null || toRemove.value == null )
        {
            return null;
        }
        V toReturn = toRemove.value.get();
        toRemove.value.compareAndSet( toReturn, null );
        toRemove.flag.set( false );
        return toReturn;
    }

    private Page<V> findValueInClock( int instanceNumber )
    {
        for ( Page<V> page : cache.values() )
        {
            if ( page.instanceNumber == instanceNumber )
            {
                return page;
            }
        }
        return null;
    }

    public String getName()
    {
        return name;
    }

    public void clear()
    {
        cache.clear();
        clock.clear();
        currentSize.set( 0 );
    }

    public int size()
    {
        return currentSize.get();
    }

    private static class Page<E>
    {
        private static final AtomicInteger instanceCounter = new AtomicInteger( 0 );
        final AtomicBoolean flag = new AtomicBoolean( true );
        final AtomicReference<E> value = new AtomicReference<E>();
        final int instanceNumber = instanceCounter.incrementAndGet();

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( !( obj instanceof Page ) )
            {
                return false;
            }
            Page<?> other = (Page<?>) obj;
            if ( value == null )
            {
                return other.value == null;
            }
            return value.equals( other.value );
        }

        @Override
        public int hashCode()
        {
            return value == null ? 0 : value.hashCode();
        }

        @Override
        public String toString()
        {
            return ( value.get() != null ? "->" : "" ) + "[Page: " + instanceNumber + ", flag: " + flag + ", value: "
                   + value.get() + "]";
        }
    }
}
