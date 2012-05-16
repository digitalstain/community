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
            theValue = new Page<V>();
            if ( cache.putIfAbsent( key, theValue ) == null )
            {
                clock.offer( theValue );
            }
            else
            {
                System.out.println( "Ouch, for key " + key );
            }
        }
        V myValue = theValue.value.get();
        theValue.flag.set( true );
        theValue.value.set( value );
        if ( myValue == null )
        {
            if ( currentSize.incrementAndGet() > maxSize )
            {
                evict();
            }
            assert currentSize.get() <= maxSize : "put: " + currentSize.get();
        }
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
            theElement.flag.set( false );
        }
        return theValue;
    }

    private void evict()
    {
        Page<V> theElement = null;
        while ( ( theElement = clock.poll() ) != null && currentSize.get() > maxSize )
        {
            if ( !theElement.flag.compareAndSet( true, false ) )
            {
                V valueCleaned = theElement.value.get();
                if ( valueCleaned == null)
                {
                    theElement.flag.set( false );
                }
                else if ( theElement.value.compareAndSet( valueCleaned, null ) )
                {
                    elementCleaned( valueCleaned );
                    currentSize.decrementAndGet();
                }
            }
            clock.offer( theElement );
        }
        if ( theElement != null )
        {
            clock.offer( theElement );
        }
    }

    protected void elementCleaned( V element )
    {
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
            if ( value.value.get() != null )
            {
                temp.put( key, value.value.get() );
                counter++;
            }
        }
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
        final AtomicBoolean flag = new AtomicBoolean( true );
        final AtomicReference<E> value = new AtomicReference<E>();

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
            return ( value.get() != null ? "->" : "" ) + "[Flag: " + flag + ", value: " + value.get() + "]";
        }
    }
}
