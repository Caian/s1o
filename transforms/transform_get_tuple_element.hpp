/*
 * Copyright (C) 2019 Caian Benedicto <caianbene@gmail.com>
 *
 * This file is part of s1o.
 *
 * s1o is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * s1o is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with s1o.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <boost/tuple/tuple.hpp>

namespace s1o {

/**
 * @brief Functor used to retrieve a single element from a boost tuple.
 *
 * @tparam N The element index in the tuple.
 * @tparam T The tuple type, this must be known beforehand as a convenience
 * when plugging the functor to a boost transform_iterator.
 */
template <unsigned int N, typename T>
struct transform_get_tuple_element
{
    /** The input type for the functor. */
    typedef T input_type;

    /** The value type of the functor. */
    typedef typename boost::tuples::element<
        N,
        T
        >::type value_type;

    /** The reference type of the functor. */
    typedef const value_type& reference;

    /**
     * @brief Retrieve a single element at a specific index from a tuple
     * object.
     *
     * @param val The tuple object.
     *
     * @return reference A single element at a specific index from a tuple
     * object.
     */
    inline reference operator ()(
        const input_type& val
    ) const
    {
        return boost::get<N>(val);
    }
};

}
