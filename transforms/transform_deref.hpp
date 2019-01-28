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
 * along with GNU Make; see the file COPYING.  If not, write to
 * the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#pragma once

#include <iterator>

namespace s1o {

template <
    typename ITI,
    typename ITR
    >
struct transform_deref
{
    /** The input type for the functor. */
    typedef typename std::iterator_traits<
        ITI
        >::value_type input_value;

    /** The value type of the functor. */
    typedef typename std::iterator_traits<
        ITR
        >::value_type value_type;

    /** The reference type of the functor. */
    typedef typename std::iterator_traits<
        ITR
        >::reference reference;

    /** The reference iterator used to dereference the real return value. */
    ITR _ref_begin;

    /**
     * @brief Retrieve the value from the reference iterator.
     *
     * @param val The offset to apply to the reference iterator.
     *
     * @return reference The value from the reference iterator with an offset
     * applied.
     */
    inline reference operator ()(
        const input_value& val
    ) const
    {
        ITR ref = _ref_begin;
        std::advance(ref, val);
        return *ref;
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_deref() :
        _ref_begin()
    {
    }

    /**
     * @brief Construct a new transform object.
     *
     * @param refbegin The iterator pointing to the beginning of the reference
     * data.
     */
    inline transform_deref(
        ITR refbegin
    ) :
        _ref_begin(refbegin)
    {
    }
};

}
