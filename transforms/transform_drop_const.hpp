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

namespace s1o {

/**
 * @brief Functor used to remove the const qualifier from a reference so it can
 * be modified.
 *
 * @tparam Value The type being passed as reference.
 */
template <typename Value>
struct transform_drop_const
{
    /** The input type for the functor. */
    typedef Value input_value;

    /** The value type of the functor. */
    typedef Value value_type;

    /** The reference type of the functor. */
    typedef Value& reference;

    /**
     * @brief Retrieve a reference to the input value with the const qualifier.
     *
     * @param val A constant reference to the value.
     *
     * @return reference A reference to the input value with the const
     * qualifier.
     */
    inline reference operator ()(
        const input_value& val
    ) const
    {
        return const_cast<reference>(val);
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_drop_const()
    {
    }
};

}
