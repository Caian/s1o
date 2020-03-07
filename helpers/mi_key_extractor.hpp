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

#include <boost/fusion/sequence/intrinsic/at_c.hpp>
#include <boost/fusion/include/at_c.hpp>
#include <boost/tuple/tuple.hpp>

namespace s1o {
namespace helpers {

/**
 * @brief Helper struct to extract a specific key from a multiindex element
 * with index.
 *
 * @tparam T The type of the extracted key.
 * @tparam I The index of the key.
 *
 * @note The element type must be compatible with boost::get and return
 * a reference to the indices with boost::get<0>(element). Also the
 * resulting indices must be compatible with
 * boost::fusion::at_c<I>(indices).
 */
template <typename T, unsigned int I>
struct mi_key_extractor
{
    /** The value type of the functor. */
    typedef T result_type;

    /**
     * @brief Extract a specific key from an element.
     *
     * @tparam Q The type of the element to extract the key.
     *
     * @param q The element to extract the key.
     *
     * @return const result_type& A constant reference to the extracted key.
     *
     * @note The element type must be compatible with boost::get and return
     * a reference to the indices with boost::get<0>(element). Also the
     * resulting indices must be compatible with
     * boost::fusion::at_c<I>(indices).
     */
    template <typename Q>
    const result_type& operator ()(
        const Q& q
    ) const
    {
        using namespace boost;
        return fusion::at_c<I>(get<0>(q));
    }
};

}}
