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
namespace helpers {

/**
 * @brief Structure used to index tuples passed to the rtrees because the
 * default indexable_t type does not work properly with the zip_iterator.
 */
template <typename T>
struct rtree_indexer_byval
{
    /** The result type of the functor. */
    typedef const T& result_type;

    /**
     * @brief Retrieve the tuple element responsible for the indexing.
     *
     * @tparam V The type of the tuple.
     *
     * @param val The tuple to retrieve the element from.
     *
     * @return result_type The tuple element responsible for the indexing.
     */
    template <typename V>
    inline result_type operator()(
        const V& val
    ) const
    {
        return val.template get<0>();
    }
};

}}
