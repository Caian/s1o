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

#include <iterator>

namespace s1o {

/**
 * @brief Functor used to retrieve the metadata-data pair from a spatial
 * storage node with slot support.
 *
 * @tparam TSpatialAdapterImpl The specialization of the helper type used to
 * interface the spatial storage structure with the dataset.
 * @tparam spatial_iterator The type of the spatial iterator used to retrieve
 * the nodes.
 * @tparam element_pair The inner type used to store data in each node of the
 * spatial structure.
 */
template <
    typename TSpatialAdapterImpl,
    typename spatial_iterator,
    typename element_pair
    >
struct transform_get_element_slot
{
    /** The input type for the functor. */
    typedef typename std::iterator_traits<
        spatial_iterator
        >::value_type input_type;

    /** The return type of the functor. */
    typedef element_pair value_type;

    /** The reference type of the functor. */
    typedef value_type reference;

    /** The offset in the data file for the selected slot. */
    size_t _slot_offset;

    /**
     * @brief Retrieve the metadata-data pair stored in a node of the
     * spatial storage.
     *
     * @param val A constant reference to the node.
     *
     * @return value_type The metadata-data pair of the element represented
     * by the node.
     */
    inline value_type operator ()(
        const input_type& val
    ) const
    {
        return element_pair(val.first,
            val.second + _slot_offset);
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_get_element_slot(
    ) :
        _slot_offset(-1)
    {
    }

    /**
     * @brief Construct a new transform object.
     *
     * @param slot_offset The byte offset to be applied on the the data
     * pointer.
     */
    inline transform_get_element_slot(
        size_t slot_offset
    ) :
        _slot_offset(slot_offset)
    {
    }
};

}
