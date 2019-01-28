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

/**
 * @brief Functor used to retrieve the metadata object from a spatial storage
 * node.
 *
 * @tparam TSpatialAdapterImpl The specialization of the helper type used to
 * interface the spatial storage structure with the dataset.
 * @tparam spatial_iterator The type of the spatial iterator used to retrieve
 * the nodes.
 * @tparam node_data The inner type used to store data in each node of the
 * spatial structure.
 */
template <
    typename TSpatialAdapterImpl,
    typename spatial_iterator,
    typename metadata_type
    >
struct transform_get_meta
{
    /** The input type for the functor. */
    typedef typename std::iterator_traits<
        spatial_iterator
        >::value_type input_type;

    /** The return type of the functor. */
    typedef metadata_type value_type;

    /** The reference type of the functor. */
    typedef value_type& reference;

    /**
     * @brief Retrieve the metadata object stored in a node of the spatial
     * storage.
     *
     * @param val A constant reference to the node.
     *
     * @return reference The metadata of the element represented by the node.
     */
    inline reference operator ()(
        const input_type& val
    ) const
    {
        return *val.first;
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_get_meta()
    {
    }
};

}
