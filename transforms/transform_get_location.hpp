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
 * @brief Functor used to retrieve the spatial point object from metadata.
 *
 * @tparam TMetaAdapter A helper type used to interface the metadata with the
 * dataset.
 * @tparam ITM The type of the iterator for the sequence of metadata
 * associated with each element.
 * @tparam spatial_point_type The type used by the spatial adapter to store
 * points in space.
 */
template <
    typename TMetaAdapter,
    typename ITM,
    typename spatial_point_type
    >
struct transform_get_location
{
    /** The input type for the functor. */
    typedef typename std::iterator_traits<
        ITM
        >::value_type input_type;

    /** The return type of the functor. */
    typedef spatial_point_type value_type;

    /** The reference type of the functor. */
    typedef value_type reference;

    /** The object used to retrieve information from the metadata. */
    const TMetaAdapter* _meta_adapter;

    /**
     * @brief Retrieve the spatial point object stored in the metadata.
     *
     * @param val A constant reference to the metadata.
     *
     * @return reference The spatial point of the element with the given
     * metadata.
     */
    inline reference operator ()(
        const input_type& val
    ) const
    {
        value_type location;
        _meta_adapter->get_location(val, location);

        return location;
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_get_location(
    ) :
        _meta_adapter(0)
    {
    }

    /**
     * @brief Construct a new transform object.
     *
     * @param meta_adapter The object used to retrieve information from the
     * metadata.
     */
    inline transform_get_location(
        const TMetaAdapter* meta_adapter
    ) :
        _meta_adapter(meta_adapter)
    {
    }
};

}
