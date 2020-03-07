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

#include <s1o/transforms/transform_get_location.hpp>

#include <boost/iterator/transform_iterator.hpp>

namespace s1o {
namespace helpers {

/**
 * @brief Helper class to generate spatial_point_type iterators from metadata
 * iterators.
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
struct location_iterator_helper
{
    typedef transform_get_location<
        TMetaAdapter,
        ITM,
        spatial_point_type
        > location_transform;

    typedef boost::transform_iterator<
        location_transform,
        ITM,
        typename location_transform::reference,
        typename location_transform::value_type
        > iterator;

    /** The transform used to retrieve the spatial point type using the
        metadata adapter. */
    location_transform _loc_transform;

    /**
     * @brief Construct a new location_iterator_helper object.
     *
     * @param meta_adapter The object used to retrieve information from the
     * metadata.
     */
    location_iterator_helper(
        const TMetaAdapter& meta_adapter
    ) :
        _loc_transform(&meta_adapter)
    {
    }

    /**
     * @brief Wrap a metadata iterator around a transform to retrieve the
     * spatial point type.
     *
     * @param it An iterator to a metadata sequence.
     *
     * @return iterator An iterator to a spatial_point_type sequence.
     */
    iterator operator ()(
        ITM it
    ) const
    {
        return iterator(it, _loc_transform);
    }
};

}}
