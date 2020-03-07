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

#include <s1o/transforms/transform_get_tuple_element.hpp>

#include <boost/iterator/transform_iterator.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/mpl/integral_c.hpp>

#include <iterator>

namespace s1o {
namespace helpers {

/**
 * @brief Functor that initializes a series of spatial storage objects using
 * the initialization_data as parameter.
 *
 * @tparam TMetaAdapter A helper type used to interface the metadata with the
 * dataset.
 * @tparam TSpatialAdapterImplsTie A tuple of references to specializations of
 * spatial adapters used to initialize the spatial storages.
 * @tparam StoragesTie A tuple of references to the spatial storage types, of
 * for each spatial adapter.
 * @tparam ITN The type of the iterator for the sequence of data elements to
 * be stored in the spatial storages.
 * @tparam ITM The type of the iterator for the sequence of metadata
 * associated with each element.
 */
template <
    typename TMetaAdapter,
    typename TSpatialAdapterImplsTie,
    typename StoragesTie,
    typename ITN,
    typename ITM
    >
struct tuple_callback
{
    /** The integral type of the last index in the tuple. */
    typedef boost::mpl::integral_c<
        unsigned int,
        boost::tuples::length<TSpatialAdapterImplsTie>::value
        > last_t;

    /** The type of the metadata being passed by the metadata iterator. */
    typedef typename std::iterator_traits<
        ITM
        >::value_type metadata_type;

    /** The metadata adapter of the dataset. */
    const TMetaAdapter& _meta_adapter;

    /** A tuple of references to the spatial adapters of the dataset.*/
    const TSpatialAdapterImplsTie _spatial_adapters;

    /** A tuple of references to the spatial storages being initialized. */
    StoragesTie _storages;

    /** The iterator pointing to the beginning of a sequence of data elements
        to be stored. */
    const ITN _nodebegin;

    /** The iterator pointing to after the last element of sequence of data
        elements to be stored. */
    const ITN _nodeend;

    /** The iterator pointing to the beginning of a sequence of metadata
        associated with each data element. */
    const ITM _metabegin;

    /** The iterator pointing to after the last element of a sequence of
        metadata associated with each data element. */
    const ITM _metaend;

    /**
     * @brief Construct a new tuple_callback object
     *
     * @param meta_adapter The metadata adapter of the dataset.
     * @param adapters A boost tie with the spatial adapters of the dataset.
     * @param storages A boost tie with the spatial storages being
     * initialized.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of data elements to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of data elements to be stored.
     * @param metabegin The iterator pointing to the beginning of a sequence of
     * metadata associated with each data element.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata associated with each data element.
     */
    tuple_callback(
        const TMetaAdapter& meta_adapter,
        const TSpatialAdapterImplsTie& spatial_adapters,
        const StoragesTie& storages,
        ITN nodebegin,
        ITN nodeend,
        ITM metabegin,
        ITM metaend
    ) :
        _meta_adapter(meta_adapter),
        _spatial_adapters(spatial_adapters),
        _storages(storages),
        _nodebegin(nodebegin),
        _nodeend(nodeend),
        _metabegin(metabegin),
        _metaend(metaend)
    {
    }

    /**
     * @brief Recursively apply the initialization data to the current index in
     * the spatial storage tuple and the following indices.
     *
     *
     * @tparam InitData The type of initialization_data object.
     * @tparam I The integral type of the current index in the tuple.
     *
     * @param data The initialization_data object.
     * @param tag The tag used to identify the current index in the tuple.
     */
    template <typename InitData, typename I>
    void apply(
        const InitData& data,
        const I& tag=I()
    ) const
    {
        typedef boost::mpl::integral_c<
            unsigned int,
            I::value + 1
            > next_t;

        boost::get<I::value>(_spatial_adapters).initialize(
            boost::get<I::value>(_storages), _meta_adapter, data,
            _nodebegin, _nodeend, _metabegin, _metaend);

        next_t next;
        apply(data, next);
    }

    /**
     * @brief The tail of the apply template recursion.
     *
     * @tparam InitData The type of initialization_data object.
     *
     * @param data The initialization_data object.
     * @param tag The tag used to identify the tail.
     */
    template <typename InitData>
    void apply(
        const InitData& data,
        const last_t& tag=last_t()
    ) const
    {
        (void)data;
        (void)tag;
    }

    /**
     * @brief Initialize a spatial storage using its associated adapter.
     *
     * @tparam InitData The type of initialization_data object.
     *
     * @param data The initialization_data object.
     */
    template <typename InitData>
    void operator ()(
        const InitData& data
    )
    {
        typedef boost::mpl::integral_c<
            unsigned int,
            0
            > first_t;

        first_t first;
        apply(data, first);
    }
};

}}
