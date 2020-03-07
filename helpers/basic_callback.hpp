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
 * @brief Functor that initializes a spatial storage object using the
 * initialization_data as parameter.
 *
 * @tparam TMetaAdapter A helper type used to interface the metadata with the
 * dataset.
 * @tparam TSpatialAdapterImpl The specialization type of the spatial adapter.
 * @tparam Storage The type of the spatial storage.
 * @tparam ITN The type of the iterator for the sequence of data elements to
 * be stored in the spatial storage.
 * @tparam ITM The type of the iterator for the sequence of metadata
 * associated with each element.
 */
template <
    typename TMetaAdapter,
    typename TSpatialAdapterImpl,
    typename Storage,
    typename ITN,
    typename ITM
    >
struct basic_callback
{
    /** The metadata adapter of the dataset. */
    const TMetaAdapter& _meta_adapter;

    /** The spatial adapter of the dataset. */
    const TSpatialAdapterImpl& _spatial_adapter;

    /** The spatial storage being initialized. */
    Storage& _storage;

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
     * @brief Construct a new basic_callback object
     *
     * @param meta_adapter The metadata adapter of the dataset.
     * @param spatial_adapter The spatial adapter of the dataset.
     * @param storage The spatial storage being initialized.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of data elements to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of data elements to be stored.
     * @param metabegin The iterator pointing to the beginning of a sequence of
     * metadata associated with each data element.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata associated with each data element.
     */
    basic_callback(
        const TMetaAdapter& meta_adapter,
        const TSpatialAdapterImpl& spatial_adapter,
        Storage& storage,
        ITN nodebegin,
        ITN nodeend,
        ITM metabegin,
        ITM metaend
    ) :
        _meta_adapter(meta_adapter),
        _spatial_adapter(spatial_adapter),
        _storage(storage),
        _nodebegin(nodebegin),
        _nodeend(nodeend),
        _metabegin(metabegin),
        _metaend(metaend)
    {
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
        _spatial_adapter.initialize(_storage, _meta_adapter, data,
            _nodebegin, _nodeend, _metabegin, _metaend);
    }
};

}}
