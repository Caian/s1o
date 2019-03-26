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

namespace s1o {
namespace helpers {

/**
 * @brief functor that initializes the objects using the initialization_data
 * as parameter.
 *
 * @tparam AdapterImpl The specialization type of the spatial adapter.
 * @tparam Storage The type of the spatial storage.
 * @tparam ITN The type of the iterator for the sequence of data elements to
 * be stored in the spatial storage.
 * @tparam ITL The type of the iterator for the sequence of spatial locations
 * associated with each data element.
 */
template <typename AdapterImpl, typename Storage, typename ITN, typename ITL>
struct basic_callback
{
    const AdapterImpl& _adapter;
    Storage& _storage;

    const ITN _nodebegin;
    const ITN _nodeend;
    const ITL _locbegin;
    const ITL _locend;

    /**
     * @brief Construct a new basic_callback object
     *
     * @param adapter The spatial adapter of the dataset.
     * @param storage The spatial storage being initialized.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of data elements to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of data elements to be stored.
     * @param locbegin The iterator pointing to the beginning of a sequence
     * of spatial locations associated with each data element.
     * @param locend The iterator pointing to after the last element of a
     * sequence of spatial locations associated with each data element.
     */
    basic_callback(
        const AdapterImpl& adapter,
        Storage& storage,
        ITN nodebegin,
        ITN nodeend,
        ITL locbegin,
        ITL locend
    ) :
        _adapter(adapter),
        _storage(storage),
        _nodebegin(nodebegin),
        _nodeend(nodeend),
        _locbegin(locbegin),
        _locend(locend)
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
        _adapter.initialize(_storage, data, _nodebegin,
            _nodeend, _locbegin, _locend);
    }
};

}}
