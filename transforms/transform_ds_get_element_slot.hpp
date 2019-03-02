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

/**
 * @brief Functor used to retrieve metadata-data object pairs from a dataset
 * given the uid of the elements.
 *
 * @tparam dataset_impl The spectialization of the dataset used to store the
 * elements.
 */
template <typename dataset_impl>
struct transform_ds_get_element_slot
{
    /** The input type for the functor. */
    typedef uid_t input_type;

    /** The return type of the functor. */
    typedef typename dataset_impl::element_pair value_type;

    /** The reference type of the functor. */
    typedef value_type reference;

    /** The internal metadata pointer type. */
    typedef typename dataset_impl::file_metadata* metadata_ptr;

    /** The data pointer type. */
    typedef char* data_ptr;

    /** The dataset object used to retrieve data. */
    const dataset_impl* _dataset;

    /** The offset in the data file for the selected slot. */
    size_t _slot_offset;

    /**
     * @brief Retrieve the metadata-data object pair stored in the element of
     * the dataset.
     *
     * @param val The uid of the element.
     *
     * @return value_type The metadata-data object pair stored in the element
     * of the dataset.
     */
    inline value_type operator ()(
        const input_type& val
    ) const
    {
        metadata_ptr elem = _dataset->
#if defined(S1O_SAFE_ITERATORS)
            get_element_address
#else
            _get_element_address
#endif
            (val);

        data_ptr data = _dataset->_fds.no_data() ? 0 : (_dataset->
#if defined(S1O_SAFE_ITERATORS)
            get_data_address
#else
            _get_data_address
#endif
            (elem) + _slot_offset);

        return value_type(elem, data);
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_ds_get_element_slot(
    ) :
        _dataset(0),
        _slot_offset(-1)
    {
    }

    /**
     * @brief Construct a new transform object.
     *
     * @param dataset The dataset used to store the elements.
     * @param slot_offset The byte offset to be applied on the the data
     * pointer.
     */
    inline transform_ds_get_element_slot(
        const dataset_impl* dataset,
        size_t slot_offset
    ) :
        _dataset(dataset),
        _slot_offset(slot_offset)
    {
    }
};

}
