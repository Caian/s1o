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

#include <s1o/exceptions.hpp>

namespace s1o {

/**
 * @brief Functor used to read metadata objects from the dataset's metadata
 * file given the uid of the elements.
 *
 * @tparam dataset_impl The spectialization of the dataset used to store the
 * elements.
 */
template <typename dataset_impl>
struct transform_ds_read_meta
{
    /** The input type for the functor. */
    typedef uid_t input_type;

    /** The return type of the functor. */
    typedef typename dataset_impl::metadata_type value_type;

    /** The reference type of the functor. */
    typedef value_type reference;

    /** The dataset object used to retrieve data. */
    const dataset_impl* _dataset;

    /**
     * @brief Read the metadata object stored in the dataset given its uid.
     *
     * @param val The uid of the element.
     *
     * @return reference The metadata object read from the dataset.
     */
    inline reference operator ()(
        const input_type& val
    ) const
    {
        value_type meta;

        if (!_dataset->read_element(val, meta)) {
            EX3_THROW(invalid_uid_exception()
                << requested_uid(val)
                << dataset_name(_dataset->get_basename()));
        }

        return meta;
    }

    /**
     * @brief Construct a new transform object.
     *
     */
    inline transform_ds_read_meta(
    ) :
        _dataset(0)
    {
    }

    /**
     * @brief Construct a new transform object.
     *
     * @param dataset The dataset used to store the elements.
     */
    inline transform_ds_read_meta(
        const dataset_impl* dataset
    ) :
        _dataset(dataset)
    {
    }
};

}
