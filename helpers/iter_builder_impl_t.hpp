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

#include "iter_builder.hpp"

#include <s1o/transforms/transform_get_meta.hpp>
#include <s1o/transforms/transform_get_element_slot.hpp>
#include <s1o/transforms/transform_ds_get_element.hpp>

#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/counting_iterator.hpp>

namespace s1o {
namespace helpers {

template <typename dataset_impl>
struct iter_builder<dataset_impl, true>
{
    /** The transform used to get a the metadata of an element given its
        metadata-data pair. */
    typedef transform_get_meta<
        typename dataset_impl::TSpatialAdapterImpl,
        typename dataset_impl::spatial_storage_iterator,
        typename dataset_impl::metadata_type
        > transform_l_get_meta;

    /** The metadata-data pair transform type required by the trait. */
    typedef void transform_l_get_elem;

    /** The transform used to get a the metadata-pair of an element with slot
        offset given its metadata-data pair. */
    typedef transform_get_element_slot<
        typename dataset_impl::TSpatialAdapterImpl,
        typename dataset_impl::spatial_storage_iterator,
        typename dataset_impl::node_data
        > transform_l_get_elem_slot;

    /** The transform used to get a the metadata of an element given its
        metadata-data pair in queries. */
    typedef transform_get_meta<
        typename dataset_impl::TSpatialAdapterImpl,
        typename dataset_impl::spatial_storage_query_iterator,
        typename dataset_impl::metadata_type
        > transform_q_get_meta;

    /** The metadata-data pair transform type required by the trait. */
    typedef void transform_q_get_elem;

    /** The transform used to get a the metadata-pair of an element with slot
        offset given its metadata-data pair in queries. */
    typedef transform_get_element_slot<
        typename dataset_impl::TSpatialAdapterImpl,
        typename dataset_impl::spatial_storage_query_iterator,
        typename dataset_impl::node_data
        > transform_q_get_elem_slot;

    /** The initialization data transform type used to get a metadata-data
        pair in the dataset given its uid. */
    typedef transform_ds_get_element<
        dataset_impl
        > transform_d_get_elem;

    /** The metadata-data pair update transform type required by the trait. */
    typedef void transform_u_get_elem;

    /** The uid iterator used to generate sequence of uids for elements in
        the dataset. */
    typedef boost::counting_iterator<
        uid_t
        > uid_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata objects. */
    typedef boost::transform_iterator<
        transform_l_get_meta,
        typename dataset_impl::spatial_storage_iterator,
        typename transform_l_get_meta::reference,
        typename transform_l_get_meta::value_type
        > meta_l_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs without slot offset. */
    typedef typename dataset_impl::spatial_storage_iterator
        elem_l_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs with slot offset. */
    typedef boost::transform_iterator<
        transform_l_get_elem_slot,
        typename dataset_impl::spatial_storage_iterator,
        typename transform_l_get_elem_slot::reference,
        typename transform_l_get_elem_slot::value_type
        > elem_l_iterator_slot;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata objects in queries. */
    typedef boost::transform_iterator<
        transform_q_get_meta,
        typename dataset_impl::spatial_storage_query_iterator,
        typename transform_q_get_meta::reference,
        typename transform_q_get_meta::value_type
        > meta_q_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs without slot offset in queries. */
    typedef typename dataset_impl::spatial_storage_query_iterator
        elem_q_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs with slot offset in queries. */
    typedef boost::transform_iterator<
        transform_q_get_elem_slot,
        typename dataset_impl::spatial_storage_query_iterator,
        typename transform_q_get_elem_slot::reference,
        typename transform_q_get_elem_slot::value_type
        > elem_q_iterator_slot;

    /** The type of the iterator contaning initialization data for the spatial
        storage. */
    typedef boost::transform_iterator<
        transform_d_get_elem,
        uid_iterator,
        typename transform_d_get_elem::reference,
        typename transform_d_get_elem::value_type
        > elem_d_iterator;

    /** The type of the iterator for updating the metadata-data pairs from the
        spatial storage. */
    typedef typename dataset_impl::spatial_storage_update_iterator
        elem_u_iterator;

    /**
     * @brief Construct an iterator from the dataset to be used outside it.
     *
     * @tparam ITO The type of the resulting iterator.
     * @tparam TR The type of the transform used to construct the iterator.
     * @tparam ITI The type of the input iterator.
     *
     * @param iti The input iterator.
     * @param ds The dataset used to store the data.
     *
     * @return ITO An iterator that gives access to the elements of the
     * dataset.
     */
    template <typename ITO, typename TR, typename ITI>
    static inline ITO construct_out_iterator(ITI iti, const dataset_impl* ds)
    {
        return ITO(iti, TR(ds));
    }

    /**
     * @brief Construct an iterator that will be used inside the dataset.
     *
     * @tparam ITO The type of the resulting iterator.
     * @tparam TR The type of the transform used to construct the iterator.
     * @tparam ITI The type of the input iterator.
     *
     * @param iti The input iterator.
     * @param ds The dataset used to store the data.
     *
     * @return ITO An iterator that binds an external iterator with the data
     * from the current dataset.
     */
    template <typename ITO, typename TR, typename ITI>
    static inline ITO construct_in_iterator(ITI iti, const dataset_impl* ds)
    {
        (void)ds;

        return ITO(iti);
    }

    /**
     * @brief Construct an iterator that will be used inside the dataset.
     *
     * @tparam ITO The type of the resulting iterator.
     * @tparam TR The type of the transform used to construct the iterator.
     * @tparam ITI The type of the input iterator.
     *
     * @param iti The input iterator.
     * @param ds The dataset used to store the data.
     * @param slot_off slot_offset The byte offset to be applied on the the
     * data pointer of an element in the dataset.
     *
     * @return ITO An iterator that binds an external iterator with the data
     * from the current dataset.
     */
    template <typename ITO, typename TR, typename ITI>
    static inline ITO construct_in_iterator(ITI iti, const dataset_impl* ds,
        size_t slot_off)
    {
        (void)ds;

        return ITO(iti, TR(slot_off));
    }
};

}}
