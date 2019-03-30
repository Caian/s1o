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

#include "rtree_disk_slim.hpp"

#include <s1o/types.hpp>
#include <s1o/exceptions.hpp>
#include <s1o/traits/num_spatial_dims.hpp>
#include <s1o/traits/spatial_value_type.hpp>
#include <s1o/traits/spatial_point_type.hpp>
#include <s1o/traits/spatial_adapter_impl.hpp>
#include <s1o/traits/spatial_storage_type.hpp>
#include <s1o/traits/spatial_storage_iterator_type.hpp>
#include <s1o/traits/spatial_storage_query_iterator_type.hpp>
#include <s1o/traits/spatial_storage_update_iterator_type.hpp>
#include <s1o/transforms/transform_deref.hpp>
#include <s1o/initialization_data/default_data.hpp>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>

#include <string>
#include <vector>
#include <iterator>
#include <algorithm>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A hybrid spatial adapter based on boost's rtree implementation with
 * tree persistence on disk to save time when opening the dataset, as well as
 * consume less memory.
 *
 * @tparam Params The parameters used to control the tree.
 * @tparam CoordSys THe coordinate system used by the tree.
 */
template <
    typename Params,
    typename CoordSys
    >
struct rtree_disk
{
    /** The parameters used to control the tree. */
    typedef Params params_t;

    /** The rtree slim adapter that handles the rtree. */
    typedef rtree_disk_slim<Params, CoordSys> rtree_adapter;

    /** The parameters used to control the creation of the mapped file. */
    typedef typename rtree_adapter::mparams_t mparams_t;

/**
 * @brief The implemnetation of the rtree spatial adapter.
 *
 * @tparam TData The type of the data stored in the structure.
 * @tparam TSVal The type of the individual spatial coordinates.
 * @tparam NSDims The number of spatial dimensions of the data.
 */
template <
    typename TData,
    typename TSVal,
    unsigned int NSDims
    >
struct spatial_adapter_impl
{
    /** The flag to indicate that this adapter can handle node data
        directly. */
    static const bool supports_element_pair = true;

    /** The specialization of the current type. */
    typedef spatial_adapter_impl<TData, TSVal, NSDims> this_type;

    /** The implementation of the base adapter that handles the rtree. */
    typedef typename traits::spatial_adapter_impl<
        rtree_adapter,
        TData,
        TSVal,
        NSDims
        >::type rtree_adapter_impl;

    /** The number of spatial dimensions used to locate the date. */
    static const unsigned int num_spatial_dims =
        s1o::traits::num_spatial_dims<rtree_adapter_impl>::value;

    /** The type used to represent the spatial variables. */
    typedef typename s1o::traits::spatial_value_type<
        rtree_adapter_impl
        >::type spatial_value_type;

    /** The spatial storage of the base rtree. */
    typedef typename s1o::traits::spatial_storage_type<
        rtree_adapter_impl
        >::type rtree_store;

    /** A point for the rtree implementation. */
    typedef typename traits::spatial_point_type<
        rtree_adapter_impl
        >::type spatial_point_type;

    /** The pair used to store the node data in memory.  */
    typedef std::vector<TData> rpair_vector;

    /** The struct containing information about the initialization process of
        the rvec. */
    struct initialization_info
    {
        /** The approximate number of bytes occupied by the rvec object. */
        size_t rvec_size_bytes;

        /**
         * @brief Construct a new initialization_info object.
         *
         */
        initialization_info(
        ) :
            rvec_size_bytes(0)
        {
        }
    };

    /** The spatial storage type required by the trait. */
    struct spatial_storage_type
    {
        /** The rtree stored in a file */
        rtree_store _rtree;

        /** The vector stored in-memory to hold the pointers to elements. */
        rpair_vector _rvec;

        /** The information about the initialization process of the rvec. */
        initialization_info _info;

        /**
         * @brief Construct a new spatial_storage_type object
         *
         */
        spatial_storage_type() :
            _rtree(),
            _rvec(),
            _info()
        {
        }

        /**
         * @brief Destroy the spatial_storage_type object.
         *
         */
        ~spatial_storage_type()
        {
        }
    };

    /** The constant iterator used to traverse the tree. */
    typedef typename s1o::traits::spatial_storage_iterator_type<
        rtree_adapter_impl
        >::type rtree_const_iterator;

    /** The constant iterator used to traverse queries in the tree. */
    typedef typename s1o::traits::spatial_storage_query_iterator_type<
        rtree_adapter_impl
        >::type rtree_const_query_iterator;

    /** The transform used to get the read-only reference of the dataset data
        using the index retrieved from the rtree. */
    typedef transform_deref<
        rtree_const_iterator,
        typename rpair_vector::const_iterator
        > transform_s_const_deref;

    /** The transform used to get the writable reference of the dataset data
        using the index retrieved from the rtree. */
    typedef transform_deref<
        rtree_const_iterator,
        typename rpair_vector::iterator
        > transform_s_deref;

    /** The transform used to get the read-only reference of the dataset data
        using the index retrieved from the rtree when doing queries. */
    typedef transform_deref<
        rtree_const_query_iterator,
        typename rpair_vector::const_iterator
        > transform_q_deref;

    /** The iterator used to traverse the spatial storage as read-only dataset
        data rather than indices to the rvec. */
    typedef boost::transform_iterator<
        transform_s_const_deref,
        rtree_const_iterator,
        typename transform_s_const_deref::reference,
        typename transform_s_const_deref::value_type
        > deref_s_const_iterator;

    /** The iterator used to traverse the spatial storage as writable dataset
        data rather than indices to the rvec. */
    typedef boost::transform_iterator<
        transform_s_deref,
        rtree_const_iterator,
        typename transform_s_deref::reference,
        typename transform_s_deref::value_type
        > deref_s_iterator;

    /** The iterator used to traverse queries in the spatial storage as
        read-only dataset data rather than indices to the rvec. */
    typedef boost::transform_iterator<
        transform_q_deref,
        rtree_const_query_iterator,
        typename transform_q_deref::reference,
        typename transform_q_deref::value_type
        > deref_q_iterator;

    /** The iterator type required by the trait. */
    typedef deref_s_const_iterator spatial_storage_iterator_type;

    /** The update iterator type required by the trait. */
    typedef deref_s_iterator spatial_storage_update_iterator_type;

    /** The query iterator type required by the trait. */
    typedef deref_q_iterator spatial_storage_query_iterator_type;

    /** The implementation of the base adapter that handles the rtree. */
    const rtree_adapter_impl _adapter_impl;

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param params The parameters used to control the tree.
     * @param mparams The parameters used to control the creation of the
     * mapped file.
     * @param file_extension The extension to use for the rfile.
     * @param memory_prefix The prefix to add to any construct operation in the
     * memory mapped file.
     */
    spatial_adapter_impl(
        const params_t& params=params_t(),
        const mparams_t& mparams=mparams_t(),
        const std::string& file_extension=".rids",
        const std::string& memory_prefix=""
    ) :
        _adapter_impl(
            params,
            mparams,
            file_extension,
            memory_prefix +
            "s1o::spatial_adapters::rtree_disk/")
    {
    }

    /**
     * @brief Check if the spatial storage is empty
     *
     * @param st A reference to the spatial storage object.
     *
     * @return true The spatial storage object is empty.
     * @return false The spatial storage object is not empty.
     */
    bool empty(
        const spatial_storage_type& st
    ) const
    {
        return _adapter_impl.empty(st._rtree);
    }

    /**
     * @brief Compare two spatial points.
     *
     * @param first The first spatial point.
     * @param second The second spatial point.
     *
     * @return true The coordinates of both points are equal.
     * @return false One or more coordinates of the points differ.
     */
    bool equals(
        const spatial_point_type& first,
        const spatial_point_type& second
    ) const
    {
        return _adapter_impl.equals(first, second);
    }

    /**
     * @brief Communicate any extra files that are created with
     * the spatial storage.
     *
     * @tparam ITO The type of the result iterator with the filenames.
     *
     * @param iter The result iterator with the filenames.
     *
     * @return size_t The number of extra files created with the
     * spatial storage.
     */
    template <typename ITO>
    size_t get_extra_files(
        const std::string& basename,
        ITO iter) const
    {
        return _adapter_impl.get_extra_files(basename, iter);
    }

    /**
     * @brief Initialize the spatial storage with a sequence of elements.
     *
     * @tparam ITN The type of the iterator for the sequence of TData elements
     * to be stored.
     * @tparam ITL The type of the iterator for the sequence of spatial
     * locations associated with each element.
     *
     * @param st The spatial storage object being initialized.
     * @param data The initialization data for the spatial storage.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of TData objects to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of TData objects to be stored.
     * @param locbegin The iterator pointing to the beginning of a sequence
     * of spatial locations associated with each element.
     * @param locend The iterator pointing to after the last element of a
     * sequence of spatial locations associated with each element.
     */
    template <typename ITN, typename ITL>
    void initialize(
        spatial_storage_type& st,
        const initialization_data::default_data& data,
        ITN nodebegin,
        ITN nodeend,
        ITL locbegin,
        ITL locend
    ) const
    {
        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t loc_count = std::distance(locbegin, locend);

        // Must not happen
        if (node_count != loc_count) {
            EX3_THROW(location_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(loc_count));
        }

        boost::counting_iterator<uid_t> idxbegin(0);
        boost::counting_iterator<uid_t> idxend(node_count);

        _adapter_impl.initialize(st._rtree, data, idxbegin,
            idxend, locbegin, locend);

        // Initialize the vector storage for the dataset nodes

        st._rvec.resize(node_count);
        std::copy(nodebegin, nodeend, st._rvec.begin());

        st._info.rvec_size_bytes = sizeof(TData) * st._rvec.size();
    }

    /**
     * @brief Get the boundaries of the data stored in the spatial storage.
     *
     * @param st The spatial storage object being initialized.
     * @param minpoint The smallest coordinates in the spatial storage.
     * @param maxpoint The largest coordinates in the spatial storage.
     */
    void bounds(
        const spatial_storage_type& st,
        spatial_point_type& minpoint,
        spatial_point_type& maxpoint
    ) const
    {
        _adapter_impl.bounds(st._rtree, minpoint, maxpoint);
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy the predicates.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param st A reference to the spatial storage object.
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     * @param begin The resulting iterator to the beginning of a sequence of
     * elements that satisfy the predicates.
     * @param end The resulting iterator to the end of a sequence of elements
     * that satisfy the predicates.
     */
    template<typename Predicates>
    void query(
        const spatial_storage_type& st,
        const Predicates& predicates,
        deref_q_iterator& begin,
        deref_q_iterator& end
    ) const
    {
        rtree_const_query_iterator qbegin, qend;

        _adapter_impl.query(st._rtree, predicates, qbegin, qend);

        begin = deref_q_iterator(qbegin, transform_q_deref(st._rvec.begin()));
        end = deref_q_iterator(qend, transform_q_deref(st._rvec.begin()));

    }

    /**
     * @brief Get the iterator to the beginning of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return deref_s_const_iterator The iterator to the beginning of the
     * spatial storage.
     */
    deref_s_const_iterator sbegin(
        const spatial_storage_type& st
    ) const
    {
        return deref_s_const_iterator(_adapter_impl.sbegin(st._rtree),
            transform_s_const_deref(st._rvec.begin()));
    }

    /**
     * @brief Get the iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return deref_s_const_iterator The iterator to the end of the spatial
     * storage.
     */
    deref_s_const_iterator send(
        const spatial_storage_type& st
    ) const
    {
        return deref_s_const_iterator(_adapter_impl.send(st._rtree),
            transform_s_const_deref(st._rvec.begin()));
    }

    /**
     * @brief Get writable the iterator to the beginning of the spatial
     * storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return deref_s_iterator The writable iterator to the beginning of the
     * spatial storage.
     */
    deref_s_iterator sbegin_update(
        spatial_storage_type& st
    ) const
    {
        return deref_s_iterator(_adapter_impl.sbegin(st._rtree),
            transform_s_deref(st._rvec.begin()));
    }

    /**
     * @brief Get the writable iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return deref_s_iterator The writable iterator to the end of the
     * spatial storage.
     */
    deref_s_iterator send_update(
        spatial_storage_type& st
    ) const
    {
        return deref_s_iterator(_adapter_impl.send(st._rtree),
            transform_s_deref(st._rvec.begin()));
    }

    /**
     * @brief Destroy the spatial storage, if necessary.
     *
     * @param st A reference to the spatial storage object.
     */
    void destroy(
        spatial_storage_type& st
    ) const
    {
        (void)st;
    }
};

};

}}
