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

#include "rtree_base.hpp"

#include <s1o/types.hpp>
#include <s1o/exceptions.hpp>
#include <s1o/helpers/basic_callback.hpp>
#include <s1o/helpers/mapped_file_helper.hpp>
#include <s1o/traits/num_spatial_dims.hpp>
#include <s1o/traits/spatial_value_type.hpp>
#include <s1o/traits/spatial_point_type.hpp>
#include <s1o/traits/spatial_adapter_impl.hpp>
#include <s1o/traits/spatial_storage_type.hpp>
#include <s1o/traits/spatial_storage_iterator_type.hpp>
#include <s1o/traits/spatial_storage_query_iterator_type.hpp>
#include <s1o/traits/spatial_storage_update_iterator_type.hpp>
#include <s1o/initialization_data/default_data.hpp>
#include <s1o/initialization_data/mapped_data.hpp>

#include <string>
#include <iterator>
#include <algorithm>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A slim spatial adapter based on boost's rtree implementation with
 * tree persistence on disk to save time when opening the dataset, as well as
 * consume less memory. This adapter does not handle pointers to mapped data
 * as an aggressive memory saving strategy.
 *
 * @tparam Params The parameters used to control the tree.
 * @tparam CoordSys The coordinate system used by the tree.
 */
template <
    typename Params,
    typename CoordSys
    >
struct rtree_disk_slim
{
    /** The parameters used to control the tree. */
    typedef Params params_t;

    /** The base rtree adapter that handles the rtree. */
    typedef rtree_base<
        params_t,
        CoordSys,
        helpers::mapped_file_helper::allocator_t
        > rtree_adapter;

    /** The parameters used to control the creation of the mapped file. */
    typedef helpers::mapped_file_helper::params_t mparams_t;

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
    static const bool supports_element_pair = false;

    /** The specialization of the current type. */
    typedef spatial_adapter_impl<TData, TSVal, NSDims> this_type;

    /** The implementation of the base adapter that handles the rtree. */
    typedef typename traits::spatial_adapter_impl<
        rtree_adapter,
        uid_t,
        TSVal,
        NSDims
        >::type rtree_adapter_impl;

    /** The number of spatial dimensions used to locate the data. */
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

    /** The struct containing information about the initialization process of
        the rtree. */
    struct initialization_info
    {
        /** The number of allocated bytes for the rtree inside the mapped
            file. */
        size_t rtree_size_bytes;

        /** Information about the underlying memory mapped file. */
        helpers::mapped_file_helper::initialization_info mapped_file;

        /**
         * @brief Construct a new initialization_info object.
         *
         */
        initialization_info(
        ) :
            rtree_size_bytes(0),
            mapped_file()
        {
        }
    };

    /** The spatial storage type required by the trait. */
    struct spatial_storage_type
    {
        /** The rtree object stored in the mapped file. */
        rtree_store _rtree;

        /** The memory mapped data required to be stored. */
        helpers::mapped_file_helper::mapped_storage _mstorage;

        /** The information about the initialization process of the mapped file
            and rtree. */
        initialization_info _info;

        /**
         * @brief Construct a new spatial_storage_type object.
         *
         */
        spatial_storage_type() :
            _rtree(0),
            _mstorage(),
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

    /** The iterator type required by the trait. */
    typedef typename s1o::traits::spatial_storage_iterator_type<
        rtree_adapter_impl
        >::type spatial_storage_iterator_type;

    /** The update iterator type required by the trait. */
    typedef typename s1o::traits::spatial_storage_update_iterator_type<
        rtree_adapter_impl
        >::type spatial_storage_update_iterator_type;

    /** The query iterator type required by the trait. */
    typedef typename s1o::traits::spatial_storage_query_iterator_type<
        rtree_adapter_impl
        >::type spatial_storage_query_iterator_type;

    /** The implementation of the base adapter that handles the rtree. */
    const rtree_adapter_impl _adapter_impl;

    /** The object that handles memory mapped file initializations. */
    const helpers::mapped_file_helper _file_helper;

    /** The parameters used to control the creation of the mapped file. */
    const helpers::mapped_file_helper::params_t _mparams;

    /** The extension to use for the rfile. */
    const std::string _file_extension;

    /** The memory prefix used when constructing objects in the file. */
    const std::string _memory_prefix;

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
        const std::string& file_extension=".ridx",
        const std::string& memory_prefix=""
    ) :
        _adapter_impl(params),
        _file_helper(),
        _mparams(mparams),
        _file_extension(file_extension),
        _memory_prefix(memory_prefix +
            "s1o::spatial_adapters::rtree_disk_slim/")
    {
    }

    /**
     * @brief Get the path and filename of the file used to store the tree.
     *
     * @param basename The path and filename of the dataset without the
     * trailing extensions.
     *
     * @return std::string The path and filename of the file used to store the
     * tree.
     */
    std::string get_rindex_name(
        const std::string& basename
    ) const
    {
        return basename + _file_extension;
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
        *iter = get_rindex_name(basename);
        iter++;
        return 1;
    }

    /**
     * @brief Initialize the spatial storage with a sequence of elements.
     *
     * @tparam TMetaAdapter A helper type used to interface the metadata with
     * the dataset.
     * @tparam ITN The type of the iterator for the sequence of TData elements
     * to be stored.
     * @tparam ITM The type of the iterator for the sequence of metadata
     * associated with each element.
     *
     * @param st The spatial storage object being initialized.
     * @param meta_adapter The object used to retrieve information from the
     * metadata.
     * @param data The initialization data for the spatial storage.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of TData objects to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of TData objects to be stored.
     * @param metabegin The iterator pointing to the beginning of a sequence
     * of metadata associated with each element.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata associated with each element.
     */
    template <typename TMetaAdapter, typename ITN, typename ITM>
    void initialize(
        spatial_storage_type& st,
        const TMetaAdapter& meta_adapter,
        const initialization_data::default_data& data,
        ITN nodebegin,
        ITN nodeend,
        ITM metabegin,
        ITM metaend
    ) const
    {
        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t meta_count = std::distance(metabegin, metaend);

        // Must not happen
        if (node_count != meta_count) {
            EX3_THROW(metadata_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(meta_count));
        }

        std::string rfile = get_rindex_name(data.basename);

        helpers::basic_callback<
            TMetaAdapter,
            rtree_adapter_impl,
            rtree_store,
            ITN,
            ITM
            > callback(meta_adapter, _adapter_impl, st._rtree,
                nodebegin, nodeend, metabegin, metaend);

        _file_helper.initialize(rfile, _mparams, data, _memory_prefix,
            st._mstorage, st._info.mapped_file, callback);

        st._info.rtree_size_bytes =
            _file_helper.get_size_bytes(st._mstorage) -
            _file_helper.get_free_bytes(st._mstorage);
    }

    /**
     * @brief Get the boundaries of the data stored in the spatial storage.
     *
     * @param st A reference to the spatial storage object.
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
        spatial_storage_query_iterator_type& begin,
        spatial_storage_query_iterator_type& end
    ) const
    {
        _adapter_impl.query(st._rtree, predicates, begin, end);
    }

    /**
     * @brief Get the iterator to the beginning of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return spatial_storage_iterator_type The iterator to the beginning of
     * the spatial storage.
     */
    spatial_storage_iterator_type sbegin(
        const spatial_storage_type& st
    ) const
    {
        return _adapter_impl.sbegin(st._rtree);
    }

    /**
     * @brief Get the iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return spatial_storage_iterator_type The iterator to the end of the
     * spatial storage.
     */
    spatial_storage_iterator_type send(
        const spatial_storage_type& st
    ) const
    {
        return _adapter_impl.send(st._rtree);
    }

    /**
     * @brief Get the read-only iterator to the beginning of the spatial
     * storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return spatial_storage_update_iterator_type The read-only iterator to
     * the beginning of the spatial storage.
     *
     * @note Slim adapters do not store pointers to dataset data so they are
     * not required to return a writable iterator.
     */
    spatial_storage_update_iterator_type sbegin_update(
        spatial_storage_type& st
    ) const
    {
        return _adapter_impl.sbegin_update(st._rtree);
    }

    /**
     * @brief Get the read-only iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return spatial_storage_update_iterator_type The read-only iterator to
     * the end of the spatial storage.
     *
     * @note Slim adapters do not store pointers to dataset data so they are
     * not required to return a writable iterator.
     */
    spatial_storage_update_iterator_type send_update(
        spatial_storage_type& st
    ) const
    {
        return _adapter_impl.send_update(st._rtree);
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
        _file_helper.destroy(st._mstorage);
    }
};

};

}}
