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

#include <s1o/exceptions.hpp>
#include <s1o/transforms/transform_drop_const.hpp>
#include <s1o/transforms/transform_get_tuple_element.hpp>
#include <s1o/helpers/rtree_indexer_byval.hpp>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/tuple/tuple.hpp>

#include <string>
#include <iterator>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A spatial adapter based on boost's rtree implementation that is
 * instanced entirely on memory.
 *
 * @tparam Params The parameters used to control the tree.
 * @tparam CoordSys THe coordinate system used by the tree.
 */
template <
    typename Params,
    typename CoordSys
    >
struct rtree
{

/**
 * @brief The implemnetation of the rtree spatial adapter.
 *
 * @tparam TData The type of the data stored in the structure.
 * @tparam TSval The type of the individual spatial coordinates.
 * @tparam NSDims The number of spatial dimensions of the data.
 */
template <
    typename TData,
    typename TSval,
    unsigned int NSDims
    >
struct spatial_adapter_impl
{
    /** The flag to indicate that this adapter can handle node data
        directly. */
    static const bool supports_element_pair = true;

    /** The specialization of the current type. */
    typedef spatial_adapter_impl<TData, TSval, NSDims> this_type;

    /** The number of spatial dimensions used to locate the date. */
    static const unsigned int num_spatial_dims = NSDims;

    /** The type used to represent the spatial variables. */
    typedef TSval spatial_value_type;

    /** The inner type used to store data in an rtree node. */
    typedef TData rpair_data;

    /** A point for the RTree implementation. */
    typedef boost::geometry::model::point<
        spatial_value_type,
        num_spatial_dims,
        CoordSys
    > spatial_point_type;

    /** The type used to store and retrieve memory mapped content
        from the rtree. */
    typedef boost::tuple<
        spatial_point_type,
        rpair_data
    > rpair;

    /** The type used to index the tree. */
    typedef helpers::rtree_indexer_byval<spatial_point_type> indexable_t;

    /** The type of the RTree, tailored for the number of dimensions. */
    typedef boost::geometry::index::rtree<
        rpair,
        Params,
        indexable_t
    > rtree_store;

    /** The spatial storage type required by the trait. */
    typedef rtree_store spatial_storage_type;

    /** The constant iterator used to traverse the tree. */
    typedef typename rtree_store::const_iterator rtree_const_iterator;

    /** The constant iterator used to traverse queries in the tree. */
    typedef typename rtree_store::const_query_iterator
        rtree_const_query_iterator;

    /** The transform used to access a specific element from a boost tuple and
        retrieve the dataset data stored in the tree. */
    typedef transform_get_tuple_element<
        1,
        rpair
        > transform_tuple_get_second;

    /** The transform used to make the reference to the data stored in the
        rtree writable. */
    typedef transform_drop_const<
        typename transform_tuple_get_second::value_type
        > transform_no_const;

    /** The iterator used to traverse the spatial storage as read-only dataset
        data rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_tuple_get_second,
        rtree_const_iterator,
        typename transform_tuple_get_second::reference,
        typename transform_tuple_get_second::value_type
        > tuple_get_second_s_iterator;

    /** The iterator used to traverse the spatial storage as writable dataset
        data rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_no_const,
        tuple_get_second_s_iterator,
        typename transform_no_const::reference,
        typename transform_no_const::value_type
        > drop_const_s_iterator;

    /** The iterator used to traverse queries in the spatial storage as
        read-only dataset data rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_tuple_get_second,
        rtree_const_query_iterator,
        typename transform_tuple_get_second::reference,
        typename transform_tuple_get_second::value_type
        > tuple_get_second_q_iterator;

    /** The iterator type required by the trait. */
    typedef tuple_get_second_s_iterator spatial_storage_iterator_type;

    /** The iterator type required by the trait. */
    typedef drop_const_s_iterator spatial_storage_update_iterator_type;

    /** The iterator type required by the trait. */
    typedef tuple_get_second_q_iterator spatial_storage_query_iterator_type;

    /** The parameters used to control the tree. */
    Params _params;

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param params The parameters used to control the tree.
     */
    inline spatial_adapter_impl(
        const Params& params=Params()
    ) :
        _params(params)
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
    inline bool empty(
        const rtree_store& st
    ) const
    {
        return st.empty();
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
    inline bool equals(
        const spatial_point_type& first,
        const spatial_point_type& second
    ) const
    {
        return boost::geometry::equals(first, second);
    }

    /**
     * @brief Communicate any extra files that are created with the spatial
     * storage.
     *
     * @tparam ITO The type of the result iterator with the filenames.
     *
     * @param iter The result iterator with the filenames.
     *
     * @return size_t The number of extra files created with the spatial
     * storage.
     */
    template <typename ITO>
    inline size_t get_extra_files(
        const std::string& basename,
        ITO iter) const
    {
        (void)(basename);
        (void)(iter);

        return 0;
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
     * @param basename The path and filename of the dataset without the
     * trailing extensions.
     * @param is_new Indicates if the dataset is being created from a
     * collection of elements of is being opened.
     * @param can_write Indicates if the datataset allows writes.
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
    inline void initialize(
        spatial_storage_type& st,
        const std::string& basename,
        bool is_new,
        bool can_write,
        ITN nodebegin,
        ITN nodeend,
        ITL locbegin,
        ITL locend
    ) const
    {
        (void)(basename);
        (void)(is_new);
        (void)(can_write);

        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t loc_count = std::distance(locbegin, locend);

        // Must not happen
        if (node_count != loc_count) {
            EX3_THROW(location_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(loc_count));
        }

        st = rtree_store(
            boost::make_zip_iterator(
                boost::make_tuple(locbegin, nodebegin)),
            boost::make_zip_iterator(
                boost::make_tuple(locend, nodeend)),
            _params);
    }

    /**
     * @brief Get the iterator to the beginning of a sequence of elements that
     * satisfy the predicates.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param st A reference to the spatial storage object.
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     *
     * @return tuple_get_second_q_iterator the iterator to the beginning of a
     * sequence of elements that satisfy the predicates.
     */
    template<typename Predicates>
    inline tuple_get_second_q_iterator qbegin(
        const rtree_store& st,
        const Predicates& predicates
    ) const
    {
        return tuple_get_second_q_iterator(st.qbegin(predicates));
    }

    /**
     * @brief Get the iterator to the end of the sequence of elements that
     * satisfy the predicates.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_q_iterator The iterator to the end of a
     * sequence of elements that satisfy the predicates.
     */
    inline tuple_get_second_q_iterator qend(
        const rtree_store& st
    ) const
    {
        return tuple_get_second_q_iterator(st.qend());
    }

    /**
     * @brief Get the iterator to the beginning of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_s_iterator The iterator to the beginning of
     * the spatial storage.
     */
    inline tuple_get_second_s_iterator sbegin(
        const rtree_store& st
    ) const
    {
        return tuple_get_second_s_iterator(st.begin());
    }

    /**
     * @brief Get the iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_q_iterator The iterator to the end of the
     * spatial storage.
     */
    inline tuple_get_second_s_iterator send(
        const rtree_store& st
    ) const
    {
        return tuple_get_second_s_iterator(st.end());
    }

    /**
     * @brief Get writable the iterator to the beginning of the spatial
     * storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return drop_const_s_iterator The writable iterator to the beginning of
     * the spatial storage.
     */
    inline drop_const_s_iterator sbegin_update(
        const spatial_storage_type& st
    ) const
    {
        return drop_const_s_iterator(tuple_get_second_s_iterator(st.begin()));
    }

    /**
     * @brief Get the writable iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return drop_const_s_iterator The writable iterator to the end of the
     * spatial storage.
     */
    inline drop_const_s_iterator send_update(
        const spatial_storage_type& st
    ) const
    {
        return drop_const_s_iterator(tuple_get_second_s_iterator(st.end()));
    }
};

};

}}
