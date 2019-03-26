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
#include <s1o/queries/nearest.hpp>
#include <s1o/queries/closed_interval.hpp>
#include <s1o/transforms/transform_deref.hpp>
#include <s1o/transforms/transform_drop_const.hpp>
#include <s1o/transforms/transform_get_tuple_element.hpp>
#include <s1o/initialization_data/default_data.hpp>
#include <s1o/initialization_data/mapped_data.hpp>
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
 * @brief A generic implementation of a spatial adapter based on boost's
 * rtree, supporting custom allocators.
 *
 * @tparam Params The parameters used to control the tree.
 * @tparam CoordSys The coordinate system used by the tree.
 * @tparam Allocator The type of the allocator used by the tree.
 */
template <
    typename Params,
    typename CoordSys,
    typename Allocator
    >
struct rtree_base
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

    /** A point for the rtree implementation. */
    typedef boost::geometry::model::point<
        spatial_value_type,
        num_spatial_dims,
        CoordSys
    > spatial_point_type;

    /** The type used to store and retrieve content from the rtree. */
    typedef boost::tuple<
        spatial_point_type,
        rpair_data
    > rpair;

    /** The type used to index the tree. */
    typedef helpers::rtree_indexer_byval<spatial_point_type> indexable_t;

    /** The type used to compare elements in the tree. */
    typedef boost::geometry::index::equal_to<rpair> equal_to_t;

    /** The type of the allocator used to construct the tree and its
        nodes. */
    typedef typename Allocator::template rebind<rpair>::other allocator_t;

    /** The type of the rtree, tailored for the number of dimensions. */
    typedef boost::geometry::index::rtree<
        rpair,
        Params,
        indexable_t,
        equal_to_t,
        allocator_t
        > rtree_store;

    /** The spatial storage type required by the trait. */
    typedef rtree_store* spatial_storage_type;

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
        > tuple_get_second_iterator;

    /** The iterator used to traverse the spatial storage as writable dataset
        data rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_no_const,
        tuple_get_second_iterator,
        typename transform_no_const::reference,
        typename transform_no_const::value_type
        > drop_const_iterator;

    /** The iterator used to traverse queries in the spatial storage as
        read-only dataset data rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_tuple_get_second,
        rtree_const_query_iterator,
        typename transform_tuple_get_second::reference,
        typename transform_tuple_get_second::value_type
        > tuple_get_second_query_iterator;

    /** The iterator type required by the trait. */
    typedef tuple_get_second_iterator
        spatial_storage_iterator_type;

    /** The update iterator type required by the trait. */
    typedef drop_const_iterator
        spatial_storage_update_iterator_type;

    /** The query iterator type required by the trait. */
    typedef tuple_get_second_query_iterator
        spatial_storage_query_iterator_type;

    /** The parameters used to control the tree. */
    const Params _params;

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param params The parameters used to control the tree.
     */
    spatial_adapter_impl(
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
    bool empty(
        const spatial_storage_type& st
    ) const
    {
        return (st == 0) || st->empty();
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
    size_t get_extra_files(
        const std::string& basename,
        ITO iter) const
    {
        (void)basename;
        (void)iter;

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
        (void)data;

        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t loc_count = std::distance(locbegin, locend);

        // Must not happen
        if (node_count != loc_count) {
            EX3_THROW(location_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(loc_count));
        }

        // Rebind the allocator to allocate the tree itself
        typename allocator_t::template rebind<
            rtree_store
            >::other allocator;

        // Allocate and construct the tree

        rtree_store* mem = allocator.allocate(1);

        st = new (mem) rtree_store(
            boost::make_zip_iterator(
                boost::make_tuple(locbegin, nodebegin)),
            boost::make_zip_iterator(
                boost::make_tuple(locend, nodeend)),
            _params);
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
        const initialization_data::mapped_data& data,
        ITN nodebegin,
        ITN nodeend,
        ITL locbegin,
        ITL locend
    ) const
    {
        // Reset the storage
        if (data.mapped_file == 0) {
            st = 0;
            return;
        }

        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t loc_count = std::distance(locbegin, locend);

        // Must not happen
        if (node_count != loc_count) {
            EX3_THROW(location_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(loc_count));
        }

        std::string memory_id = data.prefix +
            "s1o::spatial_adapters::rtree_base";

        if (data.base_data.is_new) {

            // Rebind the allocator to allocate the tree itself
            allocator_t allocator(data.mapped_file->get_segment_manager());

            // Create the rtree
            st = data.mapped_file->template construct<
                rtree_store
                >(memory_id.c_str())(
                boost::make_zip_iterator(
                    boost::make_tuple(locbegin, nodebegin)),
                boost::make_zip_iterator(
                    boost::make_tuple(locend, nodeend)),
                _params, indexable_t(), equal_to_t(), allocator);

            // Shouldn't happen because the file is new
            if (st == 0) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(st));
            }
        }
        else {

            std::pair<rtree_store*, size_t> p;

            p = data.mapped_file->template find<
                rtree_store
                >(memory_id.c_str());

            // Shouldn't happen because we are opening a valid file
            if (p.first == 0) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(p.first)
                    << actual_num_elements(p.second));
            }

            // Shouldn't happen because we only allocate one tree
            if (p.second != 1) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(p.first)
                    << actual_num_elements(p.second));
            }

            st = p.first;
        }

        // Check if traces were added or removed after the tree was created
        if (st->size() != node_count) {
            EX3_THROW(inconsistent_index_exception()
                << expected_num_elements(st->size())
                << actual_num_elements(node_count));
        }
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
        typename rtree_store::bounds_type b = st->bounds();
        minpoint = b.min_corner();
        maxpoint = b.max_corner();
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
        tuple_get_second_query_iterator& begin,
        tuple_get_second_query_iterator& end
    ) const
    {
        begin = tuple_get_second_query_iterator(
            st->qbegin(predicates));

        end = tuple_get_second_query_iterator(st->qend());
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy a closed interval query.
     *
     * @tparam Point The spatial point type used in the predicates to be
     * satisfied by the elements in the storage.
     *
     * @param st A reference to the spatial storage object.
     * @param predicates The closed interval query to be satisfied by the
     * elements in the storage.
     * @param begin The resulting iterator to the beginning of a sequence of
     * elements that satisfy the predicates.
     * @param end The resulting iterator to the end of a sequence of elements
     * that satisfy the predicates.
     */
    template<typename Point>
    void query(
        const spatial_storage_type& st,
        const queries::closed_interval<Point>& predicates,
        tuple_get_second_query_iterator& begin,
        tuple_get_second_query_iterator& end
    ) const
    {
        using namespace boost::geometry::index;
        using namespace boost::geometry::model;

        const spatial_point_type point_min = predicates.point_min;
        const spatial_point_type point_max = predicates.point_max;

        begin = tuple_get_second_query_iterator(st->qbegin(
            intersects(box<spatial_point_type>(point_min, point_max))
        ));

        end = tuple_get_second_query_iterator(st->qend());
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy a k-nearest query.
     *
     * @tparam Point The spatial point type used in the predicates to be
     * satisfied by the elements in the storage.
     *
     * @param st A reference to the spatial storage object.
     * @param predicates The k-nearest query to be satisfied by the elements
     * in the storage.
     * @param begin The resulting iterator to the beginning of a sequence of
     * elements that satisfy the predicates.
     * @param end The resulting iterator to the end of a sequence of elements
     * that satisfy the predicates.
     */
    template<typename Point>
    void query(
        const spatial_storage_type& st,
        const queries::nearest<Point>& predicates,
        tuple_get_second_query_iterator& begin,
        tuple_get_second_query_iterator& end
    ) const
    {
        using namespace boost::geometry::index;

        const spatial_point_type point = predicates.point;
        const unsigned int k = predicates.k;

        begin = tuple_get_second_query_iterator(st->qbegin(
            nearest(point, k)
        ));

        end = tuple_get_second_query_iterator(st->qend());
    }

    /**
     * @brief Get the iterator to the beginning of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The iterator to the beginning of
     * the spatial storage.
     */
    tuple_get_second_iterator sbegin(
        const spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st->begin());
    }

    /**
     * @brief Get the iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The iterator to the end of the
     * spatial storage.
     */
    tuple_get_second_iterator send(
        const spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st->end());
    }

    /**
     * @brief Get writable the iterator to the beginning of the spatial
     * storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return drop_const_iterator The writable iterator to the beginning of
     * the spatial storage.
     */
    drop_const_iterator sbegin_update(
        spatial_storage_type& st
    ) const
    {
        return drop_const_iterator(tuple_get_second_iterator(st->begin()));
    }

    /**
     * @brief Get the writable iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return drop_const_iterator The writable iterator to the end of the
     * spatial storage.
     */
    drop_const_iterator send_update(
        spatial_storage_type& st
    ) const
    {
        return drop_const_iterator(tuple_get_second_iterator(st->end()));
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
        if (st == 0)
            return;

        typename allocator_t::template rebind<
            rtree_store
            >::other allocator;

        st->~rtree();
        allocator.deallocate(st, 1);
    }
};

};

}}
