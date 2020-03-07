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
#include <s1o/queries/closed_interval.hpp>
#include <s1o/helpers/mi_vector_to_cons.hpp>
#include <s1o/helpers/mi_vector_to_indices.hpp>
#include <s1o/helpers/location_iterator_helper.hpp>
#include <s1o/initialization_data/default_data.hpp>
#include <s1o/initialization_data/mapped_data.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/fusion/sequence/intrinsic/at_c.hpp>
#include <boost/fusion/include/at_c.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/mpl/integral_c.hpp>
#include <boost/mpl/size.hpp>

#include <string>
#include <iterator>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A generic implementation of a spatial adapter based on boost's
 * multiindex, supporting custom allocators.
 *
 * @tparam Indices A vector of types to be used as indices.
 * @tparam Allocator The type of the allocator used by the multiindex.
 *
 * @note This multiindex implementation can only be used as an auxiliary index
 * because it does not implement the full specification of a spatial adapter.
 * It does follow the spatial value type and number of dimensions
 *
 * @note Each index must be unique and must be comparable with the less than
 * operator.
 */
template <
    typename Indices,
    typename Allocator
    >
struct multiindex_base
{
    /** The number of indices stored in the multiindex. */
    static const unsigned int num_indices = boost::mpl::size<Indices>::value;

    /** A point for the multiindex implementation. */
    typedef typename helpers::mi_vector_to_cons<
        Indices
        >::type indices_t;

/**
 * @brief The implemnetation of the multiindex spatial adapter.
 *
 * @tparam TData The type of the data stored in the structure.
 */
template <
    typename TData
    >
struct spatial_adapter_impl
{
    /** The flag to indicate that this adapter can handle node data
        directly. */
    static const bool supports_element_pair = true;

    /** The specialization of the current type. */
    typedef spatial_adapter_impl<TData> this_type;

    /** The inner type used to store data in an multiindex node. */
    typedef TData mipair_data;

    /** A point for the multiindex implementation. */
    typedef indices_t spatial_point_type;

    /** A vector of indexable elements that the container can use to assemble
        itself. It wraps the indices with information about how the index
        should be constructed and how to retrieve each index. */
    typedef typename helpers::mi_vector_to_indices<
        Indices
        >::type mi_indices_t;

    /** The type used to store and retrieve content from the multiindex. */
    typedef boost::tuple<spatial_point_type, mipair_data> mipair;

    /** The type of the allocator used to construct the multiindex and its
        nodes. */
    typedef typename Allocator::template rebind<mipair>::other allocator_t;

    /** The type of the multiindex, tailored for the number of dimensions. */
    typedef boost::multi_index::multi_index_container<
        mipair,
        mi_indices_t,
        allocator_t
        > mi_store;

    /** The parameters used to construct the multiindex. */
    typedef typename mi_store::ctor_args_list mi_args;

    /** The spatial storage type required by the trait. */
    typedef mi_store* spatial_storage_type;

    /** The transform used to access a specific element from a boost tuple and
        retrieve the dataset data stored in the multiindex. */
    typedef transform_get_tuple_element<
        1,
        mipair
        > transform_tuple_get_second;

    /** The query iterator type required by the trait. */
    template <typename Index>
    struct spatial_storage_query_iterator_type
    {
        typedef typename mi_store::template index<Index>::type index_set;
        typedef typename index_set::const_iterator index_iterator;

        /** The iterator used to traverse the spatial storage as read-only
            dataset data rather than multiindex nodes. */
        typedef boost::transform_iterator<
            transform_tuple_get_second,
            index_iterator,
            typename transform_tuple_get_second::reference,
            typename transform_tuple_get_second::value_type
            > type;
    };

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     */
    spatial_adapter_impl()
    {
    }

    /**
     * @brief Fill the spatial storage with a sequence of elements.
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
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of TData objects to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of TData objects to be stored.
     * @param metabegin The iterator pointing to the beginning of a sequence
     * of metadata associated with each element.
     */
    template <typename TMetaAdapter, typename ITN, typename ITM>
    void fill_storage(
        spatial_storage_type& st,
        const TMetaAdapter& meta_adapter,
        ITN nodebegin,
        ITN nodeend,
        ITM metabegin
    ) const
    {
        // Create the location iterators from the meta iterator

        typedef helpers::location_iterator_helper<
            TMetaAdapter,
            ITM,
            spatial_point_type
            > loc_helper;

        loc_helper locs(meta_adapter);

        typename loc_helper::iterator locbegin = locs(metabegin);

        for ( ; nodebegin != nodeend; nodebegin++, locbegin++)
            st->insert(boost::make_tuple(*locbegin, *nodebegin));
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
        (void)data;

        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t meta_count = std::distance(metabegin, metaend);

        // Must not happen
        if (node_count != meta_count) {
            EX3_THROW(metadata_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(meta_count));
        }

        // Rebind the allocator to allocate the multiindex itself
        typename allocator_t::template rebind<
            mi_store
            >::other allocator;

        // Allocate and construct the multiindex

        mi_store* mem = allocator.allocate(1);

        mi_args args;
        st = new (mem) mi_store(args);

        fill_storage(st, meta_adapter, nodebegin, nodeend, metabegin);
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
        const initialization_data::mapped_data& data,
        ITN nodebegin,
        ITN nodeend,
        ITM metabegin,
        ITM metaend
    ) const
    {
        // Reset the storage
        if (data.mapped_file == 0) {
            st = 0;
            return;
        }

        const size_t node_count = std::distance(nodebegin, nodeend);
        const size_t meta_count = std::distance(metabegin, metaend);

        // Must not happen
        if (node_count != meta_count) {
            EX3_THROW(metadata_count_mismatch_exception()
                << expected_num_elements(node_count)
                << actual_num_elements(meta_count));
        }

        std::string memory_id = data.prefix +
            "s1o::spatial_adapters::multiindex_base";

        if (data.base_data.is_new) {

            // Rebind the allocator to allocate the multiindex itself
            allocator_t allocator(data.mapped_file->get_segment_manager());

            // Create the multiindex
            mi_args args;
            st = data.mapped_file->template construct<
                mi_store
                >(memory_id.c_str())(
                args,
                allocator);

            fill_storage(st, meta_adapter, nodebegin, nodeend, metabegin);

            // Shouldn't happen because the file is new
            if (st == 0) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(st));
            }
        }
        else {

            std::pair<mi_store*, size_t> p;

            p = data.mapped_file->template find<
                mi_store
                >(memory_id.c_str());

            // Shouldn't happen because we are opening a valid file
            if (p.first == 0) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(p.first)
                    << actual_num_elements(p.second));
            }

            // Shouldn't happen because we only allocate one multiindex
            if (p.second != 1) {
                EX3_THROW(inconsistent_index_exception()
                    << actual_pointer(p.first)
                    << actual_num_elements(p.second));
            }

            st = p.first;
        }

        // Check if traces were added or removed after the multiindex was created
        if (st->size() != node_count) {
            EX3_THROW(inconsistent_index_exception()
                << expected_num_elements(st->size())
                << actual_num_elements(node_count));
        }
    }

    /**
     * @brief Recursively get the boundaries of the data stored in the
     * current index of the spatial storage and in the following indices.
     *
     * @tparam I The integral type of the current index in the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     * @param minpoint The smallest coordinates in the spatial storage.
     * @param maxpoint The largest coordinates in the spatial storage.
     * @param tag The tag used to identify the current index in the spatial
     * storage.
     */
    template <typename I>
    void _bounds(
        const spatial_storage_type& st,
        spatial_point_type& minpoint,
        spatial_point_type& maxpoint,
        const I& tag
    ) const
    {
        (void)tag;

        typedef typename mi_store::template nth_index<
            I::value
            >::type index_set;

        typedef typename index_set::const_iterator index_iterator;

        const index_set& index = st->template get<I::value>();

        index_iterator begin = index.begin();
        index_iterator end = index.end();

        if (begin != end) {

            --end;

            boost::fusion::at_c<I::value>(minpoint) =
                boost::fusion::at_c<I::value>(
                boost::get<0>(*begin));

            boost::fusion::at_c<I::value>(maxpoint) =
                boost::fusion::at_c<I::value>(
                boost::get<0>(*end));
        }

        boost::mpl::integral_c<
            unsigned int,
            I::value + 1
            > next;

        _bounds(st, minpoint, maxpoint, next);
    }

    /**
     * @brief The tail of the _bounds template recursion.
     *
     * @param st A reference to the spatial storage object.
     * @param minpoint The smallest coordinates in the spatial storage.
     * @param maxpoint The largest coordinates in the spatial storage.
     * @param tag The tag used to identify the tail.
     */
    void _bounds(
        const spatial_storage_type& st,
        spatial_point_type& minpoint,
        spatial_point_type& maxpoint,
        const boost::mpl::integral_c<unsigned int, num_indices>& tag
    ) const
    {
        (void)st;
        (void)minpoint;
        (void)maxpoint;
        (void)tag;
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
        boost::mpl::integral_c<
            unsigned int,
            0
            > first;

        _bounds(st, minpoint, maxpoint, first);
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy a closed interval query.
     *
     * @tparam Index The index in the multiindex used in the predicates to be
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
    template<typename Index>
    void query(
        const spatial_storage_type& st,
        const queries::closed_interval<Index>& predicates,
        typename spatial_storage_query_iterator_type<Index>::type& begin,
        typename spatial_storage_query_iterator_type<Index>::type& end
    ) const
    {
        typedef spatial_storage_query_iterator_type<Index> I;
        typedef typename I::index_set index_set;
        typedef typename I::index_iterator index_iterator;
        typedef typename I::type iterator;

        const index_set& index = st->template get<Index>();

        const Index point_min = predicates.point_min;
        const Index point_max = predicates.point_max;

        begin = iterator(index.lower_bound(point_min));
        end = iterator(index.upper_bound(point_max));
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
            mi_store
            >::other allocator;

        st->~multiindex();
        allocator.deallocate(st, 1);
    }
};

};

}}
