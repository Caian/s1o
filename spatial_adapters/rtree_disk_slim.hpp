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

#include <s1o/types.hpp>
#include <s1o/exceptions.hpp>
#include <s1o/queries/nearest.hpp>
#include <s1o/queries/closed_interval.hpp>
#include <s1o/transforms/transform_get_tuple_element.hpp>
#include <s1o/initialization_data/default_data.hpp>
#include <s1o/helpers/rtree_indexer_byval.hpp>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/iterator/zip_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/tuple/tuple.hpp>

#include <string>
#include <vector>
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
 * @tparam CoordSys THe coordinate system used by the tree.
 */
template <
    typename Params,
    typename CoordSys
    >
struct rtree_disk_slim
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
    static const bool supports_element_pair = false;

    /** The specialization of the current type. */
    typedef spatial_adapter_impl<TData, TSval, NSDims> this_type;

    /** The number of spatial dimensions used to locate the date. */
    static const unsigned int num_spatial_dims = NSDims;

    /** The type used to represent the spatial variables. */
    typedef TSval spatial_value_type;

    /** A point for the RTree implementation. */
    typedef boost::geometry::model::point<
        spatial_value_type,
        num_spatial_dims,
        CoordSys
    > spatial_point_type;

    /** The type used to store and retrieve memory mapped content from the
        rtree. */
    typedef boost::tuple<
        spatial_point_type,
        uid_t
    > rpair;

    /** The type used to index the tree. */
    typedef helpers::rtree_indexer_byval<spatial_point_type> indexable_t;

    /** The type used to compare elements in the tree. */
    typedef boost::geometry::index::equal_to<rpair> equal_to_t;

    /** Create a memory-mapped allocator so the operations in the tree happen
        directly on disk. */
    typedef boost::interprocess::allocator<
        rpair,
        boost::interprocess::managed_mapped_file::segment_manager
        > allocator_t;

    /** The type of the RTree, tailored for the number of dimensions. */
    typedef boost::geometry::index::rtree<
        rpair,
        Params,
        indexable_t,
        equal_to_t,
        allocator_t
        > rtree_store;

    /** The struct containing information about the initialization process of
        the rtree. */
    struct initialization_info
    {
        size_t rtree_size_bytes;
        size_t rfile_size_bytes;
        size_t rfile_attempts;

        initialization_info(
        ) :
            rtree_size_bytes(0),
            rfile_size_bytes(0),
            rfile_attempts(0)
        {
        }
    };

    /** The spatial storage type required by the trait. */
    struct spatial_storage_type
    {
        rtree_store* _rtree;
        boost::interprocess::managed_mapped_file* _mfile;
        allocator_t* _alloc;

        initialization_info _info;

        spatial_storage_type() :
            _rtree(0),
            _mfile(0),
            _alloc(0),
            _info()
        {
        }

        ~spatial_storage_type()
        {
            delete _alloc;
            delete _mfile;
        }
    };

    /** The constant iterator used to traverse the tree. */
    typedef typename rtree_store::const_iterator rtree_const_iterator;

    /** The constant iterator used to traverse queries in the tree. */
    typedef typename rtree_store::const_query_iterator
        rtree_const_query_iterator;

    /** The transform used to access a specific element from a boost tuple and
        retrieve the element uid stored in the tree. */
    typedef transform_get_tuple_element<
        1,
        rpair
        > transform_tuple_get_second;

    /** The iterator used to traverse the spatial storage as read-only uids
        rather than rtree nodes. */
    typedef boost::transform_iterator<
        transform_tuple_get_second,
        rtree_const_iterator,
        typename transform_tuple_get_second::reference,
        typename transform_tuple_get_second::value_type
        > tuple_get_second_iterator;

    /** The iterator used to traverse queries in the spatial storage as
        read-only uids rather than rtree nodes. */
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
    typedef tuple_get_second_iterator
        spatial_storage_update_iterator_type;

    /** The query iterator type required by the trait. */
    typedef tuple_get_second_query_iterator
        spatial_storage_query_iterator_type;

    /** The parameters used to control the tree. */
    const Params _params;

    /** The initial size of the rfile in bytes. */
    const size_t _starting_rfile_size;

    /** The increment in bytes to be made to the rfile if the previous size is
        too small to allocate the tree. */
    const size_t _rfile_increment;

    /** The maximum number of resize attempts to be made when a tree
        allocation fails. */
    const size_t _resize_attempts;

    /** The extension to use for the rfile. */
    const std::string _file_extension;

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param params The parameters used to control the tree.
     * @param starting_rfile_size The initial size of the rfile in bytes.
     * @param rfile_increment The increment in bytes to be made to the rfile
     * if the previous size is too small to allocate the tree.
     * @param resize_attempts The maximum number of resize attempts to be made
     * when a tree allocation fails.
     * @param file_extension The extension to use for the rfile.
     */
    inline spatial_adapter_impl(
        const Params& params=Params(),
        size_t starting_rfile_size=512ULL*1024*1024,
        size_t rfile_increment=512ULL*1024*1024,
        size_t resize_attempts=5,
        const std::string& file_extension=".ridx"
    ) :
        _params(params),
        _starting_rfile_size(starting_rfile_size),
        _rfile_increment(rfile_increment),
        _resize_attempts(resize_attempts),
        _file_extension(file_extension)
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
    inline std::string get_rindex_name(
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
    inline bool empty(
        const spatial_storage_type& st
    ) const
    {
        return (st._rtree == 0) || st._rtree->empty();
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
    inline size_t get_extra_files(
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
     * @tparam ITN The type of the iterator for the sequence o element uids to
     * be stored.
     * @tparam ITL The type of the iterator for the sequence of spatial
     * locations associated with each element.
     *
     * @param st The spatial storage object being initialized.
     * @param data The initialization data for the spatial storage.
     * @param nodebegin The iterator pointing to the beginning of a sequence
     * of uids to be stored.
     * @param nodeend The iterator pointing to after the last element of
     * sequence of uids to be stored.
     * @param locbegin The iterator pointing to the beginning of a sequence
     * of spatial locations associated with each element.
     * @param locend The iterator pointing to after the last element of a
     * sequence of spatial locations associated with each element.
     *
     * @note The uid is used instead of TData because this is a slim adapter.
     */
    template <typename ITN, typename ITL>
    inline void initialize(
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

        std::string rfile = get_rindex_name(data.basename);

        if (data.is_new) {

            if (!data.can_write) {
                EX3_THROW(read_only_exception()
                    << file_name(rfile));
            }

            create_new_tree(st, rfile,
                boost::make_zip_iterator(
                    boost::make_tuple(locbegin, nodebegin)),
                boost::make_zip_iterator(
                    boost::make_tuple(locend, nodeend)));
        }
        else {

            open_existing_tree(st, rfile);

            // Check if traces were added or removed
            // after the tree was created
            if (st._rtree->size() != node_count) {
                EX3_THROW(inconsistent_index_exception()
                    << expected_num_elements(st._rtree->size())
                    << actual_num_elements(node_count)
                    << file_name(rfile));
            }
        }
    }

    /**
     * @brief Open the rtree from the rfile.
     *
     * @param st A reference to the spatial storage object being initialized.
     * @param rfile The path and filename of the file used to store the tree.
     */
    inline void open_existing_tree(
        spatial_storage_type& st,
        const std::string& rfile
    ) const
    {
        st._info.rfile_attempts = 0;

        try {
            // Force open the file
            st._mfile = new boost::interprocess::managed_mapped_file(boost::
                interprocess::open_read_only, rfile.c_str());
        }
        catch (const std::exception& ex) {

            if (boost::get_error_info<ex3::traced>(ex) == 0) {
                EX3_THROW(
                    EX3_ENABLE(ex)
                        << file_name(rfile));
            }
            else {
                EX3_RETHROW(
                    EX3_ENABLE(ex)
                        << file_name(rfile));
            }
        }

        // Create the allocator
        st._alloc = new allocator_t(st._mfile->get_segment_manager());

        st._info.rfile_size_bytes =
            st._alloc->get_segment_manager()->get_size();

        st._info.rtree_size_bytes =
            st._alloc->get_segment_manager()->get_size() -
            st._alloc->get_segment_manager()->get_free_memory();

        // Retrieve the r-tree from the storage

        std::pair<rtree_store*, size_t> p = st._mfile->template find<
            rtree_store
            >("rtree");

        // Shouldn't happen because we are opening a valid file
        if (p.first == 0) {
            EX3_THROW(inconsistent_index_exception()
                << actual_pointer(p.first)
                << actual_num_elements(p.second)
                << file_name(rfile));
        }

        // Shouldn't happen because we only allocate one tree
        if (p.second != 1) {
            EX3_THROW(inconsistent_index_exception()
                << actual_pointer(p.first)
                << actual_num_elements(p.second)
                << file_name(rfile));
        }

        st._rtree = p.first;
    }

    /**
     * @brief Create a new tree object
     *
     * @tparam IT The type of the iterator of rtree nodes that will be
     * inserted into the tree.
     *
     * @param st A reference to the spatial storage object being initialized.
     * @param rfile The path and filename of the file used to store the tree.
     * @param begin The iterator pointing to the beginning of a sequence
     * of rtree node objects to be inserted into the tree.
     * @param end The iterator pointing to after the last element of
     * sequence of rtree node objects to be inserted into the tree.
     */
    template <typename IT>
    inline void create_new_tree(
        spatial_storage_type& st,
        const std::string& rfile,
        IT begin,
        IT end
    ) const
    {
        // Try to allocate the map file with increasing
        // size until a limit is reached

        for (size_t attempt = 0; attempt <= _resize_attempts; attempt++) {

            // Compute the new mapped file size
            const size_t file_size = _starting_rfile_size +
                attempt * _rfile_increment;

            st._info.rfile_size_bytes = file_size;
            st._info.rfile_attempts = attempt+1;

            // Remove the old file
            boost::interprocess::file_mapping::remove(rfile.c_str());

            // Force create the new file
            st._mfile = new boost::interprocess::managed_mapped_file(boost::
                interprocess::create_only, rfile.c_str(), file_size);

            // Create the allocator
            st._alloc = new allocator_t(st._mfile->get_segment_manager());

            try {

                // Create the r-tree
                st._rtree = st._mfile->template construct<
                    rtree_store
                    >("rtree")(begin, end, _params, indexable_t(),
                        equal_to_t(), *st._alloc);

                // Shouldn't happen because the file is new
                if (st._rtree == 0) {
                    EX3_THROW(inconsistent_index_exception()
                        << actual_pointer(st._rtree)
                        << file_name(rfile));
                }

                break;
            }
            catch(const boost::interprocess::bad_alloc&) {

                // Delete the old allocator and file

                delete st._alloc;
                delete st._mfile;

                st._rtree = 0;
                st._alloc = 0;
                st._mfile = 0;

                // Give up if the maximum number of attempts is reached
                if (attempt == _resize_attempts) {
                    EX3_THROW(index_size_too_big_exception()
                        << maximum_attempts(attempt)
                        << maximum_size(file_size)
                        << file_name(rfile));
                }

                // Try again
                continue;
            }
        }

        st._info.rtree_size_bytes =
            st._alloc->get_segment_manager()->get_size() -
            st._alloc->get_segment_manager()->get_free_memory();
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
        typename rtree_store::bounds_type b = st._rtree->bounds();
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
            st._rtree->qbegin(predicates));

        end = tuple_get_second_query_iterator(st._rtree->qend());
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

        begin = tuple_get_second_query_iterator(
            st._rtree->qbegin(
                intersects(box<spatial_point_type>(point_min, point_max))
            ));

        end = tuple_get_second_query_iterator(st._rtree->qend());
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

        begin = tuple_get_second_query_iterator(
            st._rtree->qbegin(
                nearest(point, k)
            ));

        end = tuple_get_second_query_iterator(st._rtree->qend());
    }

    /**
     * @brief Get the iterator to the beginning of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The iterator to the beginning of
     * the spatial storage.
     */
    inline tuple_get_second_iterator sbegin(
        const spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st._rtree->begin());
    }

    /**
     * @brief Get the iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The iterator to the end of the
     * spatial storage.
     */
    inline tuple_get_second_iterator send(
        const spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st._rtree->end());
    }

    /**
     * @brief Get the read-only iterator to the beginning of the spatial
     * storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The writable iterator to the
     * beginning of the spatial storage.
     *
     * @note Slim adapters do not store pointers to dataset data so they are
     * not required to return a writable iterator.
     */
    inline tuple_get_second_iterator sbegin_update(
        spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st._rtree->begin());
    }

    /**
     * @brief Get the read-only iterator to the end of the spatial storage.
     *
     * @param st A reference to the spatial storage object.
     *
     * @return tuple_get_second_iterator The writable iterator to the end of
     * the spatial storage.
     *
     * @note Slim adapters do not store pointers to dataset data so they are
     * not required to return a writable iterator.
     */
    inline tuple_get_second_iterator send_update(
        spatial_storage_type& st
    ) const
    {
        return tuple_get_second_iterator(st._rtree->end());
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
