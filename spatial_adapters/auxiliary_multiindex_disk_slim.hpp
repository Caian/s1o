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

#include "multiindex_base.hpp"

#include <s1o/types.hpp>
#include <s1o/dataset.hpp>
#include <s1o/exceptions.hpp>
#include <s1o/helpers/tuple_callback.hpp>
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

#include <string>
#include <iterator>
#include <algorithm>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A wrapper for a primary (main) spatial adaptor that adds a set of
 * operations on a secondary (auxiliary) index base on boost multiindex.
 *
 * @tparam PrimaryAdapter The main spatial adapter type that will be used for
 * all standard methods.
 * @tparam Indices A vector of types to be used as the indices of the
 * auxiliary multiindex.
 */
template <typename PrimaryAdapter, typename Indices>
struct auxiliary_multiindex_disk_slim
{
    /** Create a memory-mapped allocator so the operations in the tree happen
        directly on disk. */
    typedef boost::interprocess::allocator<
        void,
        boost::interprocess::managed_mapped_file::segment_manager
        > allocator_t;

    /** The primary adapter that constrols everything. */
    typedef PrimaryAdapter primary_adapter;

    /** The multiindex secondary adapter that enables other queries. */
    typedef s1o::spatial_adapters::multiindex_base<
        Indices,
        allocator_t
        > secondary_adapter;

    /** The number of indices stored in the multiindex. */
    static const unsigned int num_indices = secondary_adapter::num_indices;

    /** A point for the multiindex implementation. */
    typedef typename secondary_adapter::indices_t indices_t;

    /** The parameters used to control the creation of the mapped file. */
    typedef helpers::mapped_file_helper::params_t mparams_t;

/**
 * @brief The implemnetation of the auxiliary spatial adapter.
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

    /** The implementation of the primary adapter that handles everything. */
    typedef typename traits::spatial_adapter_impl<
        primary_adapter,
        uid_t,
        TSVal,
        NSDims
        >::type primary_adapter_impl;

    /** The implementation of the secondary adapter that enables other
        queries. It does not follow the full implementation template. */
    typedef typename secondary_adapter::template spatial_adapter_impl<
        uid_t
        > secondary_adapter_impl;

    /** The number of spatial dimensions used to locate the data. */
    static const unsigned int num_spatial_dims =
        s1o::traits::num_spatial_dims<primary_adapter_impl>::value;

    /** The type used to represent the spatial variables. */
    typedef typename s1o::traits::spatial_value_type<
        primary_adapter_impl
        >::type spatial_value_type;

    /** The spatial storage of the base rtree. */
    typedef typename s1o::traits::spatial_storage_type<
        primary_adapter_impl
        >::type primary_store;

    /** The spatial storage of the base rtree. */
    typedef typename s1o::traits::spatial_storage_type<
        secondary_adapter_impl
        >::type secondary_store;

    /** A point for the primary adapter. */
    typedef typename traits::spatial_point_type<
        primary_adapter_impl
        >::type primary_spatial_point_type;

    /** A point for the secondary adapter. */
    typedef indices_t secondary_spatial_point_type;

    /** The point used by the standard interface. */
    typedef primary_spatial_point_type spatial_point_type;

    /** The struct containing information about the initialization process of
        the adapter. */
    struct initialization_info
    {
        /** The number of allocated bytes for the adapters inside the mapped
            file. */
        size_t adapters_size_bytes;

        /** Information about the underlying memory mapped file. */
        helpers::mapped_file_helper::initialization_info mapped_file;

        /**
         * @brief Construct a new initialization_info object.
         *
         */
        initialization_info(
        ) :
            adapters_size_bytes(0),
            mapped_file()
        {
        }
    };

    /** The spatial storage type required by the trait. */
    struct spatial_storage_type
    {
        /** The primary object stored in the mapped file. */
        primary_store _primary;

        /** The secondary object stored in the mapped file. */
        secondary_store _secondary;

        /** The memory mapped data required to be stored. */
        helpers::mapped_file_helper::mapped_storage _mstorage;

        /** The information about the initialization process of the mapped file
            and adapter. */
        initialization_info _info;

        /**
         * @brief Construct a new spatial_storage_type object.
         *
         */
        spatial_storage_type() :
            _primary(0),
            _secondary(0),
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
        primary_adapter_impl
        >::type spatial_storage_iterator_type;

    /** The update iterator type required by the trait. */
    typedef typename s1o::traits::spatial_storage_update_iterator_type<
        primary_adapter_impl
        >::type spatial_storage_update_iterator_type;

    /** The query iterator type required by the trait. */
    typedef typename s1o::traits::spatial_storage_query_iterator_type<
        primary_adapter_impl
        >::type spatial_storage_query_iterator_type;

    /** The query iterator type used for queries to the multiindex. */
    template <typename Index>
    struct secondary_query_iterator_type
    {
        typedef typename secondary_adapter_impl::template
            spatial_storage_query_iterator_type<
            Index
            >::type type;
    };

    /** The implementation of the primary adapter that handles everything. */
    const primary_adapter_impl _primary_adapter_impl;

    /** The implementation of the secondary adapter that enables additional
        queries. */
    const secondary_adapter_impl _secondary_adapter_impl;

    /** The object that handles memory mapped file initializations. */
    const helpers::mapped_file_helper _file_helper;

    /** The parameters used to control the creation of the mapped file. */
    const helpers::mapped_file_helper::params_t _mparams;

    /** The extension to use for the mifile. */
    const std::string _file_extension;

    /** The memory prefix used when constructing objects in the file. */
    const std::string _memory_prefix;

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param mparams The parameters used to control the creation of the
     * mapped file.
     * @param file_extension The extension to use for the mifile.
     * @param memory_prefix The prefix to add to any construct operation in the
     * memory mapped file.
     */
    spatial_adapter_impl(
        const mparams_t& mparams=mparams_t(),
        const std::string& file_extension=".sidx",
        const std::string& memory_prefix=""
    ) :
        _primary_adapter_impl(),
        _secondary_adapter_impl(),
        _file_helper(),
        _mparams(mparams),
        _file_extension(file_extension),
        _memory_prefix(memory_prefix +
            "s1o::spatial_adapters::auxiliary_multiindex_disk_slim/")
    {
    }

    /**
     * @brief Construct a new spatial_adapter_impl object.
     *
     * @param primary A primary adapter to copy-construct the internal one.
     * @param mparams The parameters used to control the creation of the
     * mapped file.
     * @param file_extension The extension to use for the mifile.
     * @param memory_prefix The prefix to add to any construct operation in the
     * memory mapped file.
     */
    spatial_adapter_impl(
        const primary_adapter_impl& primary,
        const mparams_t& mparams=mparams_t(),
        const std::string& file_extension=".sidx",
        const std::string& memory_prefix=""
    ) :
        _primary_adapter_impl(primary),
        _secondary_adapter_impl(),
        _file_helper(),
        _mparams(mparams),
        _file_extension(file_extension),
        _memory_prefix(memory_prefix +
            "s1o::spatial_adapters::auxiliary_multiindex_disk_slim/")
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
    std::string get_sindex_name(
        const std::string& basename
    ) const
    {
        return basename + _file_extension;
    }

    /**
     * @brief Check if the primary spatial storage is empty
     *
     * @param st A reference to the spatial storage object.
     *
     * @return true The primary spatial storage object is empty.
     * @return false The primary spatial storage object is not empty.
     */
    bool empty(
        const spatial_storage_type& st
    ) const
    {
        return _primary_adapter_impl.empty(st._primary);
    }

    /**
     * @brief Compare two spatial points from the primary storage.
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
        return _primary_adapter_impl.equals(first, second);
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
        *iter = get_sindex_name(basename);
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
        std::string mifile = get_sindex_name(data.basename);

        helpers::tuple_callback<
            TMetaAdapter,
            boost::tuple<
                const primary_adapter_impl&,
                const secondary_adapter_impl&>,
            boost::tuple<
                primary_store&,
                secondary_store&>,
            ITN,
            ITM
            > callback(
                meta_adapter,
                boost::tie(
                    _primary_adapter_impl,
                    _secondary_adapter_impl),
                boost::tie(
                    st._primary,
                    st._secondary),
                nodebegin, nodeend, metabegin, metaend);

        _file_helper.initialize(mifile, _mparams, data, _memory_prefix,
            st._mstorage, st._info.mapped_file, callback);

        st._info.adapters_size_bytes =
            _file_helper.get_size_bytes(st._mstorage) -
            _file_helper.get_free_bytes(st._mstorage);
    }

    /**
     * @brief Get the boundaries of the data stored in the primary spatial
     * storage.
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
        _primary_adapter_impl.bounds(st._primary, minpoint, maxpoint);
    }

    /**
     * @brief Get the boundaries of the data stored in the secondary spatial
     * storage.
     *
     * @param st The spatial storage object being initialized.
     * @param minpoint The smallest coordinates in the spatial storage.
     * @param maxpoint The largest coordinates in the spatial storage.
     */
    void secondary_bounds(
        const spatial_storage_type& st,
        secondary_spatial_point_type& minpoint,
        secondary_spatial_point_type& maxpoint
    ) const
    {
        _secondary_adapter_impl.bounds(st._secondary, minpoint, maxpoint);
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy the predicates in the primary spatial storage.
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
        _primary_adapter_impl.query(st._primary, predicates, begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of a sequence of
     * elements that satisfy a closed interval query in the secondary spatial
     * storage.
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
    void secondary_query(
        const spatial_storage_type& st,
        const queries::closed_interval<Index>& predicates,
        typename secondary_query_iterator_type<Index>::type& begin,
        typename secondary_query_iterator_type<Index>::type& end
    ) const
    {
        _secondary_adapter_impl.query(st._secondary, predicates, begin, end);
    }

    /**
     * @brief Get the iterator to the beginning of the primary spatial storage.
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
        return _primary_adapter_impl.sbegin(st._primary);
    }

    /**
     * @brief Get the iterator to the end of the primary spatial storage.
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
        return _primary_adapter_impl.send(st._primary);
    }

    /**
     * @brief Get the read-only iterator to the beginning of the primary
     * spatial storage.
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
        return _primary_adapter_impl.sbegin_update(st._primary);
    }

    /**
     * @brief Get the read-only iterator to the end of the primary spatial
     * storage.
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
        return _primary_adapter_impl.send_update(st._primary);
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

/**
 * @brief Wrapper for creating a dataset using the
 * auxiliary_multiindex_disk_slim as the spatial adapter.
 *
 * @tparam TMetaAdapter A helper type used to interface the metadata.
 *
 * @note See 'dataset'.
 */
template <typename TMetaAdapter>
class make_dataset : public dataset<TMetaAdapter,
    auxiliary_multiindex_disk_slim<PrimaryAdapter, Indices> >
{
public:

    /** The inherited dataset type. */
    typedef s1o::dataset<TMetaAdapter, auxiliary_multiindex_disk_slim<
        PrimaryAdapter, Indices> > base_type;

    /**
     * @brief The type of the iterator that converts nodes from the spatial
     * storage into objects in queries.
     *
     * @tparam Index The type of the index in the multiindex used to make the
     * query.
     */
    template <typename Index>
    struct secondary_q_iterator
    {
        /** The type of the iterator for the current index. */
        typedef typename base_type::TSpatialAdapterImpl::template
            secondary_query_iterator_type<
            Index
            >::type secondary_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata objects in queries. */
        typedef boost::transform_iterator<
            typename base_type::iter_builder::transform_q_get_meta,
            secondary_type,
            typename base_type::iter_builder::
                transform_q_get_meta::reference,
            typename base_type::iter_builder::
                transform_q_get_meta::value_type
            > meta_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata-data pairs without slot offset in queries. */
        typedef boost::transform_iterator<
            typename base_type::iter_builder::transform_q_get_elem,
            secondary_type,
            typename base_type::iter_builder::
                transform_q_get_elem::reference,
            typename base_type::iter_builder::
                transform_q_get_elem::value_type
            > elem_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata-data pairs with slot offset in queries. */
        typedef boost::transform_iterator<
            typename base_type::iter_builder::transform_q_get_elem_slot,
            secondary_type,
            typename base_type::iter_builder::
                transform_q_get_elem_slot::reference,
            typename base_type::iter_builder::
                transform_q_get_elem_slot::value_type
            > elem_slot_type;
    };

    /**
     * @brief The type of the iterator that converts nodes from the spatial
     * storage into objects in queries.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     */
    template <typename Predicates>
    struct secondary_q_iterator_p
    {
        /** The type of index, inferred from the predicate. */
        typedef typename s1o::traits::spatial_point_type<
            Predicates
            >::type Index;

        /** The type of the iterator for the current index, inferred from the
            predicate. */
        typedef typename secondary_q_iterator<
            Index
            >::secondary_type secondary_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata objects in queries. */
        typedef typename secondary_q_iterator<
            Index
            >::meta_type meta_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata-data pairs without slot offset in queries. */
        typedef typename secondary_q_iterator<
            Index
            >::elem_type elem_type;

        /** The type of the iterator that converts nodes from the spatial
            storage into metadata-data pairs with slot offset in queries. */
        typedef typename secondary_q_iterator<
            Index
            >::elem_slot_type elem_slot_type;
    };

public:

    /**
     * @brief Construct a new make_dataset object.
     *
     * @param basepath The path and filename of the dataset
     * without the trailing extensions '.meta' and '.data'.
     * @param mode A combination of dataset_open modes determining
     * how the dataset files are going to be opened / created.
     * @param flags A combination of dataset_flags determining
     * additional options for handling the dataset being opened.
     * @param meta_adapter The object used to retrieve information
     * from the metadata.
     * @param spatial_adapter The object used to construct and access
     * information from the spatial storage.
     *
     * @note Opening a dataset with Read/Write/Push (RWP) support and
     * without the NO_DATA flag requires te ALLOW_UNSORTED flag, because
     * no spatial storage will be created to ensure the data is sorted.
     *
     * @note Opening a dataset with the NO_DATA flag requires num_slots
     * to be zero.
     *
     * @note New datasets require num_slots to be equal to one, unless
     * the NO_DATA flag is set, then it should be zero.
     *
     * @note New datasets imply RWP.
     */
    make_dataset(
        const std::string& basepath,
        int mode,
        int flags,
        size_t num_slots,
        const TMetaAdapter& meta_adapter=TMetaAdapter(),
        const typename base_type::TSpatialAdapterImpl& spatial_adapter=
            typename base_type::TSpatialAdapterImpl()
    ) :
        base_type(
            basepath,
            mode,
            flags,
            num_slots,
            meta_adapter,
            spatial_adapter)
    {
    }

    /**
     * @brief Construct a new make_dataset object from a sequence of
     * existing metadata objects.
     *
     * @tparam IT The type of the iterator used to get the
     * metadata from.
     *
     * @param basepath The path and filename of the dataset
     * without the trailing extensions '.meta' and '.data'.
     * @param flags A combination of dataset_flags determining
     * additional options for handling the dataset being opened.
     * @param metabegin The iterator pointing to the beginning
     * of a sequence of metadata objects.
     * @param metaend The iterator pointing to after the last
     * element of a sequence of metadata objects.
     * @param meta_adapter The object used to retrieve information
     * from the metadata.
     * @param spatial_adapter The object used to construct and access
     * information from the spatial storage.
     */
    template <typename IT>
    make_dataset(
        const std::string& basepath,
        int flags,
        size_t num_slots,
        IT metabegin,
        IT metaend,
        const TMetaAdapter& meta_adapter=TMetaAdapter(),
        const typename base_type::TSpatialAdapterImpl& spatial_adapter=
            typename base_type::TSpatialAdapterImpl()
    ) :
        base_type(
            basepath,
            flags,
            num_slots,
            metabegin,
            metaend,
            meta_adapter,
            spatial_adapter)
    {
    }

    template <typename T>
    void secondary_bounds(
        T& minpoint,
        T& maxpoint
    ) const
    {
        base_type::get_spatial_adapter().secondary_bounds(
            base_type::get_spatial_storage(), minpoint, maxpoint);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs that satisfy a set of predicates, at data slot 0.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     * @param begin The iterator to the beginning of the metadata-data pairs
     * that satisfy a set of predicates, at data slot 0.
     * @param end The iterator to the end of the metadata-data pairs  that
     * satisfy a set of predicates, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    template<typename Predicates>
    void secondary_query_elements(
        const Predicates& predicates,
        typename secondary_q_iterator_p<Predicates>::elem_type& begin,
        typename secondary_q_iterator_p<Predicates>::elem_type& end
    ) const
    {
        base_type::assert_has_location_data();
        base_type::assert_has_data();

        typedef typename secondary_q_iterator_p<
            Predicates
            >::secondary_type secondary_q_iterator;

        secondary_q_iterator qbegin, qend;

        base_type::get_spatial_adapter().secondary_query(
            base_type::get_spatial_storage(), predicates, qbegin, qend);

        begin = typename secondary_q_iterator_p<Predicates>::elem_type(
            qbegin, typename base_type::iter_builder::
            transform_q_get_elem(this));

        end = typename secondary_q_iterator_p<Predicates>::elem_type(
            qend, typename base_type::iter_builder::
            transform_q_get_elem(this));
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, at data
     * slot 0.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     *
     * @return std::pair<
     *  typename secondary_q_iterator_p<Predicates>::elem_type,
     *  typename secondary_q_iterator_p<Predicates>::elem_type
     *  > A pair of iterators pointing to the beginning and end of the
     * metadata-data pairs inside a hypercube formed by the specified points,
     * at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    template<typename Predicates>
    std::pair<
        typename secondary_q_iterator_p<Predicates>::elem_type,
        typename secondary_q_iterator_p<Predicates>::elem_type
        > secondary_query_elements(
        const Predicates& predicates
    ) const
    {
        typedef typename secondary_q_iterator_p<
            Predicates
            >::elem_type elem_q_iterator;

        elem_q_iterator begin, end;
        secondary_query_elements(predicates, begin, end);
        return std::make_pair(begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs that satisfy a set of predicates, with data slot selection.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     * @param slot The slot of data to iterate over.
     * @param begin The iterator to the beginning of the metadata-data pairs
     * that satisfy a set of predicates, with data slot selection.
     * @param end The iterator to the end of the metadata-data pairs that
     * satisfy a set of predicates, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    template<typename Predicates>
    void secondary_query_elements(
        const Predicates& predicates,
        size_t slot,
        typename secondary_q_iterator_p<Predicates>::elem_slot_type& begin,
        typename secondary_q_iterator_p<Predicates>::elem_slot_type& end
    ) const
    {
        base_type::assert_has_location_data();
        base_type::assert_has_data();

        typedef typename secondary_q_iterator_p<
            Predicates
            >::secondary_type secondary_q_iterator;

        const size_t slot_off = base_type::compute_slot_offset(slot);

        secondary_q_iterator qbegin, qend;

        base_type::get_spatial_adapter().secondary_query(
            base_type::get_spatial_storage(), predicates, qbegin, qend);

        begin = typename secondary_q_iterator_p<
            Predicates
            >::elem_slot_type(qbegin, typename base_type::iter_builder::
            transform_q_get_elem_slot(this, slot_off));

        end = typename secondary_q_iterator_p<
            Predicates
            >::elem_slot_type(qend, typename base_type::iter_builder::
            transform_q_get_elem_slot(this, slot_off));
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, with data slot
     * selection.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     *
     * @return std::pair<
     *  typename secondary_q_iterator_p<Predicates>::elem_slot_type,
     *  typename secondary_q_iterator_p<Predicates>::elem_slot_type
     *  > A pair of iterators pointing to the beginning and end of the
     * metadata-data pairs inside a hypercube formed by the specified points,
     * with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     *
     * @note minpoint must contain values smaller than maxpoint, otherwise
     * there is no guarantee the query will succeed.
     */
    template<typename Predicates>
    std::pair<
        typename secondary_q_iterator_p<Predicates>::elem_slot_type,
        typename secondary_q_iterator_p<Predicates>::elem_slot_type
        > secondary_query_elements(
        const Predicates& predicates,
        size_t slot
    ) const
    {
        typedef typename secondary_q_iterator_p<
            Predicates
            >::elem_slot_type elem_q_iterator_slot;

        elem_q_iterator_slot begin, end;
        secondary_query_elements(predicates, slot, begin, end);
        return std::make_pair(begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata
     * objects that satisfy a set of predicates.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     * @param begin The iterator to the beginning of the metadata objects that
     * satisfy a set of predicates.
     * @param end The iterator to the end of the metadata objects that satisfy
     * a set of predicates.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     */
    template<typename Predicates>
    void secondary_query_metadata(
        const Predicates& predicates,
        typename secondary_q_iterator_p<Predicates>::meta_type& begin,
        typename secondary_q_iterator_p<Predicates>::meta_type& end
    ) const
    {
        base_type::assert_has_location_data();
        base_type::assert_has_data();

        typedef typename secondary_q_iterator_p<
            Predicates
            >::secondary_type secondary_q_iterator;

        secondary_q_iterator qbegin, qend;

        base_type::get_spatial_adapter().secondary_query(
            base_type::get_spatial_storage(), predicates, qbegin, qend);

        begin = typename secondary_q_iterator_p<Predicates>::meta_type(
            qbegin, typename base_type::iter_builder::
            transform_q_get_meta(this));

        end = typename secondary_q_iterator_p<Predicates>::meta_type(
            qend, typename base_type::iter_builder::
            transform_q_get_meta(this));
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata
     * objects inside a hypercube formed by the specified points, with data
     * slot selection.
     *
     * @tparam Predicates The type representing the predicates to be satisfied
     * by the elements in the storage.
     *
     * @param predicates The predicates to be satisfied by the elements in the
     * storage.
     *
     * @return std::pair<
     *  typename secondary_q_iterator_p<Predicates>::meta_type,
     *  typename secondary_q_iterator_p<Predicates>::meta_type
     *  > A pair of iterators pointing to the beginning and end of the
     * metadata objects inside a hypercube formed by the specified points,
     * with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note minpoint must contain values smaller than maxpoint, otherwise
     * there is no guarantee the query will succeed.
     */
    template<typename Predicates>
    std::pair<
        typename secondary_q_iterator_p<Predicates>::meta_type,
        typename secondary_q_iterator_p<Predicates>::meta_type
        > secondary_query_metadata(
        const Predicates& predicates
    ) const
    {
        typedef typename secondary_q_iterator_p<
            Predicates
            >::meta_type meta_q_iterator;

        meta_q_iterator begin, end;
        secondary_query_metadata(predicates, begin, end);
        return std::make_pair(begin, end);
    }
};

};

}}
