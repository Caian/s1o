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
#include <s1o/initialization_data/default_data.hpp>
#include <s1o/initialization_data/mapped_data.hpp>

#include <boost/interprocess/managed_mapped_file.hpp>

#include <string>

namespace s1o {
namespace helpers {

class mapped_file_helper
{
public:

    /** Forward the definition of the memory mapped file. */
    typedef initialization_data::mapped_data::mfile_t mfile_t;

    /** The segment manager that handles data allocation. */
    typedef mfile_t::segment_manager manager_t;

    /** The configuration parameters for the mapped file */
    struct params_t
    {
        /** The initial raw size of the mapped file in bytes. */
        size_t starting_file_size;

        /** The increment in bytes to be made to the mapped file if the
            previous size is too small to allocate the tree. */
        size_t file_increment;

        /** The maximum number of resize attempts to be made when the
            allocation of the objects fails. */
        size_t resize_attempts;

        /**
         * @brief Construct a new params_t object.
         *
         * @param starting_file_size The initial raw size of the mapped file
         * in bytes.
         * @param file_increment The increment in bytes to be made to the
         * mapped file if the previous size is too small to allocate the tree.
         * @param resize_attempts The maximum number of resize attempts to be
         * made when the allocation of the objects fails.
         * @param read_only Indicate if an existing mapped file should be
         * opened as read-only.
         */
        params_t(
            size_t starting_file_size=512ULL*1024*1024,
            size_t file_increment=512ULL*1024*1024,
            size_t resize_attempts=5
        ) :
            starting_file_size(starting_file_size),
            file_increment(file_increment),
            resize_attempts(resize_attempts)
        {
        }
    };

    /** The data that must persist with the storage. */
    struct mapped_storage
    {
        /** The pointer to the memory mapped file. */
        mfile_t* _mfile;

        /**
         * @brief Construct a new mapped_storage object.
         *
         */
        mapped_storage() :
            _mfile(0)
        {
        }

        /**
         * @brief Destroy the mapped_storage object.
         *
         */
        virtual ~mapped_storage()
        {
            delete _mfile;
        }
    };

    /** The struct containing information about the initialization process of
        the mapped file and data structures. */
    struct initialization_info
    {
        size_t raw_size_bytes;
        size_t initial_free_bytes;
        size_t size_bytes;
        size_t attempts;

        /**
         * @brief Construct a new initialization_info object.
         *
         */
        initialization_info(
        ) :
            raw_size_bytes(0),
            initial_free_bytes(0),
            size_bytes(0),
            attempts(0)
        {
        }
    };

private:

    /**
     * @brief Open an existing file and initialize the objects from the
     * existing data.
     *
     * @tparam Callback The type of the functor that initializes the objects
     * in the memory mapped file.
     *
     * @param filename The path and filename of the memory mapped file.
     * @param params The parameters used to control the creation of the
     * mapped file.
     * @param data The initialization data for the storage.
     * @param memory_prefix The prefix to add to any construct operation in
     * the memory mapped file.
     * @param st A reference to the mapped storage object.
     * @param info The resulting initialization information for the memory
     * mapped file.
     * @param callback The functor that initializes the objects  in the memory
     * mapped file.
     */
    template <typename Callback>
    void open_existing(
        const std::string& filename,
        const params_t& params,
        const initialization_data::default_data& data,
        const std::string& memory_prefix,
        mapped_storage& st,
        initialization_info& info,
        Callback& callback
    ) const
    {
        info.attempts = 0;

        try {
            // Force open the file
            st._mfile = new boost::interprocess::managed_mapped_file(
                boost::interprocess::open_read_only, filename.c_str());
        }
        catch (const std::exception& ex) {

            if (boost::get_error_info<ex3::traced>(ex) == 0) {
                EX3_THROW(
                    EX3_ENABLE(ex)
                        << file_name(filename));
            }
            else {
                EX3_RETHROW(
                    EX3_ENABLE(ex)
                        << file_name(filename));
            }
        }

        info.raw_size_bytes = get_raw_size_bytes(st);
        info.initial_free_bytes = get_free_bytes(st);
        info.size_bytes = get_size_bytes(st);

        // Retrieve the contents of the storage

        initialization_data::mapped_data mdata(data,
            memory_prefix, st._mfile);

        callback(mdata);
    }

    /**
     * @brief Create a new file and construct the objects inside it.
     *
     * @tparam Callback The type of the functor that initializes the objects
     * in the memory mapped file.
     *
     * @param filename The path and filename of the memory mapped file.
     * @param params The parameters used to control the creation of the
     * mapped file.
     * @param data The initialization data for the storage.
     * @param memory_prefix The prefix to add to any construct operation in
     * the memory mapped file.
     * @param st A reference to the mapped storage object.
     * @param info The resulting initialization information for the memory
     * mapped file.
     * @param callback The functor that initializes the objects  in the memory
     * mapped file.
     */
    template <typename Callback>
    void create_new(
        const std::string& filename,
        const params_t& params,
        const initialization_data::default_data& data,
        const std::string& memory_prefix,
        mapped_storage& st,
        initialization_info& info,
        Callback& callback
    ) const
    {
        // Try to allocate the map file with increasing
        // size until a limit is reached

        info.size_bytes = 0;

        for (size_t attempt = 0;
                    attempt <= params.resize_attempts;
                    attempt++) {

            // Compute the new mapped file size
            const size_t file_size = params.starting_file_size +
                attempt * params.file_increment;

            info.raw_size_bytes = file_size;
            info.attempts = attempt+1;

            // Remove the old file
            boost::interprocess::file_mapping::remove(filename.c_str());

            // Force create the new file
            st._mfile = new boost::interprocess::managed_mapped_file(
                boost::interprocess::create_only, filename.c_str(),
                file_size);

            info.initial_free_bytes = get_free_bytes(st);

            try {

                // Initialize the contents of the mapped file

                initialization_data::mapped_data mdata(data,
                    memory_prefix, st._mfile);

                callback(mdata);

                break;
            }
            catch (const boost::interprocess::bad_alloc&) {

                // Delete the old file

                delete st._mfile;
                st._mfile = 0;

                // Trigger a failed callback to clear the storage

                initialization_data::mapped_data mdata(data,
                    memory_prefix, 0);

                callback(mdata);

                // Give up if the maximum number of attempts is reached
                if (attempt == params.resize_attempts) {
                    EX3_THROW(index_size_too_big_exception()
                        << maximum_attempts(attempt)
                        << maximum_size(file_size)
                        << file_name(filename));
                }

                // Try again
                continue;
            }
            catch (const s1o_exception& e)
            {
                EX3_RETHROW(e
                    << file_name(filename));
            }
        }

        info.size_bytes = get_size_bytes(st);
    }

public:

    /**
     * @brief Get the pointer to the memory mapped file.
     *
     * @param st A reference to the mapped storage object.
     *
     * @return const mfile_t* The pointer to the memory mapped file.
     */
    const mfile_t* get_mfile(
        const mapped_storage& st
    ) const
    {
        return st._mfile;
    }

    /**
     * @brief Get the pointer to the memory mapped file.
     *
     * @param st A reference to the mapped storage object.
     *
     * @return mfile_t* The pointer to the memory mapped file.
     */
    mfile_t* get_mfile(
        mapped_storage& st
    ) const
    {
        return st._mfile;
    }

    /**
     * @brief Get the pointer to the segment manager of the mapped file.
     *
     * @param st A reference to the mapped storage object.
     * @return const manager_t* The pointer to the segment manager of the
     * mapped file.
     */
    const manager_t* get_segment_manager(
        const mapped_storage& st
    ) const
    {
        const mfile_t* mfile = get_mfile(st);

        if (mfile == 0) {
            EX3_THROW(null_mapped_file_pointer_exception());
        }

        return mfile->get_segment_manager();
    }

    /**
     * @brief Get the pointer to the segment manager of the mapped file.
     *
     * @param st A reference to the mapped storage object.
     * @return manager_t* The pointer to the segment manager of the mapped
     * file.
     */
    manager_t* get_segment_manager(
        mapped_storage& st
    ) const
    {
        mfile_t* mfile = get_mfile(st);

        if (mfile == 0) {
            EX3_THROW(null_mapped_file_pointer_exception());
        }

        return mfile->get_segment_manager();
    }

    /**
     * @brief Get the size in bytes of the allocatable mapped region.
     *
     * @param st A reference to the mapped storage object.
     *
     * @return size_t The size in bytes of the mapped region.
     *
     * @note This size may be different from the raw size due to extra file
     * metadata.
     */
    size_t get_size_bytes(
        const mapped_storage& st
    ) const
    {
        return get_segment_manager(st)->get_size();
    }

    /**
     * @brief Get the free space in bytes of the allocatable mapped region.
     *
     * @param st A reference to the mapped storage object.
     *
     * @return size_t The free space in bytes of the allocatable mapped
     * region.
     */
    size_t get_free_bytes(
        const mapped_storage& st
    ) const
    {
        return get_segment_manager(st)->get_free_memory();
    }

    /**
     * @brief Get the used space in bytes of the allocatable mapped region.
     *
     * @param st A reference to the mapped storage object.
     *
     * @return size_t The used space in bytes of the allocatable mapped
     * region.
     */
    size_t get_used_bytes(
        const mapped_storage& st
    ) const
    {
        return get_size_bytes(st) - get_free_bytes(st);
    }

    /**
     * @brief Get the size in bytes of the mapped file (raw size).
     *
     * @param st A reference to the mapped storage object.
     *
     * @return size_t The size in bytes of the mapped file (raw size).
     *
     * @note The actual size of the file may be different if it is sparse.
     */
    size_t get_raw_size_bytes(
        const mapped_storage& st
    ) const
    {
        const mfile_t* mfile = get_mfile(st);

        if (mfile == 0) {
            EX3_THROW(null_mapped_file_pointer_exception());
        }

        return mfile->get_size();
    }

    /**
     * @brief Initialize the mapped storage and its contents.
     *
     * @tparam Callback The type of the functor that initializes the objects
     * in the memory mapped file.
     *
     * @param filename The path and filename of the memory mapped file.
     * @param params The parameters used to control the creation of the
     * mapped file.
     * @param data The initialization data for the storage.
     * @param memory_prefix The prefix to add to any construct operation in
     * the memory mapped file.
     * @param st A reference to the mapped storage object.
     * @param info The resulting initialization information for the memory
     * mapped file.
     * @param callback The functor that initializes the objects  in the memory
     * mapped file.
     */
    template <typename Callback>
    void initialize(
        const std::string& filename,
        const params_t& params,
        const initialization_data::default_data& data,
        const std::string memory_prefix,
        mapped_storage& st,
        initialization_info& info,
        Callback& callback
    ) const
    {
        if (data.is_new) {

            if (!data.can_write) {
                EX3_THROW(read_only_exception()
                    << file_name(filename));
            }

            create_new(filename, params, data, memory_prefix,
                st, info, callback);
        }
        else {

            open_existing(filename, params, data, memory_prefix,
                st, info, callback);
        }
    }

    /**
     * @brief Destroy the contents of the mapped storage object.
     *
     * @param st A reference to the mapped storage object.
     */
    void destroy(
        mapped_storage& st
    ) const
    {
        (void)st;
    }
};

}}
