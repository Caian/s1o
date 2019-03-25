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

#include "types.hpp"
#include "checked.hpp"
#include "metadata.hpp"
#include "exceptions.hpp"
#include "queries/nearest.hpp"
#include "queries/closed_interval.hpp"
#include "traits/metadata_type.hpp"
#include "traits/num_spatial_dims.hpp"
#include "traits/supports_element_pair.hpp"
#include "traits/spatial_value_type.hpp"
#include "traits/spatial_point_type.hpp"
#include "traits/spatial_adapter_impl.hpp"
#include "traits/spatial_storage_type.hpp"
#include "traits/spatial_storage_iterator_type.hpp"
#include "traits/spatial_storage_query_iterator_type.hpp"
#include "traits/spatial_storage_update_iterator_type.hpp"
#include "transforms/transform_ds_get_element.hpp"
#include "transforms/transform_ds_get_location.hpp"
#include "transforms/transform_ds_read_meta.hpp"
#include "initialization_data/default_data.hpp"
#include "helpers/iter_builder.hpp"
#include "helpers/copy_location.hpp"

#include "z0rg/zero_copy.hpp"

#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>
#include <string>

#include <errno.h>

#define S1O_VERSION_MAJOR    0
#define S1O_VERSION_MINOR    1
#define S1O_VERSION_REVISION 0

namespace s1o {

/**
 * @brief Dataset open modes.
 *
 */
enum dataset_open
{
    /** Allow creating files if the do not exist and writing to them. */
    S1O_OPEN_WRITE = 1,

    /** Set the file size to zero if it already exists. */
    S1O_OPEN_TRUNC = 2,

    /** Write + Trunc, create an empty file existing allowing writing. */
    S1O_OPEN_NEW   = S1O_OPEN_TRUNC | S1O_OPEN_WRITE,
};

/**
 * @brief Dataset open flags.
 *
 */
enum dataset_flags
{
    /** Allow Read, Write and Push operations (no memory mapping). */
    S1O_FLAGS_RWP            = 1,

    /** Do not open the data file. */
    S1O_FLAGS_NO_DATA        = 2,

    /** Do not check for unsorted data in the data file. */
    S1O_FLAGS_ALLOW_UNSORTED = 4,

    /** Do not check if the data offsets overlap with each other or
        pass the limit of the file / mapped memory. */
    S1O_FLAGS_NO_DATA_CHECK = 8,
};

#pragma pack(push, 4)
struct meta_base_structure
{
    unsigned int one;      //  0 - 3
    unsigned int uintsz;   //  4 - 7
    unsigned int fofsz;    //  8 - 11
    unsigned int checksz;  // 12 - 15
    unsigned int metasz;   // 16 - 19
    unsigned int version;  // 20 - 23
    unsigned int revision; // 24 - 27
    char magic[8];         // 28 - 35
};
#pragma pack(pop)

/**
 * @brief Representation of a memory region.
 *
 */
struct _mregion
{
    char* const addr; /** The address of the region. */
    const size_t size; /** The size of the region. */

    /**
     * @brief Construct a new memory region object.
     *
     * @param addr The address of the region.
     * @param size The size of the region.
     */
    _mregion(
        char* const addr,
        size_t size
    ) :
        addr(addr),
        size(size)
    {
    }
};

/**
 * @brief Class responsible for managing the file descriptors
 * and memory mapping for a dataset.
 *
 */
class _dataset_fd_base
{
public:

    static const int FD_META = 0; /** Identifies the meta file. */
    static const int FD_DATA = 1; /** Identifies the data file. */

private:

    /** The permission when creating file, user read+write. */
    static const int file_sharing_mode = S_IRUSR | S_IWUSR;

    /** The base flags when mapping a file to memory, they may be
        changed by the method. */
    static const int mapping_flags = MAP_SHARED;

private:

    const std::string _basename; /** The file name + path of the dataset
                                     without the trailing extensions. */

    const std::string _meta_ext; /** The extension of the metadata file,
                                     without the leading dot. */

    const std::string _data_ext; /** The extension of the data file, without
                                     the leading dot. */

    const bool _can_write; /** The dataset is open for writting. */

    const bool _no_data; /** Only the meta file is open, not the data file. */

    const int _fd_meta; /** Meta file descriptor. */
    const int _fd_data; /** Data file descriptor. */

    const bool _is_mmapped; /** Meta and data files are mapped to memory
                               for better caching management. */

    const _mregion _m_meta; /** The representation of the
                                memory-mapped meta file. */
    const _mregion _m_data; /** The representation of the
                                memory-mapped data file. */

    /**
     * @brief Get the position of the cursor in a file descriptor.
     *
     * @param fd The file descriptor.
     *
     * @return off64_t The position of the cursor in a file descriptor.
     */
    inline off64_t get_file_position_fd(
        int fd
    ) const
    {
        try {
            return lseek64_checked(fd, 0, SEEK_CUR);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename));
        }
    }

    /**
     * @brief Get the size of a file.
     *
     * @param fd The file descriptor.
     *
     * @return off64_t The size of the file.
     *
     * @note This method will preserve the original
     * position of the file descriptor's cursor.
     */
    inline off64_t get_file_size_fd(
        int fd
    ) const
    {
        try {

            // Store the current position
            const off64_t pos = get_file_position_fd(fd);

            // Move to the end of the file and get its size
            lseek64_checked(fd, 0, SEEK_END);
            const off64_t size = get_file_position_fd(fd);

            // Restore the original position
            lseek64_checked(fd, pos, SEEK_SET);

            return size;
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename));
        }
    }

    /**
     * @brief Open a file specified by a path.
     *
     * @param path The path to the file.
     * @param new_file True if the file should be created
     * (if it does not exist) or truncated (if it does exist).
     * @param new_size The size of the file being created. It
     * must be zero if new_file is false.
     *
     * @return int The file descriptor of the opened file.
     *
     * @note Creating a new file with _can_write set to false
     * will cause an exception because it does not make sense
     * to truncate the file without the intent of writing to it.
     *
     * @note Opening a file without truncating (new_file = false)
     * and specifying a new_size != 0 will cause an exception
     * because the size of the file is already determined.
     */
    inline int open_file(
        const std::string& path,
        bool new_file,
        off64_t new_size
    ) const
    {
        // It only makes sense to create a new dataset
        // with write permission
        if (new_file && !can_write()) {
            EX3_THROW(create_without_write_exception()
                << dataset_name(_basename)
                << file_name(path));
        }

        // The size of the new file must be zero if the
        // dataset is not being created
        if (!new_file && new_size != 0) {
            EX3_THROW(open_with_size_exception()
                << dataset_name(_basename)
                << file_name(path));
        }

        // Use the dataset's mode to determine the
        // permissions of the
        const int flags = (can_write() ? O_RDWR : O_RDONLY) |
            (new_file ? (O_CREAT | O_TRUNC) : 0);

        try {

            const int fd = open_checked(path.c_str(),
                flags, file_sharing_mode);

            if (new_size > 0) {

                const char zero = 0;

                // Seek to new_size - 1 and write 1 byte
                // Resulting in a file with 'new_size' bytes

                lseek64_checked(fd, new_size-1, SEEK_SET);
                write_checked(fd, &zero, 1);

                // Seek back to the beginning of the file
                lseek64_checked(fd, 0, SEEK_SET);
            }

            return fd;
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename)
                << file_name(path));
        }
    }

    /**
     * @brief Get the file descriptor for either the
     * meta file or the data file.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     *
     * @note This method should not be used directly, instead,
     * one should use the methods get_fd_r and get_fd_w that
     * perform additional checks depending on the intended usage
     * of the file descriptor.
     */
    inline int _get_fd(
        int who
    ) const
    {
        switch (who) {
        case FD_DATA:
            if (no_data()) {
                EX3_THROW(no_data_exception()
                    << dataset_name(_basename));
            }
            return _fd_data;
        case FD_META:
            return _fd_meta;
        default:
            EX3_THROW(invalid_who_exception()
                << dataset_name(_basename)
                << who_value(who));
        }
    }

    /**
     * @brief Get the file descriptor for either the meta
     * file or the data file with the intent of reading.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     */
    inline int get_fd_r(
        int who
    ) const
    {
        if (_is_mmapped) {
            EX3_THROW(mmapped_exception()
                << dataset_name(_basename)
                << who_value(who));
        }

        return _get_fd(who);
    }

    /**
     * @brief Get the file descriptor for either the meta
     * file or the data file with the intent of writing.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     */
    inline int get_fd_w(
        int who
    ) const
    {
        if (_is_mmapped) {
            EX3_THROW(mmapped_exception()
                << dataset_name(_basename)
                << who_value(who));
        }

        if (!can_write()) {
            EX3_THROW(read_only_exception()
                << dataset_name(_basename)
                << who_value(who));
        }

        return _get_fd(who);
    }

    /**
     * @brief Get the file descriptor for either the meta
     * file or the data file with the intent of mapping it
     * in memory.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     */
    inline int get_fd_m(
        int who
    ) const
    {
        return _get_fd(who);
    }

    /**
     * @brief Get the file descriptor for either the meta
     * file or the data file with the intent of closing it.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     */
    inline int get_fd_c(
        int who
    ) const
    {
        return _get_fd(who);
    }

    /**
     * @brief Get the file descriptor for either the meta
     * file or the data file with the intent of seeking it.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return int The file descriptor for either the
     * meta file or the data file.
     */
    inline int get_fd_s(
        int who
    ) const
    {
        return _get_fd(who);
    }

    /**
     * @brief Map a source to memory.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return _mregion The memory region object for the mapped data.
     *
     * @note The dataset must have been opened with write permission
     * to be able to write to the resulting memory region.
     */
    inline _mregion map(
        int who
    ) const
    {
        const int fd = get_fd_m(who);

        try {
            return map_fd(fd);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << who_value(who));
        }
    }

    /**
     * @brief Map a file descriptor to memory.
     *
     * @param fd The file descriptor.
     *
     * @return _mregion The memory region object for the mapped data.
     *
     * @note The dataset must have been opened with write permission
     * to be able to write to the resulting memory region.
     */
    inline _mregion map_fd(
        int fd
    ) const
    {
        const off64_t size = get_file_size_fd(fd);

        if (size == 0) {
            EX3_THROW(empty_mmap_exception()
                << dataset_name(_basename));
        }

        const int prot = PROT_READ | (can_write() ? PROT_WRITE : 0);
        const int flags = mapping_flags;

        try {
            void* const ptr = mmap_checked(0, size, prot, flags, fd, 0);
            return _mregion(reinterpret_cast<char*>(ptr), size);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename));
        }
    }

    /**
     * @brief Unmap the specified memory region.
     *
     * @param The memory region to be unmapped.
     */
    inline void unmap_mem(
        const _mregion& region
    ) const
    {
        try {
            munmap_checked(region.addr, region.size);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename));
        }
    }

    /**
     * @brief Ensure the extensions for the metadata and the data files are
     * valid.
     *
     */
    inline void assert_extensions() const
    {
        if (_meta_ext.compare(_data_ext) == 0) {
            EX3_THROW(extensions_equal_exception()
                << file_extension(_meta_ext));
        }
    }

public:

    /**
     * @brief Construct a new _dataset_fd_base object
     *
     * @param basepath The path and filename of the dataset
     * without the trailing extensions.
     * @param meta_ext The extension of the metadata file, without
     * the leading dot.
     * @param meta_ext The extension of the data file, without
     * the leading dot.
     * @param mode A combination of dataset_open modes determining
     * how the dataset files are going to be opened / created.
     * @param no_data Do not open / create the data file, only
     * the metadata file.
     * @param map_fds Map the files to memory to improve caching.
     * @param new_meta_size The size of the meta file being created.
     * It must be zero if S1O_OPEN_TRUNC is specified in mode.
     * @param new_data_size The size of the data file being created.
     * If 'no_data' is specified, then this value is ignored, otherwise
     * it must be zero if S1O_OPEN_TRUNC is specified in mode.
     *
     * @note Specifying S1O_OPEN_TRUNC without S1O_OPEN_WRITE in
     * the mode will cause an exception to be thrown because it
     * does not make sense to truncate the dataset without the
     * intent of writing to it. Using S1O_OPEN_NEW is the
     * preferred method when creating a new dataset.
     *
     * @note Opening a dataset without truncating and specifying
     * anything other than zero for new_meta_size or new_data_size
     * will cause an exception because the size of the files are
     * already determined.
     *
     * @note Using S1O_OPEN_TRUNC with map_fds = true will cause
     * an exception if new_meta_size or new_data_size is equal to
     * zero, this is because data cannot be appended to the file
     * after it is mapped (memory remapping is out of the scope
     * of this project).
     *
     * @note Opening an empty dataset map_fds = true  will cause
     * an exception for the same reason as S1O_OPEN_TRUNC: The
     * mapping cannot occur if the size of the files is zero.
     *
     * @note If the dataset was created with no_data = true, then
     * it must be also opened with no_data = true in order to
     * use map_fds = true, otherwise an empty file may be created,
     * resulting in an exception.
     */
    inline _dataset_fd_base(
        const std::string& basepath,
        const std::string& meta_ext,
        const std::string& data_ext,
        int mode,
        bool no_data,
        bool map_fds,
        off64_t new_meta_size,
        off64_t new_data_size
    ) :
        _basename(basepath),
        _meta_ext(meta_ext),
        _data_ext(data_ext),
        _can_write((mode & S1O_OPEN_WRITE) != 0),
        _no_data(no_data),
        _fd_meta(
            open_file(
                get_meta_file_path(),
                (mode & S1O_OPEN_TRUNC) != 0,
                new_meta_size)),
        _fd_data(
            _no_data ?
                -1 :
                open_file(
                    get_data_file_path(),
                    (mode & S1O_OPEN_TRUNC) != 0,
                    new_data_size)),
        _is_mmapped(map_fds),
        _m_meta(_is_mmapped ?
            map(FD_META) :
            _mregion(0, 0)),
        _m_data((_is_mmapped && !_no_data) ?
            map(FD_DATA) :
            _mregion(0, 0))
    {
        assert_extensions();
    }

    /**
     * @brief Destroy the class, unmapping any mapped pointers and closing any
     * file descriptors.
     *
     */
    virtual ~_dataset_fd_base()
    {
        if (_is_mmapped) {

            unmap_mem(_m_meta);

            if (!no_data()) {
                unmap_mem(_m_data);
            }
        }

        close_checked(get_fd_c(FD_META));

        if (!no_data()) {
            close_checked(get_fd_c(FD_DATA));
        }
    }

    /**
     * @brief Get the basename used to open/create the dataset.
     *
     * @return const std::string The basename used to
     * open/create the dataset.
     */
    inline const std::string get_basename() const
    {
        return _basename;
    }

    /**
     * @brief Construct the path of a file.
     *
     * @param basename The path and filename of the dataset without the
     * trailing extensions.
     * @param ext The extension of the file, without the leading dot.
     *
     * @return std::string The path of a file.
     */
    inline static std::string get_file_path(
        const std::string& basename,
        const std::string& ext
    )
    {
        return basename + "." + ext;
    }

    /**
     * @brief Get the path of the meta file.
     *
     * @return std::string The path of the meta file.
     */
    inline std::string get_meta_file_path() const
    {
        return get_file_path(_basename, _meta_ext);
    }

    /**
     * @brief Get the path of the data file.
     *
     * @return std::string The path of the data file.
     */
    inline std::string get_data_file_path() const
    {
        return get_file_path(_basename, _data_ext);
    }

    /**
     * @brief Tell if the files in the dataset were
     * open for writing.
     *
     * @return true The dataset can be written to.
     * @return false The dataset cannot be written to.
     */
    inline bool can_write() const
    {
        return _can_write;
    }

    /**
     * @brief Tell if the data file from the dataset
     * is not open.
     *
     * @return true The data file is not open.
     * @return false The data file is open.
     */
    inline bool no_data() const
    {
        return _no_data;
    }

    /**
     * @brief Get the position of the cursor in a source.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return off64_t The position of the cursor in a file descriptor.
     */
    inline off64_t get_file_position(
        int who
    ) const
    {
        const int fd = get_fd_s(who);

        try {
            return get_file_position_fd(fd);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << who_value(who));
        }
    }

    /**
     * @brief Get the size of a source.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return off64_t The size of the file.
     *
     * @note This method will preserve the original
     * position of the file descriptor's cursor.
     */
    inline off64_t get_file_size(
        int who
    ) const
    {
        const int fd = get_fd_s(who);

        try {
            return get_file_size_fd(fd);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << who_value(who));
        }
    }

    /**
     * @brief Get the memory region of a memory-mapped
     * file descriptor.
     *
     * @param who The source (FD_META or FD_DATA).
     *
     * @return _mregion The memory-mapped region of the
     * file descriptor.
     */
    inline const _mregion& get_mapped(
        int who
    ) const
    {
        if (!_is_mmapped) {
            EX3_THROW(not_mmapped_exception()
                << dataset_name(_basename)
                << who_value(who));
        }

        switch (who) {
        case FD_META:
            return _m_meta;
        case FD_DATA:
            return _m_data;
        default:
            EX3_THROW(invalid_who_exception()
                << dataset_name(_basename)
                << who_value(who));
        }
    }

    /**
     * @brief Set the file position for reading / writing.
     *
     * @param who The source (FD_META or FD_DATA).
     * @param offset The offset in bytes relative to whence
     * to set the file pointer at.
     * @param whence The reference for setting the pointer at.
     *
     * @return off64_t The resulting offset location in bytes
     * from the beginning of the file.
     */
    inline off64_t seek(
        int who,
        off64_t offset,
        int whence
    ) const
    {
        const int fd = get_fd_s(who);

        try {
            return lseek64_checked(fd, offset, whence);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename)
                << who_value(who));
        }
    }

    /**
     * @brief Read a requence of bytes from a file source.
     *
     * @param who The source (FD_META or FD_DATA).
     * @param buf The destination for the data being read.
     * @param count The number of bytes to read.
     * @param complete Require all requested bytes to be
     * read or zero.
     * @param required Do not allow a zero size read.
     *
     * @return size_t The size read.
     */
    inline size_t read(
        int who,
        void* buf,
        size_t count,
        bool complete,
        bool required
    ) const
    {
        const int fd = get_fd_r(who);
        size_t sr;

        try {
            sr = read_checked(fd, buf, count);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename)
                << who_value(who));
        }

        // Check if the read was partial or not did not
        // happen when it was required
        if ((complete && sr != 0 && sr != count) || (required && sr == 0)) {
            EX3_THROW(incomplete_read_exception()
                << expected_size(count)
                << actual_size(sr)
                << dataset_name(_basename)
                << who_value(who));
        }

        return sr;
    }

    /**
     * @brief Write a sequence of bytes to a file source.
     *
     * @param who The source (FD_META or FD_DATA).
     * @param buf The source data to write.
     * @param count The number of bytes to write.
     *
     * @return size_t The number of bytes written.
     */
    inline size_t write(
        int who,
        const void* buf,
        size_t count
    ) const
    {
        const int fd = get_fd_w(who);

        try {
            write_checked(fd, buf, count);
        }
        catch (const io_exception& e) {
            EX3_RETHROW(e
                << dataset_name(_basename)
                << who_value(who));
        }

        return count;
    }

    /**
     * @brief Read a serializable type from a source.
     *
     * @tparam T The type of the object being read.
     *
     * @param who The source (FD_META or FD_DATA).
     * @param o The object read from the source.
     * @param required The object is required to be
     * read, so an EOF is an error.
     *
     * @note The size of the file is determined using
     * sizeof(T).
     */
    template <typename T>
    inline size_t read_object(
        int who,
        T& o,
        bool required
    ) const
    {
        const size_t s = sizeof(T);
        char* const po = reinterpret_cast<char*>(&o);
        return read(who, po, s, true, required);
    }

    /**
     * @brief Write a serializable type to a source.
     *
     * @tparam T The type of the object being written.
     *
     * @param who The source (FD_META or FD_DATA).
     * @param o The object to write.
     *
     * @note The size of the file is determined using
     * sizeof(T).
     */
    template <typename T>
    inline size_t write_object(
        int who,
        const T& o
    ) const
    {
        const size_t s = sizeof(T);
        const char* const po = reinterpret_cast<const char*>(&o);
        write(who, po, s);
        return s;
    }

    /**
     * @brief Ensure the data has been written to the disk.
     *
     * @param who The source (FD_META or FD_DATA).
     */
    inline void sync(
        int who
    ) const
    {
        if (_is_mmapped) {
            const _mregion& region = get_mapped(who);
            msync_checked(region.addr, region.size, MS_SYNC);
        }
        else {
            const int fd = get_fd_w(who);
            fsync_checked(fd);
        }
    }
};

/**
 * @brief Class for creating and managing in-disk spatially correlated
 * data, with split metadata / data and memory mapping capabilities.
 *
 * @tparam TMetaAdapter A helper type used to interface the metadata
 * with the dataset.
 * @tparam TSpatialAdapter A helper type used to interface the spatial
 * storage structure with the dataset.
 *
 * @note TMetaAdapter must be compatible with the following traits:
 *
 *  - num_spatial_dims
 *  - spatial_value_type
 *  - metadata_type
 *
 * @note TMetaAdapter object must expose the following methods:
 *
 *  - get_location
 *  - get_uid
 *  - set_uid
 *  - get_data_size
 *  - get_meta_check_ptr
 *  - get_meta_check_size
 *  - get_meta_file_ext
 *  - get_data_file_ext
 *
 * @note TSpatialAdapter must be compatible with the following traits:
 *
 *  - spatial_adapter_impl
 *
 * @note TSpatialAdapterImpl must be compatible with the following traits:
 *
 *  - supports_element_pair
 *  - spatial_point_type
 *  - spatial_storage_type
 *  - spatial_storage_iterator_type
 *  - spatial_storage_update_iterator_type
 *  - spatial_storage_query_iterator_type
 *
 * @note TSpatialAdapterImpl object must expose the following methods:
 *
 *  - bounds
 *  - destroy
 *  - empty
 *  - equals
 *  - get_extra_files
 *  - initialize
 *  - qbegin
 *  - qend
 *  - sbegin
 *  - send
 *  - sbegin_update
 *  - send_update
 */
template <typename TMetaAdapter, typename TSpatialAdapter>
class dataset
{
    /** The specialization of the current type. */
    typedef dataset<TMetaAdapter, TSpatialAdapter> this_type;

    /** Allow the transform direct access to the metadata addressing
        methods. */
    friend class transform_ds_get_meta<this_type>;

    /** Allow the transform direct access to the element addressing
        methods. */
    friend class transform_ds_get_element<this_type>;

    /** Allow the transform direct access to the element addressing
        methods with slot offset. */
    friend class transform_ds_get_element_slot<this_type>;

    /** Allow the transform direct access to the metadata addressing
        methods to extract location. */
    friend class transform_ds_get_location<this_type>;

private:

    /** Identifies the meta file. */
    static const int FD_META = _dataset_fd_base::FD_META;

    /** Identifies the data file. */
    static const int FD_DATA = _dataset_fd_base::FD_DATA;

public:

    /** The number of spatial dimensions used to locate the date. */
    static const unsigned int num_spatial_dims =
        s1o::traits::num_spatial_dims<
            TMetaAdapter
        >::value;

    /** The type used to represent the spatial variables. */
    typedef typename s1o::traits::spatial_value_type<
        TMetaAdapter
    >::type spatial_value_type;

    /** The base metadata type provided by the adapter. */
    typedef typename s1o::traits::metadata_type<
        TMetaAdapter
    >::type metadata_type;

    /** The inner type used to store data in each node of the
        spatial structure. */
    typedef std::pair<metadata_type*, char*> element_pair;

    /** The specialization of the helper type used to interface the spatial
        storage structure with the dataset. */
    typedef typename s1o::traits::spatial_adapter_impl<
        TSpatialAdapter,
        element_pair,
        spatial_value_type,
        num_spatial_dims
        >::type TSpatialAdapterImpl;

    /** The type used by the spatial adapter to store points in space. */
    typedef typename s1o::traits::spatial_point_type<
        TSpatialAdapterImpl
        >::type spatial_point_type;

    /** A pair of spatial points. */
    typedef typename std::pair<
        spatial_point_type,
        spatial_point_type
        > spatial_point_pair;

private:

    /** The extended metadata type carrying information from the file. */
    typedef s1o::file_metadata<
        metadata_type
    > file_metadata;

    /** The type of the spatial structure, tailored for the
        number of dimensions. */
    typedef typename s1o::traits::spatial_storage_type<
        TSpatialAdapterImpl
        >::type spatial_storage;

    /** The iterator used to traverse the spatial storage. */
    typedef typename s1o::traits::spatial_storage_iterator_type<
        TSpatialAdapterImpl
        >::type spatial_storage_iterator;

    /** The iterator used to update the spatial storage. */
    typedef typename s1o::traits::spatial_storage_update_iterator_type<
        TSpatialAdapterImpl
        >::type spatial_storage_update_iterator;

    /** The iterator used to traverse the spatial storage queries. */
    typedef typename s1o::traits::spatial_storage_query_iterator_type<
        TSpatialAdapterImpl
        >::type spatial_storage_query_iterator;

    /** The maximum size allowed for the metadata check (1MB). */
    static const off64_t max_meta_check_size = 1024*1024;

    /** The size of the metadata structure before alignment. */
    static const off64_t meta_szof = sizeof(file_metadata);

    /** The size of the base structure that defines the meta file. */
    static const off64_t meta_base_structure_size = sizeof(meta_base_structure);

    /** The offset, in bytes, check data is located in the meta file. */
    static const off64_t meta_check_offset = meta_base_structure_size;

    /** The representation for unlimited number of elements that can
        be stored in a dataset. */
    static const size_t max_elements_unlimited = static_cast<size_t>(-1);

private:

    const TMetaAdapter _meta_adapter; /** The object used to retrieve
                                          information from the metadata. */

    const TSpatialAdapterImpl _spatial_adapter; /** The object used to
                                                    construct and access
                                                    information from the
                                                    spatial storage. */

    const bool _can_rwp; /** The dataset is open so data can be read,
                             written and pushed directly to the file
                             descriptors instead of using mapped memory. */

    const bool _allow_unsorted; /** The dataset will not enforce that
                                    contents in the data file are
                                    spatially close to each other. */

    const off64_t _file_metadata_size; /** The size of the metadata, after
                                           alignment that will be stored in
                                           the meta file. */

    const _dataset_fd_base _fds; /** File descriptors associated with
                                     the meta and data files and its
                                     methods. */

    const uid_t _max_elements; /** Maximum number of elements that can
                                   be stored in the dataset. */

    const size_t _num_slots; /** The number of data slots in the dataset.
                                 Each slot is a different section of data
                                 in the data file associated with the
                                 elements. */

    const off64_t _slot_size; /** The size of each data slot in the
                                  data file. */


    const off64_t _meta_file_elem_beg_off; /** The starting offset of the
                                               metadata elements in the
                                               meta file. */

    char* const _meta_file_elem_beg_ptr; /** The starting pointer of the
                                             metadata elements in memory. */

    char* const _data_file_elem_beg_ptr; /** The starting pointer of the
                                             metadata elements in memory. */

    spatial_storage _spatial_storage; /** The spatial storage used to
                                          accelerate the spatial queries of
                                          memory mapped elements. */

public:

    /** Allow the helper direct access to the private typedefs. */
    friend class helpers::iter_builder<
        this_type,
        traits::supports_element_pair<
            TSpatialAdapterImpl
            >::value
        >;

    /** Helper for switching iterator transformations based on spatial
        adapter's support for node data. */
    typedef helpers::iter_builder<
        this_type,
        traits::supports_element_pair<
            TSpatialAdapterImpl
            >::value
        > iter_builder;

    /** The uid iterator used to generate sequence of uids for elements in
        the dataset. */
    typedef typename iter_builder::uid_iterator uid_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata objects. */
    typedef typename iter_builder::meta_l_iterator meta_l_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs without slot offset. */
    typedef typename iter_builder::elem_l_iterator elem_l_iterator;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs with slot offset. */
    typedef typename iter_builder::elem_l_iterator_slot elem_l_iterator_slot;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata objects in queries. */
    typedef typename iter_builder::meta_q_iterator meta_q_iterator;

    /** The type of the pair of iterators that convert nodes from the spatial
        storage into metadata objects in queries. */
    typedef std::pair<
        meta_q_iterator,
        meta_q_iterator
        > meta_q_iterator_pair;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs without slot offset in queries. */
    typedef typename iter_builder::elem_q_iterator elem_q_iterator;

    /** The type of the pair of iterators that convert nodes from the spatial
        storage into metadata-data pairs without slot offset in queries. */
    typedef std::pair<
        elem_q_iterator,
        elem_q_iterator
        > elem_q_iterator_pair;

    /** The type of the iterator that converts nodes from the spatial storage
        into metadata-data pairs with slot offset in queries. */
    typedef typename iter_builder::elem_q_iterator_slot elem_q_iterator_slot;

    /** The type of the pair of iterators that convert nodes from the spatial
        storage into metadata-data pairs with slot offset in queries. */
    typedef std::pair<
        elem_q_iterator_slot,
        elem_q_iterator_slot
        > elem_q_iterator_slot_pair;

    /** The type of the iterator contaning initialization data for the spatial
        storage. */
    typedef typename iter_builder::elem_d_iterator elem_d_iterator;

    /** The type of the iterator for updating the metadata-data pairs from the
        spatial storage. */
    typedef typename iter_builder::elem_u_iterator elem_u_iterator;

    /** The transform used to get the location from the metadata in the
        dataset given its uid. */
    typedef transform_ds_get_location<
        this_type
        > location_transform;

    /** The transform used to read the metadata from the metadata file given
        the uid of the element. */
    typedef transform_ds_read_meta<
        this_type
        > read_meta_transform;

    /** The type of the iterator containing the spatial location of the
        elements in the dataset. */
    typedef boost::transform_iterator<
        location_transform,
        uid_iterator,
        typename location_transform::reference,
        typename location_transform::value_type
        > node_location_iterator;

    /** The type of the iterator that reads through the metadata file of the
        dataset */
    typedef boost::transform_iterator<
        read_meta_transform,
        uid_iterator,
        typename read_meta_transform::reference,
        typename read_meta_transform::value_type
        > read_meta_iterator;

private:

    /**
     * @brief Align a size with a 64 byte boundary.
     *
     * @param size The size to be aligned.
     *
     * @return size_t The resulting size aligned with
     * a 64 byte boundary.
     *
     * @note zero is aligned to 64 bytes as well.
     */
    inline size_t align64(
        size_t size
    ) const
    {
        return size == 0 ? 64 : (((size-1) / 64) + 1) * 64;
    }

    /**
     * @brief Get the asserted size, in bytes, of the check data
     * used to validate the dataset.
     *
     * @return size_t The asserted size, in bytes, of the check
     * data used to validate the dataset.
     */
    inline size_t get_meta_check_size() const
    {
        const size_t meta_check_size = _meta_adapter.get_meta_check_size();

        if (meta_check_size > max_meta_check_size) {
            EX3_THROW(check_size_too_big_exception()
                << maximum_size(max_meta_check_size)
                << actual_size(meta_check_size)
                << dataset_name(get_basename()));
        }

        return meta_check_size;
    }

    /**
     * @brief Compute the size of the header in the meta file
     * with the meta check data and memory alignment.
     *
     * @return size_t The size, in bytes, of the header in the meta file
     * with the meta check data after memory alignment.
     */
    inline size_t compute_meta_file_header_size() const
    {
        return align64(get_meta_check_size() +
            meta_base_structure_size);
    }

    /**
     * @brief Compute the size of the metadata file given a
     * fixed number of elements.
     *
     * @param num_elements The number of elements to be stored.
     * @param meta_check_size The size of the check data.
     *
     * @return size_t The size, in bytes, of the metadata file.
     */
    inline size_t compute_meta_file_size(
        size_t num_elements
    ) const
    {
        return (num_elements == 0) ? 0 :
            compute_meta_file_header_size() +
            _file_metadata_size * num_elements;
    }

    /**
     * @brief Compute the size of the metadata file given a
     * sequence of metadata objects.
     *
     * @tparam IT The type of the iterator.
     *
     * @param metabegin The iterator pointing to the beginning
     * of a sequence of metadata objects.
     * @param metaend The iterator pointing to after the last
     * element of a sequence of metadata objects.
     *
     * @return size_t The size, in bytes, of the metadata file.
     */
    template <typename IT>
    inline size_t compute_meta_file_size(
        IT metabegin,
        IT metaend
    ) const
    {
        const size_t num_elements = std::distance(metabegin, metaend);
        return compute_meta_file_size(num_elements);
    }

    /**
     * @brief Compute the size of the data file given a
     * sequence of metadata objects.
     *
     * @tparam IT The type of the iterator.
     *
     * @param metabegin The iterator pointing to the beginning
     * of a sequence of metadata objects.
     * @param metaend The iterator pointing to after the last
     * element of a sequence of metadata objects.
     * @param num_slots The number of data slots in the dataset.
     *
     * @return size_t The size, in bytes, of the data file.
     */
    template <typename IT>
    inline off64_t compute_data_file_size(
        IT metabegin,
        IT metaend,
        size_t num_slots
    ) const
    {
        off64_t size = 0;

        for (IT meta = metabegin; meta != metaend; meta++) {
            size += align64(_meta_adapter.get_data_size(*meta));
        }

        return size * num_slots;
    }

    /**
     * @brief Compute the number of elements that fit
     * inside the meta file.
     *
     * @return uid_t The number of elements that fit
     * inside the meta file.
     */
    inline uid_t compute_num_elements() const
    {
        // Get the size of the metadata region inside the
        // meta file, which is the size of the file excluding
        // the header + check data section
        const off64_t meta_size = _fds.get_file_size(FD_META) -
            compute_meta_file_header_size();

        // Get the number of elements that fit inside that
        // region
        return meta_size / _file_metadata_size;
    }

     /**
     * @brief Compute the slot size in bytes using the number of slots
     * passed to the constructor and the size of the data file.
     *
     * @return off64_t The slot size in bytes.
     *
     * @note This method cannot guarantee that slot_size and
     * num_slots are the ones used to create the dataset, only that
     * their combination result in the current size of the data file.
     */
    inline off64_t compute_slot_size() const
    {
        const off64_t data_size = _fds.get_file_size(FD_DATA);
        const off64_t extra_bytes = data_size % _num_slots;

        if (extra_bytes != 0) {
            EX3_THROW(extra_slot_bytes_exception()
                << size_value(extra_bytes)
                << dataset_name(get_basename()));
        }

        return data_size / _num_slots;
    }

    /**
     * @brief Compute the offset in the data file where the
     * slot begins.
     *
     * @param slot The slot in the data file being accessed.
     *
     * @return size_t The offset in the data file where the
     * slot begins.
     *
     * @note An exception is thrown if the requested slot is
     * greater than the number of slots in the data file.
     */
    inline size_t compute_slot_offset(size_t slot) const
    {
        if (slot >= _num_slots) {
            EX3_THROW(invalid_slot_exception()
                << maximum_slot(_num_slots-1)
                << requested_slot(slot)
                << dataset_name(get_basename()));
        }

        return slot * _slot_size;
    }

    /**
     * @brief Fill the base data with hardware specific information,
     * the magic code used to identify the file, and information
     * about the data actually being stored.
     *
     * @param base_data The structure to fill.
     */
    inline void fill_base_data(
        meta_base_structure& base_data
    ) const
    {
        const unsigned int meta_check_size = static_cast<unsigned int>
            (_meta_adapter.get_meta_check_size());

        const unsigned int file_metadata_size = static_cast<unsigned int>
            (_file_metadata_size);

        base_data.one = 1;
        base_data.uintsz = sizeof(unsigned int);
        base_data.fofsz = sizeof(s1o::foffset_t);
        base_data.checksz = meta_check_size;
        base_data.metasz = file_metadata_size;
        base_data.version = (S1O_VERSION_MAJOR << 16) | S1O_VERSION_MINOR;
        base_data.revision = S1O_VERSION_REVISION;
        base_data.magic[0] = 0x43;
        base_data.magic[1] = 0x42;
        base_data.magic[2] = 0x45;
        base_data.magic[3] = 0x4E;
        base_data.magic[4] = 0x45;
        base_data.magic[5] = 0x53;
        base_data.magic[6] = 0x31;
        base_data.magic[7] = 0x4F;
    }

    /**
     * @brief Fill the meta check data with custom information
     * provided by the meta adapter that allows validation of
     * the dataset when opening.
     *
     * @param p_meta_check A pointer to the check data to fill.
     */
    inline void fill_meta_check_data(
        meta_check_ptr_t p_meta_check
    ) const
    {
        c_meta_check_ptr_t p_meta_check_begin =
            _meta_adapter.get_meta_check_ptr();

        c_meta_check_ptr_t p_meta_check_end = p_meta_check_begin +
            get_meta_check_size();

        std::copy(p_meta_check_begin, p_meta_check_end, p_meta_check);
    }

    /**
     * @brief Validate the base data against the hardware information,
     * the magic number of the file, and the structure of the data
     * actually being stored.
     *
     * @param base_data The header of the meta file.
     */
    inline void assert_base_data(
        const meta_base_structure& base_data
    ) const
    {
        const char* const p_base_data = reinterpret_cast
            <const char*>(&base_data);

        meta_base_structure src_base_data;
        fill_base_data(src_base_data);

        const char* const p_base_data_begin = reinterpret_cast
            <const char*>(&src_base_data);

        const char* const p_base_data_end = p_base_data_begin +
            meta_base_structure_size;

        std::pair<const char*, const char*> comp;

        comp = std::mismatch(p_base_data_begin, p_base_data_end, p_base_data);

        if (comp.first != p_base_data_end) {
            EX3_THROW(base_data_mismatch_exception()
                << expected_value(*comp.first)
                << actual_value(*comp.second)
                << position_value(std::distance(p_base_data_begin, comp.first))
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure the size of the meta file aligns with
     * the header, meta check data and the meta information
     * supposedly stored.
     *
     */
    inline void assert_meta_file_size() const
    {
        // Get the size of the metadata region inside the
        // meta file, which is the size of the file excluding
        // the header + check data section
        const off64_t meta_size = _fds.get_file_size(FD_META) -
            compute_meta_file_header_size();

        // Check if the metadata size is multiple of the metadata size
        const off64_t extra_bytes = meta_size % _file_metadata_size;
        if (extra_bytes != 0) {
            EX3_THROW(extra_meta_bytes_exception()
                << size_value(extra_bytes)
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure the meta check data stored in the meta file
     * matches with the one provided by the meta adapter class.
     *
     * @param p_meta_check The pointer to the meta check data
     * stored in the meta file.
     */
    inline void assert_meta_check_data(
        c_meta_check_ptr_t p_meta_check
    ) const
    {
        c_meta_check_ptr_t p_meta_check_begin =
            _meta_adapter.get_meta_check_ptr();

        c_meta_check_ptr_t p_meta_check_end = p_meta_check_begin +
            get_meta_check_size();

        std::pair<c_meta_check_ptr_t, c_meta_check_ptr_t> comp;

        comp = std::mismatch(p_meta_check_begin, p_meta_check_end, p_meta_check);

        if (comp.first != p_meta_check_end) {
            EX3_THROW(check_data_mismatch_exception()
                << expected_value(*comp.first)
                << actual_value(*comp.second)
                << position_value(std::distance(p_meta_check_begin, comp.first))
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure the dataset is open with read/write/push support.
     *
     */
    inline void assert_can_rwp() const
    {
        if (!_can_rwp) {
            EX3_THROW(mmapped_exception()
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Initialize a recently opened dataset where the files are
     * mapped to memory.
     *
     * @param new_ds True if the dataset if being created rather than
     * just being opened.
     */
    void init_meta_mem(
        bool new_ds
    ) const
    {
        char* const p_meta_addr = _fds.get_mapped(FD_META).addr;

        meta_base_structure* const p_base_data = reinterpret_cast
            <meta_base_structure*>(p_meta_addr);

        meta_check_ptr_t p_meta_check = reinterpret_cast<meta_check_ptr_t>
            (p_meta_addr + meta_check_offset);

        if (new_ds) {
            fill_base_data(*p_base_data);
            fill_meta_check_data(p_meta_check);
        }
        else {
            assert_base_data(*p_base_data);
            assert_meta_check_data(p_meta_check);
            assert_meta_file_size();
        }
    }

    /**
     * @brief Initialize a recently opened dataset where the files are
     * meant to be accessed using conventional read/write operations.
     *
     * @param new_ds True if the dataset if being created rather than
     * just being opened.
     */
    void init_meta_fd(
        bool new_ds
    ) const
    {
        if (new_ds) {


            meta_base_structure base_data;
            fill_base_data(base_data);

            const size_t meta_header_size = align64(
                _fds.write_object(FD_META, base_data) +
                _fds.write(FD_META,
                    _meta_adapter.get_meta_check_ptr(),
                    get_meta_check_size()));

            // Align the meta header size
            if (meta_header_size > 0) {
                const char zero = 0;
                _fds.seek(FD_META, meta_header_size-1, SEEK_SET);
                _fds.write(FD_META, &zero, 1);
            }
        }
        else {

            meta_base_structure base_data;
            _fds.read_object(FD_META, base_data, true);

            std::vector<char> meta_check(get_meta_check_size());

            const size_t sr = _fds.read(FD_META, &meta_check[0],
                meta_check.size(), false, false);

            if (sr != meta_check.size()) {
                EX3_THROW(check_data_mismatch_exception()
                    << expected_size(meta_check.size())
                    << actual_size(sr)
                    << dataset_name(get_basename()));
            }

            assert_base_data(base_data);
            assert_meta_check_data(&meta_check[0]);
            assert_meta_file_size();
        }
    }

    /**
     * @brief Get the byte offset of the element with a given uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return off64_t The byte offset of the element with a given uid.
     *
     * @note This method does not check if the uid is valid so it can cause a
     * segmentation fault if used incorrectly.
     */
    inline off64_t _get_element_offset(
        uid_t uid
    ) const
    {
        return (uid - 1) * _file_metadata_size;
    }

    /**
     * @brief Get the byte offset of the element with a given uid.
     *
     * @param uid The unique identifier of the element.
     * @param offset The resulting byte offset of the element with
     * a given uid.
     *
     * @return true The uid is valid and 'offset' contains the result.
     * @return false The uid is invalid so 'offset' was not modified.
     */
    inline bool try_get_element_offset(
        uid_t uid,
        off64_t& offset
    ) const
    {
        if (uid == 0 || uid > _max_elements) {
            return false;
        }

        offset = _get_element_offset(uid);
        return true;
    }

    /**
     * @brief Get the byte offset of the element with a given uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return off64_t The byte offset of the element with a given uid.
     *
     * @note This method will throw an exception on failure.
     */
    inline off64_t get_element_offset(
        uid_t uid
    ) const
    {
        off64_t offset;

        if (!try_get_element_offset(uid, offset)) {
            EX3_THROW(invalid_uid_exception()
                << maximum_uid(_max_elements)
                << requested_uid(uid)
                << dataset_name(get_basename()));
        }

        return offset;
    }

    /**
     * @brief Get the byte offset of the data from an element's metadata.
     *
     * @param meta The metadata of the element.
     *
     * @return foffset_t The byte offset of the data from the element.
     */
    inline foffset_t get_data_offset(
        const file_metadata* meta
    ) const
    {
        return meta->data_offset;
    }

    /**
     * @brief Get the byte offset in the metadata file of the element
     * with a given uid.
     *
     * @param uid The unique identifier of the element.
     * @param offset The resulting byte offset of the element in the
     * metadata file with a given uid.
     *
     * @return true The uid is valid and 'offset' contains the result.
     * @return false The uid is invalid so 'offset' was not modified.
     */
    inline bool try_get_element_file_offset(
        uid_t uid,
        off64_t& offset
    ) const
    {
        if (!get_element_offset(uid, offset)) {
            return false;
        }

        offset += _meta_file_elem_beg_off;
        return true;
    }

    /**
     * @brief Get the byte offset in the metadata file of the element
     * with a given uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return off64_t The byte offset in the metadata file of the
     * element with a given uid.
     *
     * @note This method will throw an exception on failure.
     */
    inline off64_t get_element_file_offset(
        uid_t uid
    ) const
    {
        return _meta_file_elem_beg_off +
            get_element_offset(uid);
    }

    /**
     * @brief Get the byte offset in the data file of the data from
     * an element's metadata.
     *
     * @param meta The metadata of the element.
     *
     * @return off64_t The byte offset of the data from the element.
     */
    inline off64_t get_data_file_offset(
        const file_metadata* meta
    ) const
    {
        assert_has_data();

        return get_data_offset(meta);
    }

    /**
     * @brief Get the memory mapped address of the element
     * with a given uid.
     *
     * @param uid The unique identifier of the element.
     * @param elem The resulting pointer to the element mapped
     * in memory with a given uid.
     *
     * @return true The uid is valid and 'elem' contains the result.
     * @return false The uid is invalid so 'elem' was not modified.
     */
    inline bool try_get_element_address(
        uid_t uid,
        file_metadata*& elem
    ) const
    {
        off64_t offset;

        if (!try_get_element_offset(uid, offset)) {
            return false;
        }

        elem = reinterpret_cast<file_metadata*>
            (_meta_file_elem_beg_ptr + offset);

        return true;
    }

    /**
     * @brief Get the memory mapped address of the element with a given uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return file_metadata* The memory mapped address of the element with a
     * given uid.
     *
     * @note This method does not check if the uid is valid so it can cause a
     * segmentation fault if used incorrectly.
     */
    inline file_metadata* _get_element_address(
        uid_t uid
    ) const
    {
        return reinterpret_cast<file_metadata*>(
            _meta_file_elem_beg_ptr +
            _get_element_offset(uid));
    }

    /**
     * @brief Get the memory mapped address of the element
     * with a given uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return file_metadata* The memory mapped address of the element
     * with a given uid.
     *
     * @note This method will throw an exception on failure.
     */
    inline file_metadata* get_element_address(
        uid_t uid
    ) const
    {
        return reinterpret_cast<file_metadata*>(
            _meta_file_elem_beg_ptr +
            get_element_offset(uid));
    }

    /**
     * @brief Get the memory mapped address of the data from an element's
     * metadata.
     *
     * @param meta The metadata of the element.
     *
     * @return char* The memory mapped address of the data from an element's
     * metadata.
     *
     * @note This method does not check if the uid is valid so it can cause a
     * segmentation fault if used incorrectly.
     */
    inline char* _get_data_address(
        const file_metadata* meta
    ) const
    {
        assert_has_data();

        return _data_file_elem_beg_ptr +
            get_data_offset(meta);
    }

    /**
     * @brief Get the memory mapped address of the data from
     * an element's metadata.
     *
     * @param meta The metadata of the element.
     *
     * @return char* The memory mapped address of the data from
     * an element's metadata.
     *
     * @note This method will throw an exception if the dataset does not have
     * a data file associated to it.
     */
    inline char* get_data_address(
        const file_metadata* meta
    ) const
    {
        assert_has_data();

        return _data_file_elem_beg_ptr +
            get_data_offset(meta);
    }

    /**
     * @brief Set the value of an element's clean bit.
     *
     * @param uid The unique identifier of the element.
     * @param value The value to store in the clean bit.
     */
    inline void set_element_clean_bit(
        uid_t uid,
        int value
    ) const
    {
        file_metadata* elem = get_element_address(uid);
        elem->clean_bit = value;
    }

    /**
     * @brief Set the value of an element's clean bit.
     *
     * @param meta The base metadata used to reference the element.
     * @param value The value to store in the clean bit.
     */
    inline void set_element_clean_bit(
        metadata_type& meta,
        int value
    ) const
    {
        set_element_clean_bit(_meta_adapter.get_uid(meta), value);
    }

    /**
     * @brief Get the value of an element's clean bit.
     *
     * @param uid The unique identifier of the element.
     *
     * @return int The value of the element's clean bit.
     */
    inline int get_element_clean_bit(
        uid_t uid
    ) const
    {
        const file_metadata* const elem = get_element_address(uid);
        return elem->clean_bit;
    }

    /**
     * @brief Get the value of an element's clean bit.
     *
     * @param meta The base metadata used to reference the element.
     *
     * @return int The value of the element's clean bit.
     */
    inline int get_element_clean_bit(
        metadata_type& meta
    ) const
    {
        return get_element_clean_bit(_meta_adapter.get_uid(meta));
    }

    /**
     * @brief Initialize the spatial storage from data that is
     * already in memory and perform the check to see if the
     * stored data follows the sorting of the storage.
     *
     * @param new_ds Specifies if the dataset is being created.
     */
    void init_spstruct_mem(bool new_ds)
    {
        if (! _spatial_adapter.empty(_spatial_storage)) {
            EX3_THROW(already_initialized_exception()
                << dataset_name(get_basename()));
        }

        uid_iterator uid_begin(1);
        uid_iterator uid_end(_max_elements+1);

        elem_d_iterator elem_begin =
            iter_builder::template construct_out_iterator<
                typename iter_builder::elem_d_iterator,
                typename iter_builder::transform_d_get_elem
                >(uid_begin, this);

        elem_d_iterator elem_end =
            iter_builder::template construct_out_iterator<
                typename iter_builder::elem_d_iterator,
                typename iter_builder::transform_d_get_elem
                >(uid_end, this);

        location_transform loc_transform(this);

        node_location_iterator loc_begin(uid_begin, loc_transform);
        node_location_iterator loc_end(uid_end, loc_transform);

        initialization_data::default_data init_data(get_basename(),
            new_ds, _fds.can_write());

        _spatial_adapter.initialize(_spatial_storage, init_data,
            elem_begin, elem_end, loc_begin, loc_end);

        // Check if the data is sorted

        if (!_allow_unsorted && !_fds.no_data()) {

            elem_u_iterator ssit, ssbegin, ssend;

            ssbegin = begin_update();
            ssend = end_update();

            const char* last_ptr = 0;

            for (ssit = ssbegin; ssit != ssend; ssit++) {

                // Get the pointer to the metadata's data
                const element_pair& data = *ssit;
                const char* data_ptr = data.second;

                if (data_ptr < last_ptr) {
                    EX3_THROW(unsorted_data_exception()
                        << last_pointer(last_ptr)
                        << actual_pointer(data_ptr)
                        << dataset_name(get_basename()));
                }

                last_ptr = data_ptr;
            }
        }
    }

    /**
     * @brief Set the content of an element's metadata
     * from a generic object compatible with the method
     * TMetaAdapter::get_metadata.
     *
     * @tparam T The type of the object containing the
     * new metadata.
     *
     * @param elem A pointer to the destination element.
     * @param ptr The object with the source of the metadata.
     *
     * @note The type T must be compatible with some overload
     * of TMetaAdapter::get_metadata.
     */
    template <typename T>
    inline void set_metadata(
        metadata_type& elem,
        const T& ptr
    ) const
    {
        const metadata_type& src = _meta_adapter.get_metadata(ptr);
        elem = src;
    }

    /**
     * @brief Set the content of an element's metadata
     * from another metadata object.
     *
     * @param elem A pointer to the destination element.
     * @param ptr A pointer to the source element.
     */
    inline void set_metadata(
        metadata_type& elem,
        const metadata_type& src
    ) const
    {
        elem = src;
    }

    /**
     * @brief Fill the metadata of elements mapped
     * to memory from iterators.
     *
     * @tparam IT The type of the iterator.
     *
     * @param metabegin The iterator pointing to the beginning
     * of a sequence of metadata objects.
     * @param metaend The iterator pointing to after the last
     * element of a sequence of metadata objects.
     *
     * @note The type IT must be compatible with the
     * TMetaAdapter type.
     */
    template <typename IT>
    inline void fill_metadata_mem(
        IT metabegin,
        IT metaend
    ) const
    {
        for (IT meta = metabegin; meta != metaend; meta++) {

            // Get the uid of the source metadata
            const s1o::uid_t euid = _meta_adapter.get_uid(*meta);

            // Get the pointer to the metadata object in the dataset
            file_metadata* const elem = get_element_address(euid);

            // Set the base metadata content and leave the data
            // pointer for later

            set_metadata(*elem, *meta);
            elem->data_offset = 0;
            elem->clean_bit = 0;
        }
    }

    /**
     * @brief Set the data offset of each element in the dataset using the
     * ordering imposed by the spatial storage so that are nearby in the
     * storage may be nearby in the file.
     *
     */
    void set_data_offsets_from_storage()
    {
        assert_has_data();

        if ( _spatial_adapter.empty(_spatial_storage)) {
            EX3_THROW(not_initialized_exception()
                    << dataset_name(get_basename()));
        }

        elem_u_iterator ssit, ssbegin, ssend;

        ssbegin = begin_update();
        ssend = end_update();

        foffset_t current_offset = 0;

        for (ssit = ssbegin; ssit != ssend; ssit++) {

            // Get the pointer to the metadata and its data pointer

            typename std::iterator_traits<
                elem_u_iterator
                >::reference data = *ssit;

            file_metadata* const elem = reinterpret_cast
                <file_metadata*>(data.first);

            char*& data_ptr = data.second;

            // Set the data pointer at the structure and spatial storage

            elem->data_offset = current_offset;
            elem->clean_bit = file_metadata::CLEAN_BIT_MAGIC;
            data_ptr = get_data_address(elem);

            // Update the current data offset including data alignment

            current_offset += align64(_meta_adapter.get_data_size(*elem));
        }
    }

    /**
     * @brief Create a sequence of pairs data_offset-data_size for
     * each element in the dataset that is mapped to memory.
     *
     * @tparam IT The type of the output iterator.
     *
     * @param out The output iterator.
     */
    template <typename IT>
    void get_data_info_mem(
        IT out
    ) const
    {
        for (uid_t euid = 1; euid <= _max_elements; euid++, out++) {

            // Get the pointer to the metadata object in the dataset
            const file_metadata* const elem = get_element_address(euid);

            *out = std::make_pair(
                elem->data_offset,
                _meta_adapter.get_data_size(*elem)
            );
        }
    }

    /**
     * @brief Create a sequence of pairs data_offset-data_size for
     * each element in the dataset that is stored in the meta file.
     *
     * @tparam IT The type of the output iterator.
     *
     * @param out The output iterator.
     */
    template <typename IT>
    void get_data_info_fd(
        IT out
    ) const
    {
        const uid_t num_elements = compute_num_elements();

        for (uid_t euid = 1; euid <= num_elements; euid++, out++) {

            // Read the entire file_metadata from the file
            file_metadata elem;
            if (!read_metadata(euid, elem)) {
                EX3_THROW(inconsistent_meta_exception()
                    << expected_num_elements(num_elements)
                    << requested_uid(euid)
                    << dataset_name(get_basename()));
            }

            *out = std::make_pair(
                elem.data_offset,
                _meta_adapter.get_data_size(elem)
            );
        }
    }

    /**
     * @brief Ensure that the dataset has location data.
     *
     */
    inline void assert_has_location_data() const
    {
        // RWP means no memory mapping, which means
        // no spatial structure, so no location data

        if (_can_rwp || _spatial_adapter.empty(_spatial_storage)) {
            EX3_THROW(location_data_unavailable_exception()
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure that the dataset has mapped pointers
     * for the metadata and data files.
     *
     */
    inline void assert_is_mmapped() const
    {
        // RWP means no memory mapping, which means
        // no spatial structure, so no location data

        if (_can_rwp) {
            EX3_THROW(not_mmapped_exception()
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure that the dataset has a valid data file.
     *
     */
    inline void assert_has_data() const
    {
        if (_fds.no_data()) {
            EX3_THROW(no_data_exception()
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure the uid has already been written
     * to the meta file.
     *
     * @param uid The unique identifier of the element.
     */
    inline void assert_uid_in_file(
        uid_t uid
    ) const
    {
        const uid_t num_elements = compute_num_elements();
        if (uid > num_elements) {
            EX3_THROW(invalid_uid_exception()
                << maximum_uid(num_elements)
                << requested_uid(uid)
                << dataset_name(get_basename()));
        }
    }

    /**
     * @brief Ensure the filenames for extra files used by the spatial adapter
     * are not equal among themselves and equal to the data and metadata
     * files.
     *
     */
    inline void assert_filenames() const
    {
        std::vector<std::string> files;

        files.push_back(_fds.get_meta_file_path());
        files.push_back(_fds.get_data_file_path());

        _spatial_adapter.get_extra_files(get_basename(),
            std::back_inserter(files));

        std::sort(files.begin(), files.end());

        for (size_t i = 1; i < files.size(); i++) {

            if (files[i-1].compare(files[i]) == 0) {
                EX3_THROW(extensions_equal_exception()
                    << file_extension(files[i-1]));
            }
        }
    }

    /**
     * @brief Read an existing metadata object from the metadata file in disk
     * into a compatible object.
     *
     * @tparam T The type of the metadata being read.
     *
     * @param uid The uid of the metadata element being read.
     * @param meta The resulting metadata object read from the metadata file,
     * if successfull.
     *
     * @return true The uid is valid and the metadata was successfully read.
     * @return false The uid is invalid so the metadata was not read.
     */
    template <typename T>
    inline bool read_metadata(uid_t uid, T& meta) const
    {
        assert_can_rwp();

        const off64_t off = get_element_file_offset(uid);
        _fds.seek(FD_META, off, SEEK_SET);
        const size_t read = _fds.read_object(FD_META, meta, false);

        if (read > 0) {

            const uid_t file_uid = _meta_adapter.get_uid(meta);

            if (file_uid != uid) {
                EX3_THROW(inconsistent_meta_exception()
                    << requested_uid(uid)
                    << read_uid(file_uid)
                    << dataset_name(get_basename()));
            }
        }

        return read > 0;
    }

    /**
     * @brief Get the iterator to the beginning of modifiable metadata-data
     * pairs according to the ordering imposed by the spatial storage.
     *
     * @return elem_u_iterator The iterator to the beginning of modifiable
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage.
     */
    inline elem_u_iterator begin_update()
    {
        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_u_iterator,
            typename iter_builder::transform_u_get_elem
            >(_spatial_adapter.sbegin_update(_spatial_storage), this);
    }

    /**
     * @brief Get the iterator to the end of modifiable metadata-data pairs
     * according to the ordering imposed by the spatial storage.
     *
     * @return elem_u_iterator The iterator to the end of modifiable
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage.
     */
    inline elem_u_iterator end_update()
    {
        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_u_iterator,
            typename iter_builder::transform_u_get_elem
            >(_spatial_adapter.send_update(_spatial_storage), this);
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
    void _query_elements(
        const Predicates& predicates,
        elem_q_iterator& begin,
        elem_q_iterator& end
    ) const
    {
        assert_has_location_data();
        assert_has_data();

        spatial_storage_query_iterator qbegin, qend;
        _spatial_adapter.query(_spatial_storage, predicates, qbegin, qend);

        begin = iter_builder::template construct_in_iterator<
            typename iter_builder::elem_q_iterator,
            typename iter_builder::transform_q_get_elem
            >(qbegin, this);

        end = iter_builder::template construct_in_iterator<
            typename iter_builder::elem_q_iterator,
            typename iter_builder::transform_q_get_elem
            >(qend, this);
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
    inline void _query_elements(
        const Predicates& predicates,
        size_t slot,
        elem_q_iterator_slot& begin,
        elem_q_iterator_slot& end
    ) const
    {
        assert_has_location_data();
        assert_has_data();

        const size_t slot_off = compute_slot_offset(slot);

        spatial_storage_query_iterator qbegin, qend;
        _spatial_adapter.query(_spatial_storage, predicates, qbegin, qend);

        begin = iter_builder::template construct_in_iterator<
            typename iter_builder::elem_q_iterator_slot,
            typename iter_builder::transform_q_get_elem_slot
            >(qbegin, this, slot_off);

        end = iter_builder::template construct_in_iterator<
            typename iter_builder::elem_q_iterator_slot,
            typename iter_builder::transform_q_get_elem_slot
            >(qend, this, slot_off);
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
    void _begin_query_metadata(
        const Predicates& predicates,
        meta_q_iterator& begin,
        meta_q_iterator& end
    ) const
    {
        assert_has_location_data();

        spatial_storage_query_iterator qbegin, qend;
        _spatial_adapter.query(_spatial_storage, predicates, qbegin, qend);

        begin = iter_builder::template construct_in_iterator<
            typename iter_builder::meta_q_iterator,
            typename iter_builder::transform_q_get_meta
            >(qbegin, this);

        end = iter_builder::template construct_in_iterator<
            typename iter_builder::meta_q_iterator,
            typename iter_builder::transform_q_get_meta
            >(qend, this);
    }

public:

    /**
     * @brief Construct a new dataset object.
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
    dataset(
        const std::string& basepath,
        int mode,
        int flags,
        size_t num_slots,
        const TMetaAdapter& meta_adapter=TMetaAdapter(),
        const TSpatialAdapterImpl& spatial_adapter=TSpatialAdapterImpl()
    ) :
        _meta_adapter(meta_adapter),
        _spatial_adapter(spatial_adapter),
        _can_rwp(
            ((flags & S1O_FLAGS_RWP) != 0) ||
            ((mode & S1O_OPEN_TRUNC) != 0)),
        _allow_unsorted(
            ((flags & S1O_FLAGS_ALLOW_UNSORTED) != 0) ||
            ((flags & S1O_FLAGS_NO_DATA) != 0)),
        _file_metadata_size(align64(meta_szof)),
        _fds(
            basepath,
            meta_adapter.get_meta_file_ext(),
            meta_adapter.get_data_file_ext(),
            mode,
            (flags & S1O_FLAGS_NO_DATA) != 0,
            !_can_rwp,
            0,
            0),
        _max_elements(_can_rwp ?
            max_elements_unlimited :
            compute_num_elements()),
        _num_slots(num_slots),
        _slot_size(
            (flags & S1O_FLAGS_NO_DATA) != 0 ? 0 :
                compute_slot_size()),
        _meta_file_elem_beg_off(compute_meta_file_header_size()),
        _meta_file_elem_beg_ptr(
            _can_rwp ? 0 :
                _fds.get_mapped(FD_META).addr +
                compute_meta_file_header_size()),
        _data_file_elem_beg_ptr(
            _can_rwp ? 0 :
                _fds.get_mapped(FD_DATA).addr),
        _spatial_storage()
    {
        assert_filenames();

        const bool new_ds = (mode & S1O_OPEN_TRUNC) != 0;
        const bool data_check =
            ((flags & S1O_FLAGS_NO_DATA_CHECK) == 0) &&
            ((flags & S1O_FLAGS_NO_DATA) == 0);

        if (_can_rwp && !_allow_unsorted) {
            EX3_THROW(unsorted_data_exception()
                << dataset_name(get_basename()));
        }

        if (_fds.no_data()) {
            if (_num_slots != 0) {
                EX3_THROW(invalid_num_slots_exception()
                    << expected_num_slots(0)
                    << requested_num_slots(_num_slots)
                    << dataset_name(get_basename()));
            }
        }
        else {
            if (new_ds && _num_slots != 1) {
                EX3_THROW(invalid_num_slots_exception()
                    << expected_num_slots(1)
                    << requested_num_slots(_num_slots)
                    << dataset_name(get_basename()));
            }
        }

        if (_can_rwp) {
            init_meta_fd(new_ds);
        }
        else {
            init_meta_mem(new_ds);
            init_spstruct_mem(new_ds);
        }

        // Check for data inconsistencies

        if (data_check && !new_ds && (_max_elements > 0)) {

            // Store the offset and size of data for each element
            std::vector<std::pair<foffset_t, size_t> > data_info;

            if (_can_rwp) {
                get_data_info_fd(std::back_inserter(data_info));
            }
            else {
                get_data_info_mem(std::back_inserter(data_info));
            }

            const uid_t num_elements = _can_rwp ?
                compute_num_elements() : _max_elements;

            // Sort the data_info by offset, it must be
            // unique because of the alignment
            std::sort(data_info.begin(), data_info.end());

            if (data_info.size() != num_elements) {
                EX3_THROW(inconsistent_meta_exception()
                    << expected_num_elements(num_elements)
                    << actual_num_elements(data_info.size())
                    << dataset_name(get_basename()));
            }

            const size_t real_size = _fds.get_file_size(FD_DATA);
            off64_t current_offset = 0;

            for (size_t i = 0; i < data_info.size(); i++) {

                // The next element's data offset must
                // match the current offset
                if (current_offset != data_info[i].first) {
                    EX3_THROW(inconsistent_data_exception()
                        << expected_offset(current_offset)
                        << actual_offset(data_info[i].first)
                        << dataset_name(get_basename()));
                }

                // Increment the data offset considering alignment
                current_offset += align64(data_info[i].second);
            }

            if (current_offset != _slot_size) {
                EX3_THROW(inconsistent_data_exception()
                    << expected_slot_size(_slot_size)
                    << actual_slot_size(current_offset)
                    << dataset_name(get_basename()));
            }

            if (current_offset * _num_slots != real_size) {
                EX3_THROW(inconsistent_data_exception()
                    << expected_size(real_size)
                    << actual_size(current_offset * _num_slots)
                    << actual_offset(current_offset)
                    << actual_num_slots(_num_slots)
                    << dataset_name(get_basename()));
            }
        }
    }

    /**
     * @brief Construct a new dataset object from a sequence of
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
    dataset(
        const std::string& basepath,
        int flags,
        size_t num_slots,
        IT metabegin,
        IT metaend,
        const TMetaAdapter& meta_adapter=TMetaAdapter(),
        const TSpatialAdapterImpl& spatial_adapter=TSpatialAdapterImpl()
    ) :
        _meta_adapter(meta_adapter),
        _spatial_adapter(spatial_adapter),
        _can_rwp(false),
        _allow_unsorted((flags & S1O_FLAGS_NO_DATA) != 0),
        _file_metadata_size(align64(meta_szof)),
        _fds(
            basepath,
            meta_adapter.get_meta_file_ext(),
            meta_adapter.get_data_file_ext(),
            S1O_OPEN_NEW,
            (flags & S1O_FLAGS_NO_DATA) != 0,
            !_can_rwp,
            compute_meta_file_size(metabegin, metaend),
            compute_data_file_size(metabegin, metaend, num_slots)),
        _max_elements(compute_num_elements()),
        _num_slots(num_slots),
        _slot_size(
            (flags & S1O_FLAGS_NO_DATA) != 0 ? 0 :
                compute_slot_size()),
        _meta_file_elem_beg_off(compute_meta_file_header_size()),
        _meta_file_elem_beg_ptr(
            _can_rwp ? 0 :
                _fds.get_mapped(FD_META).addr +
                compute_meta_file_header_size()),
        _data_file_elem_beg_ptr(
            _can_rwp ? 0 :
                _fds.get_mapped(FD_DATA).addr),
        _spatial_storage()
    {
        assert_filenames();

        if (!_fds.no_data() && _num_slots == 0) {
            EX3_THROW(invalid_num_slots_exception()
                << minimum_num_slots(1)
                << requested_num_slots(_num_slots)
                << dataset_name(get_basename()));
        }

        init_meta_mem(true);
        fill_metadata_mem(metabegin, metaend);
        init_spstruct_mem(true); // We are creating a new dataset
        if (!_fds.no_data())
            set_data_offsets_from_storage();
    }

    /**
     * @brief Destroy the class and, if necessary, destroy the spatial storage
     * if needed.
     *
     */
    virtual ~dataset()
    {
        _spatial_adapter.destroy(_spatial_storage);
    }

    /**
     * @brief Remove the meta and data files of a dataset.
     *
     * @param basename The file name + path of the dataset
     * without the trailing .meta and .data extensions.
     * @param meta_adapter The object used to retrieve information
     * from the metadata.
     * @param spatial_adapter The object used to construct and access
     * information from the spatial storage.
     *
     * @note This method will not fail if the files do not exist.
     */
    inline static void unlink(
        const std::string& basename,
        const TMetaAdapter& meta_adapter=TMetaAdapter(),
        const TSpatialAdapterImpl& spatial_adapter=TSpatialAdapterImpl()
    )
    {
        std::vector<std::string> files;

        // Push the two core files of the dataset

        files.push_back(_dataset_fd_base::get_file_path(basename,
            meta_adapter.get_meta_file_ext()));

        files.push_back(_dataset_fd_base::get_file_path(basename,
            meta_adapter.get_data_file_ext()));

        // Push any extra file required by the spatial storage
        spatial_adapter.get_extra_files(basename, std::back_inserter(files));

        for (size_t i = 0; i < files.size(); i++) {
            try {
                unlink_checked(files[i].c_str());
            }
            catch(const io_exception& e) {

                const int* const p_errno = boost::
                    get_error_info<errno_value>(e);

                // Rethrow anything that is not a "file not found"
                if ((p_errno == 0) || (*p_errno != ENOENT))
                    throw;
            }
        }
    }

    /**
     * @brief Get the basename used to open/create the dataset.
     *
     * @return const std::string The basename used to
     * open/create the dataset.
     */
    inline const std::string get_basename() const
    {
        return _fds.get_basename();
    }

    /**
     * @brief Get a reference to the meta adapter used by the dataset.
     *
     * @return const TMetaAdapter& A reference to the meta adapter used by the
     * dataset.
     */
    inline const TMetaAdapter& get_meta_adapter() const
    {
        return _meta_adapter;
    }

    /**
     * @brief Get a reference to the spatial adapter used by the dataset.
     *
     * @return const TSpatialAdapterImpl& A reference to the spatial adapter
     * used by the dataset.
     */
    inline const TSpatialAdapterImpl& get_spatial_adapter() const
    {
        return _spatial_adapter;
    }

    /**
     * @brief Get the maximum number of elements that can fit in the dataset.
     *
     * @return uid_t The maximum number of elements that can fit in the
     * dataset.
     *
     * @note For datasets mapped to memory, this value is equivalente to the
     * number of elements stored.
     *
     * @note For dataset open with the RWP flag, this value is value is the
     * largest positive value that can be represented by the return type.
     */
    inline uid_t get_max_elements() const
    {
        return _max_elements;
    }

    /**
     * @brief Retrieve a constant reference to the spatial storage in the
     * dataset.
     *
     * @return const spatial_storage& The spatial storage in the dataset.
     *
     * @note The spatial storage must not be modified or it may cause the
     * dataset to become inconsistent.
     */
    inline const spatial_storage& get_spatial_storage() const
    {
        return _spatial_storage;
    }

    /**
     * @brief Get the memory mapped metadata and data from
     * an element giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param meta The resulting metadata of the element.
     * @param data The resulting data of the element.
     *
     * @return true The uid is valid so 'meta' and 'data'
     * contain the result.
     * @return false The uid is invalid so 'meta' and 'data'
     * were not modified.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * dataset is open with the NO_DATA flag.
     */
    inline bool try_get_element(
        uid_t uid,
        metadata_type*& meta,
        char*& data
    ) const
    {
        assert_is_mmapped();
        assert_has_data();

        if (!try_get_element_address(uid, meta)) {
            return false;
        }

        data = get_data_address(meta);
        return true;
    }

    /**
     * @brief Get the memory mapped metadata from an element
     * giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param meta The resulting metadata of the element.
     *
     * @return true The uid is valid so 'meta' contains
     * the result.
     * @return false The uid is invalid so 'meta' was
     * not modified.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     */
    inline bool try_get_element(
        uid_t uid,
        metadata_type*& meta
    ) const
    {
        assert_is_mmapped();
        return try_get_element_address(uid, meta);
    }

    /**
     * @brief Get the memory mapped metadata and data from
     * an element giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param meta The resulting metadata of the element.
     * @param data The resulting data of the element.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * uid is invalid or out of range.
     *
     * @note This method will throw an exception if the
     * dataset is open with the NO_DATA flag.
     */
    inline void get_element(
        uid_t uid,
        metadata_type*& meta,
        char*& data
    ) const
    {
        assert_is_mmapped();
        assert_has_data();

        file_metadata* fmeta = get_element_address(uid);
        meta = fmeta;
        data = get_data_address(fmeta);
    }

    /**
     * @brief Get the memory mapped metadata and data from
     * an element giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param slot The slot in the data file being accessed.
     * @param meta The resulting metadata of the element.
     * @param data The resulting data of the element.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * uid is invalid or out of range.
     *
     * @note This method will throw an exception if the
     * dataset is open with the NO_DATA flag.
     */
    inline void get_element(
        uid_t uid,
        size_t slot,
        metadata_type*& meta,
        char*& data
    ) const
    {
        assert_is_mmapped();
        assert_has_data();

        file_metadata* const fmeta = get_element_address(uid);
        meta = fmeta;
        data = get_data_address(fmeta) + compute_slot_offset(slot);
    }

    /**
     * @brief Get the memory mapped metadata from an element
     * giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param meta The resulting metadata of the element.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * uid is invalid or out of range.
     */
    inline void get_element(
        uid_t uid,
        metadata_type*& meta
    ) const
    {
        assert_is_mmapped();
        meta = get_element_address(uid);
    }

    /**
     * @brief Get the memory mapped metadata from an element
     * giving its uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return metadata_type& The resulting metadata of the element.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * uid is invalid or out of range.
     */
    inline metadata_type& get_metadata(
        uid_t uid
    ) const
    {
        assert_is_mmapped();
        return *get_element_address(uid);
    }

    /**
     * @brief Get the memory mapped metadata and data from
     * an element giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param meta The resulting metadata of the element.
     * @param data The resulting data of the element.
     *
     * @note The dataset must be mapped to memory,
     * otherwise an exception is thrown.
     *
     * @note This method will throw an exception if the
     * uid is invalid or out of range.
     *
     * @note This method will throw an exception if the
     * dataset is open with the NO_DATA flag.
     */

    /**
     * @brief Get the memory mapped data from an element
     * giving its uid.
     *
     * @param uid The unique identifier of the element.
     *
     * @return char* The resulting pointer to the data of
     * the element.
     */
    inline char* get_data(
        uid_t uid
    ) const
    {
        assert_is_mmapped();
        assert_has_data();

        file_metadata* const fmeta = get_element_address(uid);
        return get_data_address(fmeta);
    }

    /**
     * @brief Get the memory mapped data from an element
     * giving its uid.
     *
     * @param uid The unique identifier of the element.
     * @param slot The slot in the data file being accessed.
     *
     * @return char* The resulting pointer to the data of
     * the element.
     */
    inline char* get_data(
        uid_t uid,
        size_t slot
    ) const
    {
        assert_is_mmapped();
        assert_has_data();

        file_metadata* const fmeta = get_element_address(uid);
        return get_data_address(fmeta) + compute_slot_offset(slot);
    }

    /**
     * @brief Ensure the metadadata has been written to the disk.
     *
     */
    inline void sync_metadata() const
    {
        _fds.sync(FD_META);
    }

    /**
     * @brief Ensure the data has been written to the disk.
     *
     * @note The dataset must have been opened without the no_data
     * flag othersise an exception is thrown.
     */
    inline void sync_data() const
    {
        assert_has_data();
        _fds.sync(FD_DATA);
    }

    /**
     * @brief Set the element's clean bit as clean.
     *
     * @param uid The unique identifier of the element.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline void set_element_clean(
        uid_t uid
    ) const
    {
        set_element_clean_bit(uid, file_metadata::CLEAN_BIT_MAGIC);
    }

    /**
     * @brief Set the element's clean bit as clean.
     *
     * @param meta The base metadata used to reference the element.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline void set_element_clean(
        metadata_type*& meta
    ) const
    {
        set_element_clean_bit(meta, file_metadata::CLEAN_BIT_MAGIC);
    }

    /**
     * @brief Set the element's clean bit as dirty.
     *
     * @param uid The unique identifier of the element.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline void set_element_dirty(
        uid_t uid
    ) const
    {
        set_element_clean_bit(uid, file_metadata::DIRTY_BIT_MAGIC);
    }

    /**
     * @brief Set the element's clean bit as dirty.
     *
     * @param meta The base metadata used to reference the element.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline void set_element_dirty(
        metadata_type*& meta
    ) const
    {
        set_element_clean_bit(meta, file_metadata::DIRTY_BIT_MAGIC);
    }

    /**
     * @brief Check if the element stored in the dataset is clean.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is clean.
     * @return false The element stored in the dataset is not clean.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_clean(
        uid_t uid
    ) const
    {
        return get_element_clean_bit(uid) == file_metadata::CLEAN_BIT_MAGIC;
    }

    /**
     * @brief Check if the element stored in the dataset is clean.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is clean.
     * @return false The element stored in the dataset is not clean.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_clean(
        metadata_type*& meta
    ) const
    {
        return get_element_clean_bit(meta) == file_metadata::CLEAN_BIT_MAGIC;
    }

    /**
     * @brief Check if the element stored in the dataset is dirty.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is dirty.
     * @return false The element stored in the dataset is not dirty.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_dirty(
        uid_t uid
    ) const
    {
        return get_element_clean_bit(uid) == file_metadata::DIRTY_BIT_MAGIC;
    }

    /**
     * @brief Check if the element stored in the dataset is dirty.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is dirty.
     * @return false The element stored in the dataset is not dirty.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_dirty(
        metadata_type*& meta
    ) const
    {
        return get_element_clean_bit(meta) == file_metadata::DIRTY_BIT_MAGIC;
    }

    /**
     * @brief Check if the element stored in the dataset is corrupt.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is corrupt.
     * @return false The element stored in the dataset is not corrupt.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_corrupt(
        uid_t uid
    ) const
    {
        const int clean_bit = get_element_clean_bit(uid);
        return (clean_bit != file_metadata::CLEAN_BIT_MAGIC) &&
               (clean_bit != file_metadata::DIRTY_BIT_MAGIC);
    }

    /**
     * @brief Check if the element stored in the dataset is corrupt.
     *
     * @param uid The unique identifier of the element.
     *
     * @return true The element stored in the dataset is corrupt.
     * @return false The element stored in the dataset is not corrupt.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    inline bool is_element_corrupt(
        metadata_type*& meta
    ) const
    {
        const int clean_bit = get_element_clean_bit(meta);
        return (clean_bit != file_metadata::CLEAN_BIT_MAGIC) &&
               (clean_bit != file_metadata::DIRTY_BIT_MAGIC);
    }

    /**
     * @brief Get the list of clean uids in the dataset.
     *
     * @tparam IT The type of the output iterator.
     *
     * @param out The output iterator.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    template <typename IT>
    void get_clean_uids(
        IT out
    ) const
    {
        for (uid_t euid = 1; euid <= _max_elements; euid++) {

            if (is_element_clean(euid)) {
                *out = euid;
                out++;
            }
        }
    }

    /**
     * @brief Get the list of dirty uids in the dataset.
     *
     * @tparam IT The type of the output iterator.
     *
     * @param out The output iterator.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    template <typename IT>
    void get_dirty_uids(
        IT out
    ) const
    {
        for (uid_t euid = 1; euid <= _max_elements; euid++) {

            if (is_element_dirty(euid)) {
                *out = euid;
                out++;
            }
        }
    }

    /**
     * @brief Get the list of corrupt uids in the dataset.
     *
     * @tparam IT The type of the output iterator.
     *
     * @param out The output iterator.
     *
     * @note WARNING: This feature is experimental and may not
     * reliably reflect the actual state of the dataset.
     */
    template <typename IT>
    void get_corrupt_uids(
        IT out
    ) const
    {
        for (uid_t euid = 1; euid <= _max_elements; euid++) {

            if (is_element_corrupt(euid)) {
                *out = euid;
                out++;
            }
        }
    }

    /**
     * @brief Get the iterator to the beginning of the metadata stored on
     * disk.
     *
     * @return read_meta_iterator Get the iterator to the beginning of the
     * metadata stored on disk.
     *
     * @note This method will throw an exception if the dataset is not open
     * with the RWP flag.
     */
    inline read_meta_iterator begin_read_metadata() const
    {
        assert_can_rwp();

        return read_meta_iterator(uid_iterator(1),
            read_meta_transform(this));
    }

    /**
     * @brief Get the iterator to the end of the of the metadata stored on
     * disk.
     *
     * @return read_meta_iterator The iterator to the end of the metadata
     * stored on disk.
     *
     * @note This method will throw an exception if the dataset is not open
     * with the RWP flag.
     */
    inline read_meta_iterator end_read_metadata() const
    {
        assert_can_rwp();

        return read_meta_iterator(uid_iterator(compute_num_elements() + 1),
            read_meta_transform(this));
    }

    /**
     * @brief Get the iterator to the beginning of the metadata-data pairs
     * according to the ordering imposed by the spatial storage, at data
     * slot 0.
     *
     * @return elem_l_iterator The iterator to the beginning of the
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open
     * with the NO_DATA flag.
     */
    inline elem_l_iterator begin_elements() const
    {
        assert_has_location_data();
        assert_has_data();

        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_l_iterator,
            typename iter_builder::transform_l_get_elem
            >(_spatial_adapter.sbegin(_spatial_storage), this);
    }

    /**
     * @brief Get the iterator to the end of the metadata-data pairs according
     * to the ordering imposed by the spatial storage, at data slot 0.
     *
     * @return elem_l_iterator The iterator to the end of the
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open
     * with the NO_DATA flag.
     */
    inline elem_l_iterator end_elements() const
    {
        assert_has_location_data();
        assert_has_data();

        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_l_iterator,
            typename iter_builder::transform_l_get_elem
            >(_spatial_adapter.send(_spatial_storage), this);
    }

    /**
     * @brief Get the iterator to the beginning of the metadata-data pairs
     * according to the ordering imposed by the spatial storage, with data
     * slot selection.
     *
     * @param slot The slot of data to iterate over.
     *
     * @return elem_l_iterator_slot The iterator to the beginning of the
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open
     * with the NO_DATA flag.
     */
    inline elem_l_iterator_slot begin_elements(
        size_t slot
    ) const
    {
        assert_has_location_data();
        assert_has_data();

        const size_t slot_off = compute_slot_offset(slot);

        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_l_iterator_slot,
            typename iter_builder::transform_l_get_elem_slot
            >(_spatial_adapter.sbegin(_spatial_storage), this, slot_off);
    }

    /**
     * @brief Get the iterator to the end of the metadata-data pairs according
     * to the ordering imposed by the spatial storage, with data slot
     * selection.
     *
     * @param slot The slot of data to iterate over.
     *
     * @return elem_l_iterator_slot The iterator to the end of the
     * metadata-data pairs according to the ordering imposed by the spatial
     * storage, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open
     * with the NO_DATA flag.
     */
    inline elem_l_iterator_slot end_elements(
        size_t slot
    ) const
    {
        assert_has_location_data();
        assert_has_data();

        const size_t slot_off = compute_slot_offset(slot);

        return iter_builder::template construct_in_iterator<
            typename iter_builder::elem_l_iterator_slot,
            typename iter_builder::transform_l_get_elem_slot
            >(_spatial_adapter.send(_spatial_storage), this, slot_off);
    }

    /**
     * @brief Get the iterator to the beginning of the metadata objects
     * according to the ordering imposed by the spatial storage.
     *
     * @return meta_l_iterator The iterator to the beginning of
     * the metadata objects according to the ordering imposed by
     * the spatial storage.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     */
    inline meta_l_iterator begin_metadata() const
    {
        assert_has_location_data();

        return iter_builder::template construct_in_iterator<
            typename iter_builder::meta_l_iterator,
            typename iter_builder::transform_l_get_meta
            >(_spatial_adapter.sbegin(_spatial_storage), this);
    }

    /**
     * @brief Get the iterator to the end of the metadata objects
     * according to the ordering imposed by the spatial storage.
     *
     * @return meta_l_iterator The iterator to the end of
     * the metadata objects according to the ordering imposed by
     * the spatial storage.
     *
     * @note This method will throw an exception if the dataset is open
     * with the RWP flag.
     */
    inline meta_l_iterator end_metadata() const
    {
        assert_has_location_data();

        return iter_builder::template construct_in_iterator<
            typename iter_builder::meta_l_iterator,
            typename iter_builder::transform_l_get_meta
            >(_spatial_adapter.send(_spatial_storage), this);
    }

    /**
     * @brief Get the boundaries of the data stored in the spatial storage.
     *
     * @param minpoint The smallest coordinates in the spatial storage.
     * @param maxpoint The largest coordinates in the spatial storage.
     */
    void bounds(
        spatial_point_type& minpoint,
        spatial_point_type& maxpoint
    ) const
    {
        _spatial_adapter.bounds(_spatial_storage, minpoint, maxpoint);
    }

    /**
     * @brief Get the boundaries of the data stored in the spatial storage.
     *
     * @return spatial_point_pair A pair of spatial points where the first
     * containts the smallest coordinates in the spatial storage and the
     * second contains the largest coordinates in the spatial storage.
     */
    spatial_point_pair bounds() const
    {
        spatial_point_pair b;
        bounds(b.first, b.second);
        return b;
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, at data
     * slot 0.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     * @param begin The iterator to the beginning of the metadata-data pairs
     * inside a hypercube formed by the specified points, at data slot 0.
     * @param end The iterator to the end of the metadata-data pairs inside a
     * hypercube formed by the specified points, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    void query_elements(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint,
        elem_q_iterator& begin,
        elem_q_iterator& end
    ) const
    {
        _query_elements(queries::closed_interval<
            spatial_point_type
            >(minpoint, maxpoint), begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, at data
     * slot 0.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     *
     * @return elem_q_iterator_pair A pair of iterators pointing to the
     * beginning and end of the metadata-data pairs inside a hypercube formed
     * by the specified points, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    elem_q_iterator_pair query_elements(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint
    ) const
    {
        elem_q_iterator_pair iterators;
        query_elements(minpoint, maxpoint, iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, with data slot
     * selection.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     * @param slot The slot of data to iterate over.
     * @param begin The iterator to the beginning of the metadata-data pairs
     * inside a hypercube formed by the specified points, with data slot
     * selection.
     * @param end The iterator to the end of the metadata-data pairs inside a
     * hypercube formed by the specified points, with data slot selection.
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
    void query_elements(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint,
        size_t slot,
        elem_q_iterator_slot& begin,
        elem_q_iterator_slot& end
    ) const
    {
        _query_elements(queries::closed_interval<
            spatial_point_type
            >(minpoint, maxpoint), slot, begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata-data
     * pairs inside a hypercube formed by the specified points, with data slot
     * selection.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     * @param slot The slot of data to iterate over.
     *
     * @return elem_q_iterator_slot_pair A pair of iterators pointing to the
     * beginning and end of the metadata-data pairs inside a hypercube formed
     * by the specified points, with data slot selection.
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
    elem_q_iterator_slot_pair query_elements(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint,
        size_t slot
    ) const
    {
        elem_q_iterator_slot_pair iterators;
        query_elements(minpoint, maxpoint, slot,
            iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata
     * objects inside a hypercube formed by the specified points, with data
     * slot selection.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     * @param begin The iterator to the beginning of the metadata objects
     * inside a hypercube formed by the specified points, with data slot
     * selection.
     * @param end The iterator to the end of the metadata objects inside a
     * hypercube formed by the specified points, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note minpoint must contain values smaller than maxpoint, otherwise
     * there is no guarantee the query will succeed.
     */
    void query_metadata(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint,
        meta_q_iterator& begin,
        meta_q_iterator& end
    ) const
    {
        _begin_query_metadata(queries::closed_interval<
            spatial_point_type
            >(minpoint, maxpoint), begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of the metadata
     * objects inside a hypercube formed by the specified points, with data
     * slot selection.
     *
     * @param minpoint The corner with the smallest coordinates of the
     * hypercube.
     * @param maxpoint The corner with the largest coordinates of the
     * hypercube.
     *
     * @return meta_q_iterator_pair A pair of iterators pointing to the
     * beginning and end of the metadata objects inside a hypercube formed by
     * the specified points, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note minpoint must contain values smaller than maxpoint, otherwise
     * there is no guarantee the query will succeed.
     */
    meta_q_iterator_pair query_metadata(
        const spatial_point_type& minpoint,
        const spatial_point_type& maxpoint
    ) const
    {
        meta_q_iterator_pair iterators;
        query_metadata(minpoint, maxpoint, iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata-data pairs nearest to a point, at data slot 0.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     * @param begin The iterator to the beginning of a set of the N
     * metadata-data pairs nearest to a point, at data slot 0.
     * @param end The iterator to the end of a set of the N
     * metadata-data pairs nearest to a point, at data slot 0.
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
    void query_elements(
        const spatial_point_type& point,
        unsigned int nearest,
        elem_q_iterator& begin,
        elem_q_iterator& end
    ) const
    {
        _query_elements(queries::nearest<
            spatial_point_type
            >(point, nearest), begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata-data pairs nearest to a point, at data slot 0.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     *
     * @return elem_q_iterator_pair A pair of iterators pointing to the
     * beginning and end of a set of the N metadata-data pairs nearest to a
     * point, at data slot 0.
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
    elem_q_iterator_pair query_elements(
        const spatial_point_type& point,
        unsigned int nearest
    ) const
    {
        elem_q_iterator_pair iterators;
        query_elements(point, nearest, iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata-data pairs nearest to a point, with data slot selection.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     * @param slot The slot of data to iterate over.
     * @param begin The iterator to the beginning of a set of the N
     * metadata-data pairs nearest to a point, with data slot selection.
     * @param end The iterator to the end of a set of the N metadata-data
     * pairs nearest to a point, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    void query_elements(
        const spatial_point_type& point,
        unsigned int nearest,
        size_t slot,
        elem_q_iterator_slot& begin,
        elem_q_iterator_slot& end
    ) const
    {
        _query_elements(queries::nearest<
            spatial_point_type
            >(point, nearest), slot, begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata-data pairs nearest to a point, with data slot selection.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     * @param slot The slot of data to iterate over.
     *
     * @return elem_q_iterator_slot_pair A pair of iterators pointing to the
     * beginning and end of a set of the N metadata-data pairs nearest to a
     * point, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     */
    elem_q_iterator_slot_pair query_elements(
        const spatial_point_type& point,
        unsigned int nearest,
        size_t slot
    ) const
    {
        elem_q_iterator_slot_pair iterators;
        query_elements(point, nearest, slot,
            iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata objects nearest to a point, with data slot selection.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     * @param begin The iterator to the beginning of a set of the N metadata
     * objects nearest to a point, with data slot selection.
     * @param end The iterator to the end of a set of the N metadata objects
     * nearest to a point, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     */
    void query_metadata(
        const spatial_point_type& point,
        unsigned int nearest,
        meta_q_iterator& begin,
        meta_q_iterator& end
    ) const
    {
        _begin_query_metadata(queries::nearest<
            spatial_point_type
            >(point, nearest), begin, end);
    }

    /**
     * @brief Get the iterators to the beginning and end of a set of the N
     * metadata objects nearest to a point, with data slot selection.
     *
     * @param point The reference point when searching for the nearest points.
     * @param nearest The number of nearest points to retrieve.
     *
     * @return meta_q_iterator_pair A pair of iterators pointing to the
     * beginning and end of a set of the N metadata objects nearest to a point,
     * with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     */
    meta_q_iterator_pair query_metadata(
        const spatial_point_type& point,
        unsigned int nearest
    ) const
    {
        meta_q_iterator_pair iterators;
        query_metadata(point, nearest, iterators.first, iterators.second);
        return iterators;
    }

    /**
     * @brief Get the metadata-data pair corresponding to the element at an
     * specific spatial location, at data slot 0.
     *
     * @param point The exact spatial location to find the element.
     *
     * @return element_pair The metadata-data pair corresponding to the element
     * at an specific spatial location, at data slot 0.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     *
     * @note This method will throw an exception if there is no element at the
     * specified location
     */
    inline element_pair find_element(
        const spatial_point_type& point
    ) const
    {
        elem_q_iterator begin, end;
        query_elements(point, 1, begin, end);

        if (begin == end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            EX3_THROW(empty_query_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        const element_pair& element = *begin;

        if (++begin != end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            size_t n = std::distance(begin, end) + 1;
            EX3_THROW(multiple_results_exception()
                << expected_num_elements(1)
                << actual_num_elements(n)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        spatial_point_type elem_point;
        _meta_adapter.get_location(*element.first, elem_point);

        if (!_spatial_adapter.equals(point, elem_point)) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            std::vector<double> eloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(elem_point, eloc.begin());

            EX3_THROW(location_mismatch_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << actual_location(eloc)
                << dataset_name(get_basename()));
        }

        return element;
    }

    /**
     * @brief Get the metadata-data pair corresponding to the element at an
     * specific spatial location, with data slot selection.
     *
     * @param point The exact spatial location to find the element.
     * @param slot The slot of data to access.
     *
     * @return element_pair The metadata-data pair corresponding to the element
     * at an specific spatial location, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if the dataset is open with
     * the NO_DATA flag.
     *
     * @note This method will throw an exception if there is no element at the
     * specified location
     */
    inline element_pair find_element(
        const spatial_point_type& point,
        size_t slot
    ) const
    {
        elem_q_iterator_slot begin, end;
        query_elements(point, 1, slot, begin, end);

        if (begin == end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            EX3_THROW(empty_query_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        const element_pair& element = *begin;

        if (++begin != end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            size_t n = std::distance(begin, end) + 1;
            EX3_THROW(multiple_results_exception()
                << expected_num_elements(1)
                << actual_num_elements(n)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        spatial_point_type elem_point;
        _meta_adapter.get_location(*element.first, elem_point);

        if (!_spatial_adapter.equals(point, elem_point)) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            std::vector<double> eloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(elem_point, eloc.begin());

            EX3_THROW(location_mismatch_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << actual_location(eloc)
                << dataset_name(get_basename()));
        }

        return element;
    }

    /**
     * @brief Get the metadata object corresponding to the element at an
     * specific spatial location, with data slot selection.
     *
     * @param point The exact spatial location to find the element.
     *
     * @return metadata_type& The object corresponding to the element at an
     * specific spatial location, with data slot selection.
     *
     * @note This method will throw an exception if the dataset is open with
     * the RWP flag.
     *
     * @note This method will throw an exception if there is no element at the
     * specified location
     */
    inline metadata_type& find_metadata(
        const spatial_point_type& point
    ) const
    {
        meta_q_iterator begin, end;
        query_metadata(point, 1, begin, end);

        if (begin == end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            EX3_THROW(empty_query_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        metadata_type& metadata = *begin;

        if (++begin != end) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(point, rloc.begin());

            size_t n = std::distance(begin, end) + 1;
            EX3_THROW(multiple_results_exception()
                << expected_num_elements(1)
                << actual_num_elements(n)
                << requested_location(rloc)
                << dataset_name(get_basename()));
        }

        spatial_point_type meta_point;
        _meta_adapter.get_location(metadata, meta_point);

        if (!_spatial_adapter.equals(point, meta_point)) {

            std::vector<double> rloc(num_spatial_dims);
            helpers::copy_location<
                num_spatial_dims
                >(point, rloc.begin());

            std::vector<double> mloc(num_spatial_dims);
            helpers::copy_location<
                 num_spatial_dims
                 >(meta_point, mloc.begin());

            EX3_THROW(location_mismatch_exception()
                << expected_num_elements(1)
                << actual_num_elements(0)
                << requested_location(rloc)
                << actual_location(mloc)
                << dataset_name(get_basename()));
        }

        return metadata;
    }

    /**
     * @brief Read an existing element from disk.
     *
     * @param uid The uid of the element to read.
     * @param meta The resulting read metadata.
     * @param pdata A pointer to the resulting data of the element.
     * @param slot The data slot to read from.
     *
     * @return true The uid is valid and the element was successfully read.
     * @return false The uid is invalid so the element was not read.
     *
     * @note If pdata is NULL, then the slot is ignored.
     *
     * @note If the dataset was open with the NO_DATA flag, then pdata must
     * be NULL, otherwise an exception is thrown.
     */
    inline bool read_element(
        uid_t uid,
        metadata_type& meta,
        char* pdata=0,
        size_t slot=0
    ) const
    {
        if (pdata == 0) {
            return read_metadata(uid, meta);
        }
        else {

            assert_has_data();

            file_metadata file_meta;
            if(!read_metadata(uid, file_meta))
                return false;

            meta = file_meta;

            const size_t data_size = _meta_adapter.get_data_size(file_meta);
            const size_t data_offset = file_meta.data_offset +
                compute_slot_offset(slot);

            _fds.seek(FD_DATA, data_offset, SEEK_SET);
            _fds.read(FD_DATA, pdata, data_size, true, true);

            return true;
        }
    }

    /**
     * @brief Read several elements from disk.
     *
     * @tparam ITI The type of the input iterator of uids.
     * @tparam ITO The type of the output iterator of metadata.
     *
     * @param uidbegin The iterator pointing to the beginning of a sequence
     * of uids.
     * @param uidend The iterator pointing to after the last element of a
     * sequence of uids.
     * @param itmeta The output iterator for the sequence of metadata read
     * from the file.
     *
     * @return size_t The number of successfully read elements until one
     * failed.
     *
     * @note The reading will start at *uidbegin and proceed sequentially
     * until the sequence ends or reading one of the uids fails.
     */
    template <typename ITI, typename ITO>
    inline size_t read_elements(
        ITI uidbegin,
        ITI uidend,
        ITO itmeta
    ) const
    {
        size_t n = 0;
        z0rg::zero_copy<metadata_type, ITO> meta;
        for (ITI ituid = uidbegin; ituid != uidend; ituid++, itmeta++, n++) {
            if (!read_element(*ituid, meta.get(itmeta)))
                break;
            meta.set(itmeta);
        }
        return n;
    }

    /**
     * @brief Write an existing element to disk.
     *
     * @param meta The element's metadata to write
     * @param pdata A pointer with the element's data to write.
     * @param slot The data slot to read from.
     *
     * @note If pdata is NULL, then the slot is ignored and nothing
     * is written.
     *
     * @note If the dataset was open with the NO_DATA flag, then pdata must
     * be NULL, otherwise an exception is thrown. This is done to avoid the
     * programming mistake of always expecting the pdata array to be written.
     *
     * @note The uid must exist in the file, otherwise push_element
     * must be used.
     *
     * @note It is not possible to resize the data of the elements, so the
     * data size contained in the metadata being passed as argument must be
     * same as the one stored on disk, otherwise an exception is thrown. This
     * behavior will not be enforced if NO_DATA is true because there is no
     * data being resized.
     */
    inline void write_element(
        const metadata_type& meta,
        const char* pdata=0,
        size_t slot=0
    ) const
    {
        const uid_t uid = _meta_adapter.get_uid(meta);

        assert_can_rwp();
        assert_uid_in_file(uid);

        if (pdata != 0) {
            assert_has_data();
        }

        file_metadata file_meta;
        read_metadata(uid, file_meta);

        const uid_t old_uid = _meta_adapter.get_uid(file_meta);

        if (old_uid != uid) {
            EX3_THROW(inconsistent_meta_exception()
                << requested_uid(uid)
                << read_uid(old_uid)
                << dataset_name(get_basename()));
        }

        // Update the data
        if (!_fds.no_data()) {

            const size_t old_data_size = _meta_adapter.get_data_size(file_meta);
            const size_t new_data_size = _meta_adapter.get_data_size(meta);

            if (old_data_size != new_data_size) {
                EX3_THROW(invalid_data_size_exception()
                << expected_size(old_data_size)
                << actual_size(new_data_size)
                << dataset_name(get_basename()));
            }

            if (pdata != 0) {

                const size_t data_offset = file_meta.data_offset +
                    compute_slot_offset(slot);

                _fds.seek(FD_DATA, data_offset, SEEK_SET);
                _fds.write(FD_DATA, pdata, new_data_size);
            }
        }

        // Update the meta file without touching the
        // file_metadata-specific fields

        const off64_t off = get_element_file_offset(uid);
        _fds.seek(FD_META, off, SEEK_SET);
        _fds.write_object(FD_META, meta);
    }

    /**
     * @brief Write several existing elements to disk.
     *
     * @tparam ITI The type of the input iterator of metadata.
     *
     * @param metabegin The iterator pointing to the beginning of a sequence
     * of metadata.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata.
     *
     * @note The uid must exist in the file, otherwise push_element
     * must be used.
     *
     * @note It is not possible to resize the data of the elements, so the
     * individual data size of each metadata element being passed as arguments
     * must be same as the ones stored on disk, otherwise an exception is
     * thrown. This behavior will not be enforced if NO_DATA is true because
     * there is no data being resized.
     */
    template <typename ITI>
    inline void write_elements(
        ITI metabegin,
        ITI metaend
    ) const
    {
        for (ITI itmeta = metabegin; itmeta != metaend; itmeta++) {
            write_element(*itmeta);
        }
    }

    /**
     * @brief Write a new element to disk.
     *
     * @param meta The element's metadata to write
     * @param pdata A pointer with the element's data to write.
     *
     * @return uid_t The uid of the newly written element.
     *
     * @note If pdata is NULL, then nothing is written.
     *
     * @note If the dataset was open with the NO_DATA flag, then pdata must
     * be NULL, otherwise an exception is thrown. This is done to avoid the
     * programming mistake of always expecting the pdata array to be written.
     *
     * @note This method creates new uids for the elements, so the current
     * uid of the metadata passed as parameter is ignored.
     *
     * @note This method will throw an exception if the number of slots in the
     * file is larger than one because that would require data relocation at
     * each push.
     */
    inline uid_t push_element(
        const metadata_type& meta,
        const char* pdata=0
    ) const
    {
        assert_can_rwp();

        // Cannot push elements to datasets with multiple slots
        if (_num_slots > 1) {
            EX3_THROW(invalid_num_slots_exception()
                << expected_num_slots(1)
                << actual_num_slots(_num_slots)
                << dataset_name(get_basename()));
        }

        const uid_t uid = compute_num_elements() + 1;

        // Create a new file_metadata loaded with
        // dataset-specific information

        file_metadata file_meta(meta);
        file_meta.clean_bit = file_metadata::CLEAN_BIT_MAGIC;

        _meta_adapter.set_uid(file_meta, uid);

        if (_fds.no_data()) {

            if (pdata != 0) {
                EX3_THROW(no_data_exception()
                    << dataset_name(get_basename()));
            }

            // Set the data offset of all elements to zero if no_data is
            // set. This will force any further the dataset to always be
            // opened with no_data.
            file_meta.data_offset = 0;
        }
        else {

            // Reserve space in the data file

            const size_t data_size = _meta_adapter.get_data_size(meta);
            const size_t aligned_data_size = align64(data_size);

            file_meta.data_offset = _fds.get_file_size(FD_DATA);

            const off64_t new_file_size = file_meta.data_offset +
                aligned_data_size;

            // Must always happen because of the alignment
            // The 'if' is on aligned_data_size, not on new_file_size,
            // otherwise you risk writing a zero byte to a
            // valid data location of another element!
            if (aligned_data_size > 0 && !_fds.no_data()) {
                const char zero = 0;
                _fds.seek(FD_DATA, new_file_size-1, SEEK_SET);
                _fds.write(FD_DATA, &zero, 1);
            }

            if (pdata != 0) {
                _fds.seek(FD_DATA, file_meta.data_offset, SEEK_SET);
                _fds.write(FD_DATA, pdata, data_size);
            }
        }

        // write the metadata to the meta file

        const off64_t off = get_element_file_offset(uid);
        _fds.seek(FD_META, off, SEEK_SET);
        _fds.write_object(FD_META, file_meta);

        // Align the metadata if necessary

        if (meta_szof < _file_metadata_size) {
            const char zero = 0;
            _fds.seek(FD_META, off + _file_metadata_size - 1, SEEK_SET);
            _fds.write(FD_META, &zero, 1);
        }

        // Ensure the metadata was properly written

        assert_meta_file_size();
        assert_uid_in_file(uid);

        return uid;
    }

    /**
     * @brief Write several new elements to disk.
     *
     * @tparam ITI The type of the input iterator of metadata.
     *
     * @param metabegin The iterator pointing to the beginning of a sequence
     * of metadata.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata.
     *
     * @note This method creates new uids for the elements, so the current
     * uids of the metadata passed as parameter are ignored.
     *
     * @note This method will throw an exception if the number of slots in the
     * file is larger than one because that would require data relocation at
     * each push.
     */
    template <typename ITI>
    inline void push_elements(
        ITI metabegin,
        ITI metaend
    ) const
    {
        for (ITI itmeta = metabegin; itmeta != metaend; itmeta++) {
            push_element(*itmeta);
        }
    }

    /**
     * @brief Write several new elements to disk.
     *
     * @tparam ITI The type of the input iterator of metadata.
     * @tparam ITO The type of the output iterator of uids.
     *
     * @param metabegin The iterator pointing to the beginning of a sequence
     * of metadata.
     * @param metaend The iterator pointing to after the last element of a
     * sequence of metadata.
     * @param uidbegin The output iterator for the sequence of new uids
     * written to the file.
     *
     * @note This method creates new uids for the elements, so the current
     * uids of the metadata passed as parameter are ignored.
     *
     * @note This method will throw an exception if the number of slots in the
     * file is larger than one because that would require data relocation at
     * each push.
     */
    template <typename ITI, typename ITO>
    inline void push_elements(
        ITI metabegin,
        ITI metaend,
        ITO uidbegin
    ) const
    {
        for (ITI itmeta = metabegin; itmeta != metaend; itmeta++, uidbegin++) {
            *uidbegin = push_element(*itmeta);
        }
    }
};

}
