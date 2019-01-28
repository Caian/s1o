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

#include "exceptions.hpp"

#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

namespace s1o {

/**
 * @brief Checked version of lseek64.
 *
 * @param fd See lseek64.
 * @param offset See lseek64.
 * @param whence See lseek64.
 *
 * @return off64_t See lseek64.
 *
 * @note This method will throw an exception on failure.
 */
static off64_t lseek64_checked(
    int fd,
    off64_t offset,
    int whence
)
{
    off64_t result = lseek64(fd, offset, whence);

    if (result == (off64_t)-1) {
        EX3_THROW(io_exception()
            << operation_name("lseek64")
            << errno_value(errno));
    }

    return result;
}

/**
 * @brief Checked version of read.
 *
 * @param fd See read.
 * @param buf See read.
 * @param count See read.
 *
 * @return size_t See read.
 *
 * @note This method will throw an exception on failure.
 */
static size_t read_checked(
    int fd,
    void* buf,
    size_t count
)
{
    ssize_t result = read(fd, buf, count);

    if (result == -1) {
        EX3_THROW(io_exception()
            << operation_name("read")
            << errno_value(errno));
    }

    return static_cast<size_t>(result);
}

/**
 * @brief Checked version of write.
 *
 * @param fd See write.
 * @param buf See write.
 * @param count See write.
 *
 * @note This method will throw an exception on failure.
 */
static void write_checked(
    int fd,
    const void* buf,
    size_t count
)
{
    ssize_t result = write(fd, buf, count);

    if (result < 0) {
        EX3_THROW(io_exception()
            << operation_name("write")
            << errno_value(errno));
    }

    size_t uresult = static_cast<size_t>(result);

    if (uresult != count) {
        EX3_THROW(incomplete_write_exception()
            << expected_size(count)
            << actual_size(uresult));
    }
}

/**
 * @brief Checked version of fsync.
 *
 * @param fd See fsync.
 */
static void fsync_checked(
    int fd
)
{
    if (fsync(fd) < 0) {
        EX3_THROW(io_exception()
            << operation_name("fsync")
            << errno_value(errno));
    }
}

/**
 * @brief Checked version of open.
 *
 * @param pathname See open.
 * @param flags See open.
 * @param mode See open.
 *
 * @return int See open.
 */
static int open_checked(
    const char *pathname,
    int flags,
    mode_t mode
)
{
    int fd = open(pathname, flags, mode);

    if (fd < 0) {
        EX3_THROW(io_exception()
            << operation_name("open")
            << file_name(pathname)
            << errno_value(errno)
            << operation_mode(mode)
            << operation_flags(flags));
    }

    return fd;
}

/**
 * @brief Checked version of close.
 *
 * @param fd See close.
 */
static void close_checked(
    int fd
)
{
    if (close(fd) != 0) {
        EX3_THROW(io_exception()
            << operation_name("close")
            << errno_value(errno));
    }
}

/**
 * @brief Checked version of unlink.
 *
 * @param pathname See unlink.
 */
static void unlink_checked(
    const char *pathname
)
{
    if (unlink(pathname) < 0) {
        EX3_THROW(io_exception()
            << operation_name("unlink")
            << file_name(pathname)
            << errno_value(errno));
    }
}

/**
 * @brief Checked version of mmap.
 *
 * @param addr See mmap.
 * @param length See mmap.
 * @param prot See mmap.
 * @param flags See mmap.
 * @param fd See mmap.
 * @param offset See mmap.
 *
 * @return void* See mmap.
 */
static void *mmap_checked(
    void *addr,
    size_t length,
    int prot,
    int flags,
    int fd,
    off_t offset
)
{
    void* ptr = mmap(addr, length, prot, flags, fd, offset);

    if (ptr == MAP_FAILED) {
        EX3_THROW(io_exception()
            << operation_name("mmap")
            << errno_value(errno));
    }

    return ptr;
}

/**
 * @brief Checked version of msync.
 *
 * @param addr See msync.
 * @param length See msync.
 * @param flags See msync.
 */
static void msync_checked(
    void *addr,
    size_t length,
    int flags
)
{
    if (msync(addr, length, flags) < 0) {
        EX3_THROW(io_exception()
            << operation_name("msync")
            << errno_value(errno));
    }
}

/**
 * @brief Checked version of munmap.
 *
 * @param addr See munmap.
 * @param length See munmap.
 */
static void munmap_checked(
    void *addr,
    size_t length
)
{
    if (munmap(addr, length) != 0) {
        EX3_THROW(io_exception()
            << operation_name("munmap")
            << errno_value(errno));
    }
}

}
