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

#include "default_data.hpp"

#include <boost/interprocess/managed_mapped_file.hpp>

#include <string>

namespace s1o {
namespace initialization_data {

/**
 * @brief Memory mapped data passed to the spatial adapter for initialization.
 *
 */
struct mapped_data
{
    /** The type of the memory managed file. */
    typedef boost::interprocess::managed_mapped_file mfile_t;

    /** The standard initialization data for the spatial storage. */
    default_data base_data;

    /** The prefix to add to any construct operation using this initialization
        data. */
    std::string prefix;

    /** The pointer to the memory mapped file. */
    mfile_t* mapped_file;

    /**
     * @brief Construct a new mapped_data object.
     *
     * @param base_data The standard initialization data for the spatial
     * storage.
     * @param prefix The prefix to add to any construct operation using this
     * initialization data.
     * @param mapped_file The pointer to the memory mapped file.
     */
    mapped_data(
        const default_data& base_data,
        const std::string& prefix,
        mfile_t* mapped_file
    ) :
        base_data(base_data),
        prefix(prefix),
        mapped_file(mapped_file)
    {
    }

    /**
     * @brief Construct a new mapped_data object adding an extra prefix
     * string.
     *
     * @param original_data The original mapped initialization data.
     * @param append_prefix An extra prefix string that will be added after
     * the original prefix.
     */
    mapped_data(
        const mapped_data& original_data,
        const std::string& append_prefix
    ) :
        base_data(original_data.base_data),
        prefix(original_data.prefix + append_prefix),
        mapped_file(original_data.mapped_file)
    {
    }
};

}}
