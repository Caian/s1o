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

#include <string>

namespace s1o {
namespace initialization_info {

/**
 * @brief Default information passed to the spatial adapter for
 * initialization.
 *
 */
struct default_info
{
    /** The path and filename of the dataset without the trailing
        extensions. */
    std::string basename;

    /** Indicates if the dataset is being created from a collection of
        elements of is being opened. */
    bool is_new;

    /** Indicates if the datataset allows writes. */
    bool can_write;

    default_info(
        const std::string& basename,
        bool is_new,
        bool can_write
    ) :
        basename(basename),
        is_new(is_new),
        can_write(can_write)
    {
    }
};

}}
