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

#include "types.hpp"

namespace s1o {

/**
 * @brief The metadata structure extended
 * for use in a dataset.
 *
 * @tparam TMetadata The base metadata structure.
 */
template <typename TMetadata>
struct file_metadata : public TMetadata
{
    static const int CLEAN_BIT_MAGIC = 0xCA02178F;
    static const int DIRTY_BIT_MAGIC = 0xDF349172;

    foffset_t data_offset; /** The offset of the data
                               in the data file. */

    int clean_bit; /** Data integrity field, CLEAN_BIT_MAGIC for clean,
                       DIRTY_BIT_MAGIC to indicate an interruption during
                       the write of metadata or data, and anything else
                       to indicate corruption of the metadata. */

    inline file_metadata()
    {
    }

    inline explicit file_metadata(
        const TMetadata& meta
    ) :
        TMetadata(meta)
    {
    }
};

/** Pointer to the data used to validate the dataset. */
typedef char* meta_check_ptr_t;

/** Constant pointer to the data used to validate the dataset. */
typedef const char* c_meta_check_ptr_t;

}
