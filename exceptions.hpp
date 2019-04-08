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

#include "metadata.hpp"

#include <ex3/exceptions.hpp>
#include <ex3/pretty.hpp>

#include <utility>
#include <string>
#include <vector>

#include <stdint.h>
#include <unistd.h>

#define S1O_MAKE_EINFO(Name, Type) \
    typedef boost::error_info< \
        struct tag_##Name##_name, \
        Type \
        > Name;

#define S1O_MAKE_DERIVED(Name, Base) \
    class Name : \
        public Base \
    { \
    };

#define S1O_MAKE_EXCEPTION(Name) \
    S1O_MAKE_DERIVED(Name, s1o_exception)

namespace s1o {

class s1o_exception :
    public ex3::exception_base
{
};

typedef ex3::pretty_container<
    std::vector<double>
    > location_container;

S1O_MAKE_EINFO(dataset_name         , std::string)
S1O_MAKE_EINFO(file_extension       , std::string)
S1O_MAKE_EINFO(file_name            , std::string)
S1O_MAKE_EINFO(who_value            , int)
S1O_MAKE_EINFO(errno_value          , int)
S1O_MAKE_EINFO(operation_name       , std::string)
S1O_MAKE_EINFO(maximum_size         , size_t)
S1O_MAKE_EINFO(expected_size        , size_t)
S1O_MAKE_EINFO(actual_size          , size_t)
S1O_MAKE_EINFO(expected_value       , int64_t)
S1O_MAKE_EINFO(actual_value         , int64_t)
S1O_MAKE_EINFO(expected_offset      , int64_t)
S1O_MAKE_EINFO(actual_offset        , int64_t)
S1O_MAKE_EINFO(expected_slot_size   , int64_t)
S1O_MAKE_EINFO(actual_slot_size     , int64_t)
S1O_MAKE_EINFO(minimum_num_slots    , size_t)
S1O_MAKE_EINFO(expected_num_slots   , size_t)
S1O_MAKE_EINFO(requested_num_slots  , size_t)
S1O_MAKE_EINFO(actual_num_slots     , size_t)
S1O_MAKE_EINFO(position_value       , size_t)
S1O_MAKE_EINFO(size_value           , size_t)
S1O_MAKE_EINFO(maximum_uid          , uid_t)
S1O_MAKE_EINFO(read_uid             , uid_t)
S1O_MAKE_EINFO(requested_uid        , uid_t)
S1O_MAKE_EINFO(maximum_slot         , size_t)
S1O_MAKE_EINFO(requested_slot       , size_t)
S1O_MAKE_EINFO(expected_num_elements, size_t)
S1O_MAKE_EINFO(actual_num_elements  , size_t)
S1O_MAKE_EINFO(last_pointer         , const void*)
S1O_MAKE_EINFO(actual_pointer       , const void*)
S1O_MAKE_EINFO(operation_mode       , int)
S1O_MAKE_EINFO(operation_flags      , int)
S1O_MAKE_EINFO(maximum_attempts     , size_t)
S1O_MAKE_EINFO(requested_location   , location_container)
S1O_MAKE_EINFO(actual_location      , location_container)

S1O_MAKE_EXCEPTION(create_without_write_exception     )
S1O_MAKE_EXCEPTION(open_with_size_exception           )
S1O_MAKE_EXCEPTION(no_data_exception                  )
S1O_MAKE_EXCEPTION(read_only_exception                )
S1O_MAKE_EXCEPTION(invalid_who_exception              )
S1O_MAKE_EXCEPTION(mmapped_exception                  )
S1O_MAKE_EXCEPTION(not_mmapped_exception              )
S1O_MAKE_EXCEPTION(empty_mmap_exception               )
S1O_MAKE_EXCEPTION(invalid_uid_exception              )
S1O_MAKE_EXCEPTION(invalid_slot_exception             )
S1O_MAKE_EXCEPTION(invalid_num_slots_exception        )
S1O_MAKE_EXCEPTION(invalid_data_size_exception        )
S1O_MAKE_EXCEPTION(unsorted_data_exception            )
S1O_MAKE_EXCEPTION(not_initialized_exception          )
S1O_MAKE_EXCEPTION(location_data_unavailable_exception)
S1O_MAKE_EXCEPTION(already_initialized_exception      )
S1O_MAKE_EXCEPTION(extensions_equal_exception         )
S1O_MAKE_EXCEPTION(metadata_count_mismatch_exception  )
S1O_MAKE_EXCEPTION(null_mapped_file_pointer_exception )
S1O_MAKE_EXCEPTION(io_exception                       )
S1O_MAKE_EXCEPTION(format_exception                   )
S1O_MAKE_EXCEPTION(query_exception                    )

S1O_MAKE_DERIVED(incomplete_read_exception    , io_exception       )
S1O_MAKE_DERIVED(incomplete_write_exception   , io_exception       )
S1O_MAKE_DERIVED(check_size_too_big_exception , format_exception   )
S1O_MAKE_DERIVED(base_data_mismatch_exception , format_exception   )
S1O_MAKE_DERIVED(extra_meta_bytes_exception   , format_exception   )
S1O_MAKE_DERIVED(extra_slot_bytes_exception   , format_exception   )
S1O_MAKE_DERIVED(check_data_mismatch_exception, format_exception   )
S1O_MAKE_DERIVED(inconsistent_meta_exception  , format_exception   )
S1O_MAKE_DERIVED(inconsistent_data_exception  , format_exception   )
S1O_MAKE_DERIVED(inconsistent_index_exception , format_exception   )
S1O_MAKE_DERIVED(index_size_too_big_exception , format_exception   )
S1O_MAKE_DERIVED(empty_query_exception        , query_exception    )
S1O_MAKE_DERIVED(multiple_results_exception   , query_exception    )
S1O_MAKE_DERIVED(location_mismatch_exception  , query_exception    )

}
