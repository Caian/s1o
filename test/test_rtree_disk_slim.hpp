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

#include <s1o/dataset.hpp>
#include <s1o/spatial_adapters/rtree_disk_slim.hpp>
#include <string>
#include <vector>

#define DATASET_TEST(Class, Test) \
    TEST(RtreeDiskSlim##Class, Test) { \
        const char* dataset_name = "RtreeDiskSlim" #Class#Test ; \
        ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

namespace test
{
    struct my_metadata
    {
        int uid;
        int size;
        float x;
        float y;
        short value1;
        char value2;

        bool operator ==(const my_metadata& other) const
        {
            return
                this->uid == other.uid &&
                this->size == other.size &&
                this->x == other.x &&
                this->y == other.y &&
                this->value1 == other.value1 &&
                this->value2 == other.value2;
        }
    };

    struct my_adapter
    {
        static const unsigned int num_spatial_dims = 2;
        typedef float spatial_value_type;
        typedef my_metadata metadata_type;

        const std::string check;
        const std::string meta;
        const std::string data;

        template <typename TPoint>
        void get_location(
            const metadata_type& p_meta,
            TPoint& point_out
        ) const
        {
            point_out.template set<0>(p_meta.x);
            point_out.template set<1>(p_meta.y);
        }

        s1o::uid_t get_uid(
            const metadata_type& p_meta
        ) const
        {
            return static_cast<s1o::uid_t>(p_meta.uid);
        }

        void set_uid(
            metadata_type& p_meta,
            const s1o::uid_t& uid
        ) const
        {
            p_meta.uid = static_cast<int>(uid);
        }

        size_t get_data_size(
            const metadata_type& p_meta
        ) const
        {
            return static_cast<size_t>(p_meta.size);
        }

        s1o::c_meta_check_ptr_t get_meta_check_ptr() const
        {
            return check.c_str();
        }

        size_t get_meta_check_size() const
        {
            return check.size() + 1;
        }

        const char* get_meta_file_ext() const
        {
            return meta.c_str();
        }

        const char* get_data_file_ext() const
        {
            return data.c_str();
        }

        my_adapter() :
            check("uid@i4/size@i4/x@f4/y@f4/value1@i2/value2@i1"),
            meta("meta"),
            data("data")
        {
        }
    };

    typedef boost::geometry::index::quadratic<16> params_t;
    typedef boost::geometry::cs::cartesian coord_sys_t;
    typedef s1o::spatial_adapters::rtree_disk_slim<
        params_t,
        coord_sys_t
        > rtree;
    typedef rtree::mparams_t mparams_t;
    typedef s1o::dataset<my_adapter, rtree> my_dataset;
    typedef my_dataset::meta_l_iterator my_meta_iter;
    typedef my_dataset::elem_l_iterator my_elem_iter;
    typedef my_dataset::elem_l_iterator_slot my_elem_iter_s;
    typedef my_dataset::meta_q_iterator my_meta_qiter;
    typedef my_dataset::elem_q_iterator my_elem_qiter;
    typedef my_dataset::elem_q_iterator_slot my_elem_qiter_s;
    typedef std::vector<my_metadata> meta_vector;
    typedef std::vector<s1o::uid_t> uid_vector;
    typedef std::vector<std::vector<std::vector<char> > > data_vector;
}
