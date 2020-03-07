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

#include "test_rtree_disk.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <vector>
#include <cmath>

#include <stdlib.h>

namespace test {

struct GetterFull
{
    typedef test::meta_vector::const_iterator meta_iterator;
    typedef test::data_vector::const_iterator data_iterator;

    meta_iterator meta_it, meta_end;
    data_iterator data_it, data_end;

    GetterFull(
        const test::meta_vector& meta,
        const test::data_vector& data
    ) :
        meta_it(meta.begin()),
        meta_end(meta.end()),
        data_it(data.begin()),
        data_end(data.end())
    {
    }

    inline bool operator ()(const test::my_metadata*& meta, const char*& data)
    {
        if ((meta_it == meta_end) || (data_it == data_end)) {

            if ((meta_it == meta_end) && (data_it == data_end)) {
                return false;
            }
            else {
                // Shouldn't happen at all!
                throw 0;
            }
        }

        meta = &(*meta_it);
        meta_it++;

        data = &(*data_it)[0][0];
        data_it++;

        return true;
    }
};

struct GetterNoData
{
    typedef test::meta_vector::const_iterator meta_iterator;

    meta_iterator meta_it, meta_end;

    GetterNoData(
        const test::meta_vector& meta
    ) :
        meta_it(meta.begin()),
        meta_end(meta.end())
    {
    }

    inline bool operator ()(const test::my_metadata*& meta, const char*& data)
    {
        if (meta_it == meta_end) {
            return false;
        }

        meta = &(*meta_it);
        meta_it++;

        data = 0;

        return true;
    }
};

}

namespace {

template <typename Getter>
inline static void create_two_stage(
    const std::string& dataset_name,
    size_t slots,
    Getter getter
)
{
    // Create the optimized dataset using a temporary and unoptimized one. This
    // happens if you are constructing your dataset from a large element stream
    // that does not allow reading the metadata first or does not allow rewind

    std::string dataset_tmp_name = dataset_name + "_tmp";

    // Create the unoptimized version of the dataset
    test::my_dataset dataset_tmp(dataset_tmp_name, s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, 1);

    // Push the elements as they come

    {
        const test::my_metadata* meta = NULL;
        const char* data = NULL;

        while (getter(meta, data)) {
            ASSERT_NO_THROW(dataset_tmp.push_element(*meta, data));
        }

        // Ensure data is written
        ASSERT_NO_THROW(dataset_tmp.sync_metadata());
        ASSERT_NO_THROW(dataset_tmp.sync_data());
    }

    // Create an iterator to wrap the metadata reading from the temporary
    // dataset

    {
        // Create a new optimized dataset with the metadata stored in the
        // temporary dataset
        test::my_dataset dataset_opt(dataset_name, 0, slots,
            dataset_tmp.begin_read_metadata(),
            dataset_tmp.end_read_metadata());

        // Copy the data to the new dataset at slot 0

        test::my_dataset::elem_l_iterator begin, end, it;

        ASSERT_NO_THROW(begin = dataset_opt.begin_elements());
        ASSERT_NO_THROW(end = dataset_opt.end_elements());

        for (it = begin; it != end; it++) {

            const test::my_metadata* const meta = it->first;
            char* const dst_data = it->second;

            test::my_metadata tmp_meta;

            ASSERT_TRUE(dataset_tmp.read_element(meta->uid, tmp_meta,
                dst_data));
        }

        // Ensure data is written
        dataset_opt.sync_metadata();
        dataset_opt.sync_data();
    }

    // Remove the unoptimized version
    test::my_dataset::unlink(dataset_tmp_name);
}

/**
 *
 */
DATASET_TEST(DatasetOptimizeSingleSlot, WithDataGetFull)

    const int N = 50;
    const int slots = 1;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

    for (int i = 0; i < N; i++) {

        int size = 33 * i + 1;

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = size;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    ASSERT_NO_THROW(create_two_stage(dataset_name, slots,
        test::GetterFull(stuff, stuff_data)));

    try {
        test::my_dataset dataset(dataset_name, 0, 0, slots);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = dataset.get_metadata(i+1);
            const char* const data = dataset.get_data(i+1);

            std::pair<std::vector<char>::iterator, const char*> comp;
            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data);

            ASSERT_TRUE(stuff[i] == meta);

            if (comp.first != stuff_data[i][0].end())
                std::cerr << "shit!!" << std::endl;

            ASSERT_TRUE(stuff_data[i][0].end() == comp.first);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

DATASET_TEST(DatasetOptimizeMultipleSlot, WithDataGetFull)

    const int N = 50;
    const int slots = 5;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

    for (int i = 0; i < N; i++) {

        int size = 33 * i + 1;

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = size;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    ASSERT_NO_THROW(create_two_stage(dataset_name, slots,
        test::GetterFull(stuff, stuff_data)));

    try {
        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_WRITE, 0, slots);

        for (int i = 0; i < N; i++) {
            for (int j = 1; j < slots; j++) {

                char* data = dataset.get_data(i+1, j);

                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {
        test::my_dataset dataset(dataset_name, 0, 0, slots);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = dataset.get_metadata(i+1);
            const char* const data = dataset.get_data(i+1);

            std::pair<std::vector<char>::iterator, const char*> comp;
            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data);

            ASSERT_TRUE(stuff[i] == meta);
            ASSERT_TRUE(stuff_data[i][0].end() == comp.first);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

DATASET_TEST(DatasetOptimizeMultipleSlot, WithDataGetNoData)

    const int N = 50;
    const int slots = 5;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

    for (int i = 0; i < N; i++) {

        int size = 33 * i + 1;

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = size;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    ASSERT_NO_THROW(create_two_stage(dataset_name, slots,
        test::GetterNoData(stuff)));

    try {
        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_WRITE, 0, slots);

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < slots; j++) {

                char* data = dataset.get_data(i+1, j);

                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {
        test::my_dataset dataset(dataset_name, 0, 0, slots);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = dataset.get_metadata(i+1);
            const char* const data = dataset.get_data(i+1);

            std::pair<std::vector<char>::iterator, const char*> comp;
            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data);

            ASSERT_TRUE(stuff[i] == meta);
            ASSERT_TRUE(stuff_data[i][0].end() == comp.first);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

}
