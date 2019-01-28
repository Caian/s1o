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

#include "test_rtree_disk.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <vector>
#include <cmath>

#include <stdlib.h>

namespace {

/**
 *
 */
DATASET_TEST(CreateFromExistingMultipleSlot, WithData)

    const int N = 50;
    const int slots = 3;

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

    try {
        test::my_dataset dataset(dataset_name,
            0, slots, stuff.begin(), stuff.end());

        for (int i = 0; i < N; i++) {

            test::my_metadata* meta;
            char* data;

            size_t size = stuff_data[i][0].size();

            dataset.get_element(i+1, meta, data);
            std::copy(&stuff_data[i][0][0], &stuff_data[i][0][0] +
                size/2, data);

            dataset.get_element(i+1, 0, meta, data);
            std::copy(&stuff_data[i][0][0] + size/2,
                &stuff_data[i][0][0] + size, data + size/2);

            for (int j = 1; j < slots; j++) {
                dataset.get_element(i+1, j, meta, data);
                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }

            ASSERT_THROW(dataset.get_element(i+1, slots, meta, data),
                s1o::invalid_slot_exception);
        }

        ASSERT_NO_THROW(dataset.sync_metadata());
        ASSERT_NO_THROW(dataset.sync_data());

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = dataset.get_metadata(i+1);
            const char* data = dataset.get_data(i+1);

            std::pair<std::vector<char>::iterator, const char*> comp;
            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data);

            ASSERT_TRUE(stuff[i] == meta);
            ASSERT_TRUE(stuff_data[i][0].end() == comp.first);
        }

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < slots; j++) {

                const char* data = dataset.get_data(i+1, j);

                std::pair<std::vector<char>::iterator, const char*> comp;
                comp = std::mismatch(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);

                ASSERT_TRUE(stuff_data[i][j].end() == comp.first);
            }
        }

        for (int i = 1; i <= N; i++) {

            ASSERT_THROW(dataset.get_data(i, slots),
                s1o::invalid_slot_exception);
        }

        {
            test::my_elem_iter ielem;
            test::my_elem_iter begin = dataset.begin_elements();
            test::my_elem_iter end = dataset.end_elements();

            for (ielem = begin; ielem != end; ielem++) {

                const test::my_metadata* meta = ielem->first;
                const char* data = ielem->second;

                int i = meta->uid - 1;

                std::pair<std::vector<char>::iterator, const char*> comp;
                comp = std::mismatch(stuff_data[i][0].begin(),
                    stuff_data[i][0].end(), data);

                ASSERT_TRUE(stuff[i] == *meta);
                ASSERT_TRUE(stuff_data[i][0].end() == comp.first);
            }
        }

        for (int j = 0; j < slots; j++) {

            test::my_elem_iter_s ielem;
            test::my_elem_iter_s begin = dataset.begin_elements(j);
            test::my_elem_iter_s end = dataset.end_elements(j);

            for (ielem = begin; ielem != end; ielem++) {

                const test::my_metadata* meta = ielem->first;
                const char* data = ielem->second;

                int i = meta->uid - 1;

                std::pair<std::vector<char>::iterator, const char*> comp;
                comp = std::mismatch(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);

                ASSERT_TRUE(stuff[i] == *meta);
                ASSERT_TRUE(stuff_data[i][j].end() == comp.first);
            }
        }

        {
            test::my_elem_iter_s ielem;
            test::my_elem_iter_s begin;
            test::my_elem_iter_s end;

            ASSERT_THROW(begin = dataset.begin_elements(slots),
                s1o::invalid_slot_exception);

            ASSERT_THROW(end = dataset.end_elements(slots),
                s1o::invalid_slot_exception);
        }

        {
            test::my_meta_iter imeta;
            test::my_meta_iter begin = dataset.begin_metadata();
            test::my_meta_iter end = dataset.end_metadata();

            for (imeta = begin; imeta != end; imeta++) {

                const test::my_metadata& meta = *imeta;
                int i = meta.uid - 1;

                ASSERT_TRUE(stuff[i] == meta);
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
            const char* data = dataset.get_data(i+1);

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
