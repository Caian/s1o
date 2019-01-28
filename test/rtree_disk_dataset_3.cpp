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
DATASET_TEST(FullTestSingleSlot, RWPMetaOnly)

    const int N = 100;
    const int slots = 1;

    test::meta_vector stuff;
    test::uid_vector uids;
    stuff.resize(N);
    uids.resize(N);

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

        uids[i] = stuff[i].uid;
    }

    try {

        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_NEW,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        ASSERT_NO_THROW(dataset.push_elements(stuff.begin(),
            stuff.begin() + N/2));

        for (size_t i = 1; i <= N/2; i++) {
            test::my_metadata meta;
            ASSERT_NO_THROW(dataset.read_element(i, meta));
        }

        ASSERT_NO_THROW(dataset.write_elements(stuff.begin(),
            stuff.begin() + N/2));

        for (size_t i = 1; i <= N/2; i++) {
            test::my_metadata meta;
            ASSERT_NO_THROW(dataset.read_element(i, meta));
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {

        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_WRITE,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        std::vector<s1o::uid_t> uids;
        ASSERT_NO_THROW(dataset.push_elements(stuff.begin() + N/2,
            stuff.begin() + N, std::back_inserter(uids)));

        for (size_t i = N/2+1; i <= N; i++) {
            test::my_metadata meta;
            ASSERT_NO_THROW(dataset.read_element(i, meta));
        }

        ASSERT_NO_THROW(dataset.write_elements(stuff.begin() + N/2,
            stuff.begin() + N));

        for (size_t i = N/2+1; i <= N; i++) {
            test::my_metadata meta;
            ASSERT_NO_THROW(dataset.read_element(i, meta));
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {

        test::my_dataset dataset(dataset_name, 0,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        for (size_t i = 0; i < N; i++) {
            test::my_metadata meta;
            ASSERT_NO_THROW(dataset.read_element(i+1, meta));
            ASSERT_TRUE(stuff[i] == meta);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(FullTestSingleSlot, RWPWithData)

    const int N = 100;
    const int slots = 1;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    test::uid_vector uids;
    stuff.resize(N);
    uids.resize(N);

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

        uids[i] = stuff[i].uid;

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    try {

        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_NEW,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        for (size_t i = 0; i < N/4; i++) {
            ASSERT_NO_THROW(dataset.push_element(stuff[i],
                &stuff_data[i][0][0]));
        }

        ASSERT_NO_THROW(dataset.push_elements(stuff.begin() + N/4,
            stuff.begin() + N/2));

        for (size_t i = N/4; i < N/2; i++) {
            ASSERT_NO_THROW(dataset.write_element(stuff[i],
                &stuff_data[i][0][0]));
        }

        ASSERT_NO_THROW(dataset.sync_metadata());
        ASSERT_NO_THROW(dataset.sync_data());

        for (size_t i = 0; i < N/2; i++) {

            test::my_metadata meta;
            std::vector<char> data(stuff_data[i][0].size());

            ASSERT_NO_THROW(dataset.read_element(i+1, meta, &data[0]));

            std::pair<std::vector<char>::iterator,
                      std::vector<char>::iterator> comp;

            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data.begin());

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

    try {

        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_WRITE,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        ASSERT_THROW(dataset.begin_elements(),
                s1o::location_data_unavailable_exception);

        ASSERT_THROW(dataset.end_elements(),
            s1o::location_data_unavailable_exception);

        ASSERT_THROW(dataset.begin_elements(slots),
            s1o::location_data_unavailable_exception);

        ASSERT_THROW(dataset.end_elements(slots),
            s1o::location_data_unavailable_exception);

        ASSERT_THROW(dataset.begin_metadata(),
            s1o::location_data_unavailable_exception);

        ASSERT_THROW(dataset.end_metadata(),
            s1o::location_data_unavailable_exception);

        for (size_t i = N/2; i < 3*N/4; i++) {
            ASSERT_NO_THROW(dataset.push_element(stuff[i],
                &stuff_data[i][0][0]));
        }

        ASSERT_NO_THROW(dataset.push_elements(stuff.begin() + 3*N/4,
            stuff.begin() + N));

        for (size_t i = 3*N/4; i < N; i++) {
            ASSERT_NO_THROW(dataset.write_element(stuff[i],
                &stuff_data[i][0][0]));
        }

        ASSERT_NO_THROW(dataset.sync_metadata());
        ASSERT_NO_THROW(dataset.sync_data());

        for (size_t i = 0; i < N; i++) {

            test::my_metadata meta;
            std::vector<char> data(stuff_data[i][0].size());

            ASSERT_NO_THROW(dataset.read_element(i+1, meta, &data[0]));

            std::pair<std::vector<char>::iterator,
                      std::vector<char>::iterator> comp;

            comp = std::mismatch(stuff_data[i][0].begin(),
                stuff_data[i][0].end(), data.begin());

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

    try {

        test::my_dataset dataset(dataset_name, 0,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        std::vector<test::my_metadata> meta;
        ASSERT_NO_THROW(dataset.read_elements(uids.begin(), uids.end(),
            std::back_inserter(meta)));

        ASSERT_TRUE(N == meta.size());

        for (size_t i = 0; i < N; i++) {

            ASSERT_TRUE(stuff[i] == meta[i]);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {

        test::my_dataset dataset(dataset_name, 0,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        std::vector<test::my_metadata> meta(N);
        ASSERT_NO_THROW(dataset.read_elements(uids.begin(), uids.end(),
            meta.begin()));

        ASSERT_TRUE(N == meta.size());

        for (size_t i = 0; i < N; i++) {

            ASSERT_TRUE(stuff[i] == meta[i]);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {

        test::my_dataset dataset(dataset_name, 0,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        std::vector<test::my_metadata> meta(N);
        ASSERT_NO_THROW(dataset.read_elements(uids.begin(), uids.end(),
            &meta[0]));

        ASSERT_TRUE(N == meta.size());

        for (size_t i = 0; i < N; i++) {

            ASSERT_TRUE(stuff[i] == meta[i]);
        }
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    try {

        test::my_dataset dataset(dataset_name, 0,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        std::vector<test::my_metadata> meta;
        ASSERT_NO_THROW(dataset.read_elements(uids.begin(), uids.end(),
            std::back_inserter(meta)));

        s1o::uid_t nouid = N+2;

        ASSERT_TRUE(false == dataset.read_element(nouid, meta[0]));

        ASSERT_TRUE(0 == dataset.read_elements(&nouid, &nouid+1,
            std::back_inserter(meta)));

        ASSERT_THROW(dataset.push_element(stuff[0]),
            s1o::read_only_exception);

        ASSERT_THROW(dataset.push_element(stuff[0], &stuff_data[0][0][0]),
            s1o::read_only_exception);

        ASSERT_THROW(dataset.write_element(stuff[0]),
            s1o::read_only_exception);

        ASSERT_THROW(dataset.write_element(stuff[0], &stuff_data[0][0][0]),
            s1o::read_only_exception);

        test::my_metadata nometa = meta[0];
        nometa.uid = static_cast<int>(nouid);

        ASSERT_THROW(dataset.write_element(nometa),
            s1o::invalid_uid_exception);

        ASSERT_THROW(dataset.write_elements(&nometa, &nometa+1),
            s1o::invalid_uid_exception);
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    // The disk version of the tree requires the dataset to
    // be created with the spatial storage because it is unsafe
    // to create one when opening
    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, 0, slots),
        boost::exception);

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(OpenCreate, WithoutWrite)

    const int slots = 1;

    ASSERT_THROW(test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_TRUNC,
        s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots),
        s1o::create_without_write_exception);

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(SlotMismatch, OneToMany)

    const int N = 100;
    const int slots1 = 1;
    const int slots2 = 5;

    test::meta_vector stuff;
    stuff.resize(N);

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
    }

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0, slots1, stuff.begin(), stuff.end()));

    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, 0, slots2),
        s1o::extra_slot_bytes_exception);

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(SlotMismatch, ManyToOne)

    const int N = 100;
    const int slots1 = 5;
    const int slots2 = 1;

    test::meta_vector stuff;
    stuff.resize(N);

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
    }

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0, slots1, stuff.begin(), stuff.end()));

    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, 0, slots2),
        s1o::inconsistent_data_exception);

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(ElementMismatch, Resize)

    const int N = 1;
    const int slots = 1;

    test::meta_vector stuff;
    stuff.resize(N);

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
    }

    try {

        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_NEW,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        ASSERT_NO_THROW(dataset.push_elements(stuff.begin(), stuff.end()));

        stuff[0].size += 1;

        ASSERT_THROW(dataset.write_element(stuff[0]),
            s1o::invalid_data_size_exception);
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

}
