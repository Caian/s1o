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

#include "test_rtree_disk_slim.hpp"

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
DATASET_TEST(FileNotFound, Open)

    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, 0, 1),
        s1o::io_exception);

    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0,
        s1o::S1O_FLAGS_NO_DATA, 1), s1o::io_exception);
}

/**
 *
 */
DATASET_TEST(EmptyDataset, Open)

    const int slots = 1;

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        slots));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        0,
        slots), s1o::empty_mmap_exception);

    // The disk version of the tree requires the dataset to
    // be created with the spatial storage because it is unsafe
    // to create one when opening
    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_NO_DATA,
        0), std::exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        slots));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP,
        slots), s1o::unsorted_data_exception);

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(EmptyDataset, OpenSeveralSlots)

    const int slots = 10;

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        slots), s1o::invalid_num_slots_exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        1));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        0,
        slots), s1o::empty_mmap_exception);

    // The disk version of the tree requires the dataset to
    // be created with the spatial storage because it is unsafe
    // to create one when opening
    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_NO_DATA,
        0), std::exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        slots));

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(DatasetNoData, Open)

    const int slots = 1;

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED |
        s1o::S1O_FLAGS_NO_DATA,
        slots), s1o::invalid_num_slots_exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED |
        s1o::S1O_FLAGS_NO_DATA,
        0));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_NO_DATA,
        slots), s1o::invalid_num_slots_exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        s1o::S1O_OPEN_NEW,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_NO_DATA,
        0));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        0,
        slots), s1o::io_exception);

    // The disk version of the tree requires the dataset to
    // be created with the spatial storage because it is unsafe
    // to create one when opening
    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_NO_DATA,
        0), std::exception);

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED,
        slots), s1o::io_exception);

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED |
        s1o::S1O_FLAGS_NO_DATA,
        slots), s1o::invalid_num_slots_exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_ALLOW_UNSORTED |
        s1o::S1O_FLAGS_NO_DATA,
        0));

    ASSERT_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_NO_DATA,
        slots), s1o::invalid_num_slots_exception);

    ASSERT_NO_THROW(test::my_dataset dataset(dataset_name,
        0,
        s1o::S1O_FLAGS_RWP |
        s1o::S1O_FLAGS_NO_DATA,
        0));

    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(MappedDatasetSingleSlot, AttemptRWP)

    const int N = 10;
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
        test::my_dataset dataset(dataset_name,
            0, slots, stuff.begin(), stuff.end());

        ASSERT_THROW(dataset.read_element(1, stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.read_elements(uids.begin(), uids.end(),
            stuff.begin()), s1o::mmapped_exception);

        ASSERT_THROW(dataset.write_element(stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.write_elements(stuff.begin(), stuff.end()),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_element(stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_elements(stuff.begin(), stuff.end()),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_elements(stuff.begin(), stuff.end(),
            uids.begin()), s1o::mmapped_exception);
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
DATASET_TEST(MappedDatasetMultiSlot, AttemptRWP)

    const int N = 10;
    const int slots = 5;

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
        test::my_dataset dataset(dataset_name,
            0, slots, stuff.begin(), stuff.end());

        ASSERT_THROW(dataset.read_element(1, stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.read_elements(uids.begin(), uids.end(),
            stuff.begin()), s1o::mmapped_exception);

        ASSERT_THROW(dataset.write_element(stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.write_elements(stuff.begin(), stuff.end()),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_element(stuff[0]),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_elements(stuff.begin(), stuff.end()),
            s1o::mmapped_exception);

        ASSERT_THROW(dataset.push_elements(stuff.begin(), stuff.end(),
            uids.begin()), s1o::mmapped_exception);
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
