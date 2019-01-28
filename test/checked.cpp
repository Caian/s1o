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

#include <s1o/checked.hpp>
#include <gtest/gtest.h>

/**
 * Simple Open/Close file test.
 */
TEST(CheckedTest, OpenClose)
{
    const int fd = s1o::open_checked("checked_test_openclose",
        O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);

    s1o::close_checked(fd);
}

/**
 * Set file size using seek + write.
 */
TEST(CheckedTest, SetSize)
{
    const size_t new_size = 15*1024 + 3;
    const char zero = '\0';

    const int fd = s1o::open_checked("checked_test_setsize",
        O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);

    s1o::lseek64_checked(fd, new_size-1, SEEK_SET);
    s1o::write_checked(fd, &zero, 1);
    s1o::lseek64_checked(fd, 0, SEEK_END);

    const off64_t size = s1o::lseek64_checked(fd, 0, SEEK_CUR);

    s1o::close_checked(fd);

    ASSERT_TRUE(new_size == size);
}

/**
 * Simple write + read test.
 */
TEST(CheckedTest, ReadWrite)
{
    const char data[] = "Th1sIsAt3st!!!!";
    const size_t size = sizeof(data) / sizeof(data[0]) + 1;
    char read[size];

    const int fd = s1o::open_checked("checked_test_readwrite",
        O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);

    s1o::write_checked(fd, &data, size);
    s1o::lseek64_checked(fd, 0, SEEK_SET);
    s1o::read_checked(fd, &read, size);
    s1o::close_checked(fd);

    ASSERT_STREQ(data, read);
}

/**
 * Test exceptions for all checked IO methods.
 */
TEST(CheckedTest, FailAll)
{
    int fd = -1;
    char thing = '\0';

    EXPECT_THROW(s1o::open_checked("checked_test_failall",
        O_RDONLY, S_IRUSR | S_IWUSR), s1o::io_exception);
    EXPECT_THROW(s1o::read_checked(fd, &thing, 1), s1o::io_exception);
    EXPECT_THROW(s1o::write_checked(fd, &thing, 1), s1o::io_exception);
    EXPECT_THROW(s1o::close_checked(fd), s1o::io_exception);
    EXPECT_THROW(s1o::lseek64_checked(fd, 0, 0), s1o::io_exception);
}
