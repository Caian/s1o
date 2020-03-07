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

namespace s1o {
namespace helpers {

template <typename T, typename IT, unsigned int N>
struct copy_location_impl
{
    static const unsigned int I = N-1;

    typedef copy_location_impl<
        T,
        IT,
        N-1
        > next_t;

    static void call(const T& location, IT out)
    {
        next_t::call(location, out);
        *out = location.template get<I>();
        out++;
    }
};

template <typename T, typename IT>
struct copy_location_impl<T, IT, 0>
{
    static void call(const T& location, IT out)
    {
        (void)location;
        (void)out;
    }
};

/**
 * @brief Copy the coordinates of a location to an iterator.
 *
 * @tparam N The number of coordinates to copy.
 * @tparam T The type of the location data.
 * @tparam IT The type of the output iterator.
 *
 * @param location The source location object.
 * @param out The output iterator.
 *
 * @note The location type must implement a 'get' template with no arguments
 * and with the index of the coordinate to get being the only template
 * argument.
 */
template <unsigned int N, typename T, typename IT>
void copy_location(const T& location, IT out)
{
    copy_location_impl<T, IT, N>::call(location, out);
}

}}
