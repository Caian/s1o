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

#include <boost/mpl/pop_front.hpp>
#include <boost/mpl/front.hpp>
#include <boost/mpl/empty.hpp>
#include <boost/fusion/container/list/cons.hpp>
#include <boost/fusion/include/cons.hpp>

namespace s1o {
namespace helpers {

namespace detail {

template <typename T, bool empty>
struct mi_vector_to_cons_impl
{
    typedef typename boost::mpl::pop_front<
        T
        >::type T_next;

    typedef mi_vector_to_cons_impl<
        T_next,
        boost::mpl::empty<T_next>::value
        > next;

    typedef boost::fusion::cons<
        typename boost::mpl::front<T>::type,
        typename next::type
        > type;
};

template <typename T>
struct mi_vector_to_cons_impl<T, true>
{
    typedef boost::fusion::nil_ type;
};

}

/**
 * @brief Helper struct to convert a boost::mpl vector of indices into a
 * cons of those indices.
 *
 * @tparam T The type of the boost::mpl vector containing the indices.
 */
template <typename T>
struct mi_vector_to_cons
{
    typedef typename detail::mi_vector_to_cons_impl<
        T,
        boost::mpl::empty<T>::value
        >::type type;
};

}}
