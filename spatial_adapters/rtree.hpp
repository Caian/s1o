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

#include "rtree_base.hpp"

#include <boost/container/new_allocator.hpp>

namespace s1o {
namespace spatial_adapters {

/**
 * @brief A spatial adapter based on boost's rtree implementation that is
 * instanced entirely in memory.
 *
 * @tparam Params The parameters used to control the tree.
 * @tparam CoordSys The coordinate system used by the tree.
 */
template <
    typename Params,
    typename CoordSys
    >
struct rtree :
    public rtree_base<
        Params,
        CoordSys,
        boost::container::new_allocator<void>
        >
{
};

}}
