/**
 * Tatoeba Project, free collaborative creation of multilingual corpuses project
 * Copyright (C) 2011 Allan SIMON <allan.simon@supinfo.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 * @category Tatodetect
 * @package  Contents
 * @author   Allan SIMON <allan.simon@supinfo.com>
 * @license  Affero General Public License
 * @link     http://tatoeba.org
 */


#ifndef TATOEBACPP_CONTENTS_DETECTS_H
#define TATOEBACPP_CONTENTS_DETECTS_H

#include "contents/content.h"

namespace contents {
namespace detects {

/**
 * Base content for every action of Detects controller
 *
 */
struct Detects : public BaseContent {
};

/**
 * @struct Simple
 * Content used by the Simple detection api
 */
struct Simple : public Detects {
    public: 
        /**
         * to store the language detected by the algo
         */
        std::string detectedLang;

        Simple() {}

};


} // end of namespace detects
} //end of namespace contents

#endif

