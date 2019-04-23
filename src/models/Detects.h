/**
 * Tatoeba Project, free collaborative creation of multilingual corpuses project
 * Copyright (C) 2012 Allan SIMON <allan.simon@supinfo.com>
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
 * @package  Models
 * @author   Allan SIMON <allan.simon@supinfo.com>
 * @license  Affero General Public License
 * @link     http://tatoeba.org
 */ 

#include <set>
#include <booster/locale.h>
#include "models/SqliteModel.h"
namespace models {


/**
 * @class Detects
 * 
 * @brief This class permit access to the languages users speak and related
 *        information (who is native in what etc.)
 *
 */
class Detects : public SqliteModel {
    private:
        /**
         * @brief Will detect the lang based on a n-gram splitting of the query
         *
         * @param query     the text to detect the lang of
         * @param userLangs In order to provide more accurate results, list of
         *                  language the user speaks
         *
         * @return string   The iso code of the detected language
         *
         */
        const std::string detects_n_gram(
            const std::string &query,
            const std::set<std::string> &userLangs = std::set<std::string>()
        );

        /**
         * @brief Retrieve the list of languages spoken by a given user
         *
         * @param user the user nickname on Tatoeba
         *
         * @return a list of languages (iso 639-3 alpha 3 code)
         */
        const std::set<std::string> get_user_langs(
            const std::string &user
        );


        /**
         * @brief Get a simple n gram starting at a given point and of a given size
         *
         * @param start Iterator, where the n_gram will start
         * @param size,  the size of the ngram (in characters, not byte)
         *
         * @return the resulting n gram
         *
         */
        const std::string get_n_gram(
            const booster::locale::boundary::ssegment_index::iterator & start,
            const int size
        );

    public:
        /**
         * @brief Constructor
         */
        Detects();

        /**
         * @brief Will try to detect the most probable language of a
         *        given text using different methods
         *
         * @param query The text to detect
         * @param user  User that makes this query in order to provide
         *              a more accurate results
         * 
         * @return string the iso  639-3 alpha 3 code of the detected language
         *
         */
        const std::string simple(
            const std::string &query,
            const std::string &user = ""
        );
};

} // end namespace models 

