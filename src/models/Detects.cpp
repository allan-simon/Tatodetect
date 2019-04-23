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
 * @package  Models
 * @author   Allan SIMON <allan.simon@supinfo.com>
 * @license  Affero General Public License
 * @link     http://tatoeba.org
 */

#include <iostream>
#include <string>
#include <map>
#include <algorithm>
#include <limits>
#include <cppdb/frontend.h>
#include <math.h>

#include "models/Detects.h"

namespace models {

/**
 *
 */
Detects::Detects() :
    SqliteModel()
{
}

/**
 * @TODO throw exception if language has already been added for that
 *       user
 */
const std::string Detects::simple(
    const std::string &query,
    const std::string &user
) {

#ifdef DEBUG
    std::cout << "[NOTICE] query" << query << std::endl;
#endif
    std::set<std::string> userLangs;
    if (!user.empty()) {
        userLangs = get_user_langs(user);
    }
    return detects_n_gram(
        query,
        userLangs
    );
}

/**
 *
 */
const std::set<std::string> Detects::get_user_langs(
    const std::string &user
) {
    std::string selectUserLangSQL =
        "SELECT lang  FROM users_langs "
        "   WHERE user = ? ";
    cppdb::statement selectUserLang = sqliteDb.prepare(
       selectUserLangSQL
    );
    selectUserLang.bind(user);

    std::set<std::string> userLangs;
    try {
        cppdb::result res = selectUserLang.query();
        while (res.next()) {
            userLangs.insert(
                res.get<std::string>("lang")
            );
        }
    } catch (cppdb::cppdb_error const &e) {
        //TODO add something better to handle erros
        // at least log it
        std::cout << e.what() << std::endl;
    }
    return userLangs;


}

const std::string Detects::detects_n_gram(
    const std::string &query,
    const std::set<std::string> &userLangs
    // add user spoken language vector
) {
    using namespace booster::locale;

    boundary::ssegment_index index(
        boundary::word,
        query.begin(),
        query.end()
    );
    index.map(
        boundary::character,
        query.begin(),
        query.end()
    );
    boundary::ssegment_index::iterator p;
    boundary::ssegment_index::iterator e;
    std::map<std::string, std::pair<int, float>> scores;

    for (int ngramSize = 2; ngramSize <= 5; ngramSize++) {
        // simply to do  end - ngramsize
        // TODO certainly a better way to do it
        e = index.end();
        for (
            int i = 1;
            i < ngramSize && e != p;
            ++i, e--
        ) {
        }

        std::string tableName = "grams" + std::to_string(ngramSize);

        std::string placeholders = "";
        p = index.begin();
        while (p != e) {
            placeholders += "?";
            p++;
            if (p != e) {
                placeholders += ", ";
            }
        }
        std::string selectNgramInfoSQL =
            "SELECT lang, count(lang) as total, sum(percent*percent*hit) as score FROM " + tableName + " " +
            "   WHERE gram IN (" + placeholders + ") group by lang";

        cppdb::statement selectNgramInfo = sqliteDb.prepare(
            selectNgramInfoSQL
        );

        for(p = index.begin(); p!=e; ++p) {
            std::string ngram = get_n_gram(p, ngramSize) ;
            selectNgramInfo.bind(ngram);
#ifdef DEBUG
            std::cout << "[NOTICE] ngram: " << ngram << std::endl;
#endif
        }

        try {
            cppdb::result res = selectNgramInfo.query();
            int total = 0;
            float score = 0.0;
            std::string lang = "";

            while (res.next()) {

                lang = res.get<std::string>("lang");
                total = res.get<int>("total");
                score = res.get<float>("score");

                if (scores.count(lang)) {
                    scores[lang].first += total;
                    scores[lang].second += score;
                } else {
                    scores[lang] = std::pair<int, float> (total, score);
                }
#ifdef DEBUG
                std::cout << "score[" << ngramSize << "]: " << lang << " " << total << " " << score << std::endl;
#endif
            }

        } catch (cppdb::cppdb_error const &e) {
            //TODO add something better to handle erros
            // at least log it
            std::cout << e.what() << std::endl;
            return "error";
        }
    }

    std::string detected = "unknown";

#ifdef DEBUG
    // need to convert into a vector in order to sort by value...
    typedef std::pair<std::string, std::pair<int, float>> score_t;
    std::vector<score_t> scores_v;

    for (auto itr = scores.begin(); itr != scores.end(); ++itr)
        scores_v.push_back(*itr);
    std::sort(
        scores_v.begin(),
        scores_v.end(),
        [=](score_t& a, score_t& b) {
            return a.second.first < b.second.first;
        }
    );
    for (auto score : scores_v) {
        std::cout << "score[total]: " << score.first << " " << score.second.first << " " << score.second.second << std::endl;
    }
#endif

    // we get the max total
    int maxTotal = 0;
    for (auto score : scores) {
        int total = score.second.first;
        if (total > maxTotal) {
            maxTotal = total;
        }
    }

    // we get the language having the best score
    // among the ones within range
    int range = sqrt(maxTotal)/3;
    float maxScore = 0.0;
    for (auto score : scores) {
        float s = score.second.second;
        if (score.second.first >= maxTotal - range && s > maxScore) {
#ifdef DEBUG
            std::cout << "considering: " << score.first << " " << score.second.first << " " << s << std::endl;
#endif
            maxScore = s;
            detected = score.first;
        }
    }
#ifdef DEBUG
    std::cout << "detected: " << detected << std::endl;
#endif
    return detected;
}

/**
 *
 */
const std::string Detects::get_n_gram(
    const booster::locale::boundary::ssegment_index::iterator & start,
    const int size
) {
    using namespace booster::locale;
    std::string ngram = "";
    boundary::ssegment_index::iterator ngramIt;
    int i = 0;
    for (
        ngramIt = start;
        i < size;
        ++ngramIt, ++i
    ) {
        ngram += *ngramIt;
    }
    return ngram;
}

} // end namespace models


