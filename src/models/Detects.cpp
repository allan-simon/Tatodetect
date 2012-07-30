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

#include "models/Detects.h"

bool compare(
 const std::pair<std::string, int> &p1,
 const std::pair<std::string, int> &p2
) {
    return p1.second < p2.second;
};


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

    std::cout << "[NOTICE] query" << query << std::endl;
    std::set<std::string> userLangs;
    if (!user.empty()) {
        userLangs = get_user_langs(user);
    }
    std::string detected = detects_n_gram(
        query,
        5,
        userLangs
    );

    if (detected == "unknown") {
        detected = detects_n_gram(
            query,
            3,
            userLangs
        );
        if (detected == "unknown") {
            detected = detects_n_gram(
                query,
                2
            );
        }
    }
    return detected;
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


/**
 *
 */
const std::string Detects::detects_n_gram(
    const std::string &query,
    const int ngramSize,
    const std::set<std::string> &userLangs 
    // add user spoken language vector
) {


    using namespace boost::locale;
    generator gen;
    // Make system default locale global
    std::locale loc = gen("en_US.UTF8");
    std::locale::global(loc); 
    std::cout.imbue(loc);
 
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
    boundary::ssegment_index::iterator p = index.begin();
    boundary::ssegment_index::iterator e = index.end();

    // simply to do  end - ngramsize 
    // TODO certainly a better way to do it
    for (
        int i = 1;
        i < ngramSize && e != p;
        ++i, e--
    ) {
    }

    // will contain a score based on how many time the ngrams appears
    // for a given language in the tatoeba database, as people are more
    // likely to detect sentences in common languages
    std::map<std::string, int> score;
    // will contain a score based on the frequency of
    // the ngrams in the language
    std::map<std::string, int> percentScore;
    std::map<std::string, int> uniqLangs;

    std::string tableName = "grams" + std::to_string(ngramSize);

    std::string selectNgramInfoSQL = 
        "SELECT lang, hit, percent FROM " + tableName + " "+
        "   WHERE gram = ? ";
        
    //TODO should be faster to directly make the user language filtering
    // there in the SQL request

    // now we cut the sentence ngram by ngram
    for(; p!=e; ++p) {
        std::string ngram = get_n_gram(p, ngramSize) ;
        cppdb::statement selectNgramInfo = sqliteDb.prepare(
            selectNgramInfoSQL
        );
        selectNgramInfo.bind(ngram);
        try {
            cppdb::result res = selectNgramInfo.query();
            // use to count the number of languages this ngram
            // appears in
            int ngramInXLangs = 0;
            int tmpHit = 0;
            int tmpPercent  = 0;
            std::string tmpLang = "";
            std::string lastFoundLang = "";
            while (res.next()) {

                tmpLang = res.get<std::string>("lang");

                if (
                    userLangs.empty() ||
                    userLangs.find(tmpLang) != userLangs.end()
                ) {
                    lastFoundLang = tmpLang;
                    tmpHit = res.get<int>("hit");
                    tmpPercent = res.get<int>("percent");


                    score[tmpLang] += tmpHit;
                    percentScore[tmpLang] += tmpPercent;
                    ngramInXLangs++;
                }
            }

            // if the ngram appears only in one language
            // then we apply to it a bonus as this ngram is more
            // significant to help guess which language it is

            if (ngramInXLangs == 1) {
                // we had this language to the list of languages
                // that have at least one ngram that is uniq
                // to that language
                uniqLangs[lastFoundLang] +=1;
               
                score[lastFoundLang] += tmpHit*tmpHit*50;
                percentScore[lastFoundLang] += tmpPercent*(1+tmpPercent)*100;

            }

        } catch (cppdb::cppdb_error const &e) {
            //TODO add something better to handle erros
            // at least log it
            std::cout << e.what() << std::endl;
            return "error";
        }


    }
    // if among the possible language only one contain ngram that the other
    // do not, then there's high chance that this language is the correct one
    if (uniqLangs.size() == 1) {
        return (*uniqLangs.begin()).first;
    }

    if (score.size() == 0 || percentScore.size() == 0) {
        return "unknown";
    }

    // we get the language that have the best frequency score
    std::pair<std::string, int> maxRelP = *std::max_element(
        percentScore.begin(),
        percentScore.end(),
        compare
    );

    // we get the language having the best absolute score
    std::pair<std::string, int> maxAbsP = *std::max_element(
        score.begin(),
        score.end(),
        compare
    );


    std::string maxRelLang = maxRelP.first;
    std::string maxAbsLang = maxAbsP.first;


    // we get relative (percent) score of the language having the
    // maximun absolute score
    int maxRelAbsScore = score[maxRelLang];
    // we get the absolute score of the language having the maximun
    // absolute score
    int maxAbsAbsScore = maxAbsP.second;

    // we do the same for the language with the best relative score
    int maxAbsRelScore = percentScore[maxAbsLang];
    int maxRelRelScore = maxRelP.second;

    // after we compare how these two languages perfom in the other
    // way, and we will take the one that perform the less "bad"
    float ratioAbs = std::numeric_limits<float>::infinity();

    if (maxRelAbsScore != 0) {
        ratioAbs = maxAbsAbsScore/maxRelAbsScore;
    }

    float ratioRel = std::numeric_limits<float>::infinity();
    if (maxAbsRelScore != 0) {
        ratioRel = maxRelRelScore/maxAbsRelScore;
    }

    if (ratioAbs > ratioRel) {
        return maxAbsLang;
    }
    return maxRelLang;

}

/**
 *
 */
const std::string Detects::get_n_gram(
    const boost::locale::boundary::ssegment_index::iterator & start,
    const int size
) {
    using namespace boost::locale;
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


