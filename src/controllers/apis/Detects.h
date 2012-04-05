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
 * @category Tatoedetect
 * @package  Controllers
 * @author   Allan SIMON <allan.simon@supinfo.com>
 * @license  Affero General Public License
 * @link     http://tatoeba.org
 */


#ifndef CONTROLLERS_APIS_DETECTS_H
#define CONTROLLERS_APIS_DETECTS_H

#include "Controller.h"

namespace models {
    class Detects;
}


namespace controllers {
namespace apis {

/**
 * @class Detects
 * Class that will controll all the page request related to the 
 * search engine
 */
class Detects : public Controller {
    private:
        /**
         * Model class for the language detection
         */
        models::Detects *detectsModel;
	public:
        /**
         * Constructor, will attach the url to the dispatcher
         * and instantiate the model
         */
		Detects(cppcms::service &serv);
        
        /**
         * Destructor
         */
        ~Detects();

        /**
         * Display the simple detection method 
         */
        void simple();


};

} // End namespace apis
} // End namespace 

#endif


