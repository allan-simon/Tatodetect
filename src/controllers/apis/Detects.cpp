#include "Controller.h"
#include "Detects.h"
#include "contents/detects.h"

#include "models/Detects.h"

#include <cppcms/filters.h>


namespace controllers {
namespace apis {
/**
 *
 */
Detects::Detects(cppcms::service &serv) : Controller(serv) {
    // TODO have /  and /show directing to some "index" page 
    // as cburgmer pointed out, some people do navigate by crafting url
    cppcms::url_dispatcher* disp = &dispatcher();

  	disp->assign("/simple", &Detects::simple, this);
    detectsModel = new models::Detects();
}

/**
 *
 */
Detects::~Detects() {
    delete detectsModel;
}


/**
 *
 */
/*
void Detects::simple(
    std::string query,
    std::string lang
) {
    simple(query, lang, "1", "10");
}
*/
/**
 *
 */
void Detects::simple () {
    std::string query = "";
    std::string user = "";

    if (request().request_method() == "GET") {
        cppcms::http::request::form_type getData = request().get();
        cppcms::http::request::form_type::const_iterator it;
        
        GET_FIELD(user, "user");
        GET_FIELD(query, "query");
    }

    contents::detects::Simple c;
    init_content(c);

    c.detectedLang = detectsModel->simple(
        query,
        user
    );
    
    render("detects_simple_api",c);
}

} // End namespace apis
} // End namespace controllers

