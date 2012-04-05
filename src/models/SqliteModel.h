#ifndef MODELS_SQLITE_H
#define MODELS_SQLITE_H

#include <cppdb/frontend.h>

namespace models {

class SqliteModel {
    protected:
        cppdb::session sqliteDb;
    public:
        SqliteModel();
        SqliteModel(cppdb::session sqliteDb);
};

}

#endif
