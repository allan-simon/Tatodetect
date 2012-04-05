#include "models/SqliteModel.h"
#include "generics/Config.h"

namespace models {
SqliteModel::SqliteModel():
    sqliteDb(
        cppdb::session(
            "sqlite3:db=" + Config::get_instance()->sqlite3Path
        )
    )
{
    // We need this to have triggers call even in some tricky case 
    // (for example "update or replace" that cause a deletion, will not call
    // the delete trigger otherwise)
    sqliteDb.create_statement("PRAGMA recursive_triggers = 1 ;").exec();
}


SqliteModel::SqliteModel(cppdb::session sqliteDb) : sqliteDb(sqliteDb) {

}

} // end of namespace models
