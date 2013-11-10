cmake_minimum_required(VERSION 2.6)

project(tatoedetect)
include_directories(
    src
)

# set the version
set(PROJECT_VERSION "0.2")

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${PROJECT_VERSION})
add_custom_target(
    dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}/ HEAD
        | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)


#set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")



file(
    GLOB
    controllers_files
    src/controllers/*
    src/controllers/webs/*
    src/controllers/apis/*
    src/controllers/generics/*
)

file(
    GLOB
    models_files
    src/models/*
)

file(
    GLOB
    results_files
    src/results/*
)

file(
    GLOB
    contents_files
    src/contents/*
)


file(
    GLOB
    generics_files
    src/generics/*
)




add_executable(
    tatodetect
    src/tatodetect.cpp
    src/main.cpp
    ${controllers_files}
    ${models_files}
    ${generics_files}
    ${results_files}
    ${contents_files}
    view_web.cpp
)

target_link_libraries(
    tatodetect
    cppcms
    cppdb
    booster
    sqlite3
)

add_custom_command(
    OUTPUT view_web.cpp
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tools/cppcms_tmpl_cc 
        ${CMAKE_CURRENT_SOURCE_DIR}/src/views/web/*/*.tmpl 
        ${CMAKE_CURRENT_SOURCE_DIR}/src/views/api/*/*.tmpl 
        -o view_web.cpp
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/views/web/*/*.tmpl 
        ${CMAKE_CURRENT_SOURCE_DIR}/src/views/api/*/*.tmpl 
)
