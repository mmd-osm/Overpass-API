set(PACKAGE overpass_api)
set(PACKAGE_NAME overpass_api)
set(PACKAGE_VERSION 0.7.59_mmd)

cmake_minimum_required(VERSION 2.8.12)

project(overpass_api)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(HAVE_OVERPASS_XML "Build with Overpass XML dialect" ON)
option(HAVE_POPUP_OUTPUT "Build with [out:popup] support" OFF)
option(HAVE_CUSTOM_OUTPUT "Build with [out:custom] support" OFF)
option(HAVE_LIBOSMIUM "Build with libosmium library" ON)
option(HAVE_FASTCGI   "Build with FastCGI library" ON)
option(HAVE_PCRE      "Build with PCRE library" ON)
option(HAVE_ICU       "BUild with ICU library" OFF)
option(HAVE_TCMALLOC  "Build with tcmalloc library" ON)
option(HAVE_LZ4       "Build with lz4 library" ON)
option(HAVE_OPENMP    "Build with OpenMP" ON)
option(BUILD_TESTS    "Build test suite" OFF)


if (PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
    message(FATAL_ERROR "In-source builds are not allowed, please use a separate build directory like `mkdir build && cd build && cmake ..`")
endif()

message(STATUS "Building Overpass API ${PACKAGE_VERSION}")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

if (NOT "${CMAKE_CXX_STANDARD}")
    set(CMAKE_CXX_STANDARD 17)
endif()
message(STATUS "Building in C++${CMAKE_CXX_STANDARD} mode")
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-Wall)
if (CMAKE_VERSION VERSION_LESS 3.1)
  add_definitions(-std=c++17)
endif()


option(EXTERNAL_LIBOSMIUM "Do not use the bundled libosmium" OFF)
option(EXTERNAL_PROTOZERO "Do not use the bundled protozero" OFF)


#############################################################
# Version
#############################################################

find_package(Git)

if (GIT_FOUND)
    execute_process(COMMAND "${GIT_EXECUTABLE}" log -1 --pretty=%h
                    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
                    OUTPUT_VARIABLE VERSION_FROM_GIT
                    ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (VERSION_FROM_GIT)
        set(GIT_VERSION "${VERSION_FROM_GIT}")
#    message(STATUS "Git version: ${GIT_VERSION}")     
    endif()
endif()

#configure_file(
#    ${PROJECT_SOURCE_DIR}/src/version.cpp.in
#    ${PROJECT_BINARY_DIR}/src/version.cpp
#)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

#############################################################
# Detect available headers and set global compiler options
#############################################################

include(CheckIncludeFiles)

#############################################################
# Find necessary libraries
#############################################################

find_package(Threads)
find_package(EXPAT)
find_package(ZLIB REQUIRED)
find_library(LIBRT rt) 
find_package(fmt)

if (HAVE_OPENMP)
  find_package(OpenMP)
endif()  

if (HAVE_LZ4)
  find_package(LZ4)
endif()  


if (HAVE_FASTCGI)
  find_package(FCGI++)
endif()

if (HAVE_PCRE)
  find_package(PCRE2)
endif()

if (HAVE_ICU)
  find_package(ICU)
endif()

if (HAVE_TCMALLOC)
  find_package(Tcmalloc)
endif()

if (NOT EXTERNAL_LIBOSMIUM)
    set(OSMIUM_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/libosmium/include")
endif()

if (NOT EXTERNAL_PROTOZERO)
    set(PROTOZERO_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/protozero/include")
endif()

#if (NOT EXTERNAL_FMT)
#    set(FMT_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/contrib/fmt/include")
#endif()


find_package(Osmium 2.17.0 REQUIRED COMPONENTS io)
include_directories(SYSTEM ${OSMIUM_INCLUDE_DIRS} ${PROTOZERO_INCLUDE_DIR} ${FMT_INCLUDE_DIR})

############### Libraries are found now ########################

set(LIBS ${OSMIUM_LIBRARIES} ${LIBRT} ${LZ4_LIBRARY}
    ${ICU_LIBRARIES}
    ${fmt_LIBRARY}
    ${Tcmalloc_LIBRARY}
    ${PCRE2_LIBRARIES}
    ${FCGI_LIBRARY}
    ${FCGIPP_LIBRARY}
    ${BZIP2_LIBRARIES})

message(STATUS "Libraries used to build: ${LIBS}")

#############################################################
# Build the library and executable file
#############################################################

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")

add_compile_options("-DHAVE_CONFIG_H=1")

add_subdirectory(src)

#############################################################
# Build tests
#############################################################

if (BUILD_TESTS)
 enable_testing()
 add_subdirectory(src/test-bin)
endif()

#############################################################
# Install
#############################################################

install(TARGETS osm3s_query update_database update_from_dir dispatcher uncgi tocgi escape_xml DESTINATION "/opt/overpass/bin")
install(TARGETS interpreter DESTINATION "/opt/overpass/cgi-bin")
install(FILES   src/rules/areas.osm3s src/rules/areas_delta.osm3s src/rules/areas.ql src/rules/areas_delta.ql DESTINATION "/opt/overpass/rules")

#############################################################
# Debian packaging
#############################################################

SET(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION_MAJOR "0")
set(CPACK_PACKAGE_VERSION_MINOR "7")
set(CPACK_PACKAGE_VERSION_PATCH "59")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "mmd.osm@gmail.com")
set(CPACK_PACKAGE_DESCRIPTION "Overpass API fork, includes a few performance and functional enhancements")
SET(CPACK_DEBIAN_PACKAGE_DEPENDS "libfcgi-bin,zlib1g,expat,libicu66,liblz4-1,libpcre2-8-0,libgomp1,libgoogle-perftools4")
INCLUDE(CPack)

