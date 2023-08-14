set(PACKAGE overpass_api)
set(PACKAGE_NAME overpass_api)

cmake_minimum_required(VERSION 3.0.0)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(overpass_api VERSION 0.7.59.120)

set(PACKAGE_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}.${PROJECT_VERSION_TWEAK} (mmd)")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(HAVE_OVERPASS_XML "Build with Overpass XML dialect" ON)
option(HAVE_FASTCGI   "Build with FastCGI library" ON)
option(HAVE_POSIX     "Use C POSIX regular expresions" ON)
option(HAVE_PCRE      "Build with PCRE library" ON)
option(HAVE_PCREJIT   "Build with PCRE library (JIT)" ON)
option(HAVE_ICU       "BUild with ICU library" OFF)
option(HAVE_TCMALLOC  "Build with tcmalloc library" ON)
option(HAVE_LZ4       "Build with lz4 library" ON)
option(HAVE_OPENMP    "Build with OpenMP" ON)
option(BUILD_TESTS    "Build test suite" OFF)
option(HAVE_WORD_ALIGNMENT "Build some structures as packed with 4-byte alignment" OFF)
option(USE_UNITY_BUILD "Enable Unity Build" OFF)
option(ENABLE_INSTALL "Enable installation" ON)
option(OSMIUM_WITH_LZ4 "Enable lz4 support for libosmium" ON)


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

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-Wall)
if (CMAKE_VERSION VERSION_LESS 3.1)
  add_definitions(-std=c++17)
endif()

option(EXTERNAL_LIBOSMIUM "Do not use the bundled libosmium" OFF)
option(EXTERNAL_PROTOZERO "Do not use the bundled protozero" OFF)

set(HAVE_LIBOSMIUM 1)
set(PACKAGE_VERSION ${PACKAGE_VERSION})

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

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

#############################################################
# Detect available headers and set global compiler options
#############################################################

include(CheckIncludeFiles)

add_compile_options("-DHAVE_CONFIG_H=1")

#############################################################
# Find necessary libraries
#############################################################

find_package(Threads REQUIRED)
find_package(EXPAT REQUIRED)
find_package(ZLIB REQUIRED)
find_library(LIBRT rt) 
find_package(fmt REQUIRED)

if (HAVE_OPENMP)
  find_package(OpenMP)
endif()

if (HAVE_LZ4)
  find_package(LZ4 REQUIRED)
endif()


if (HAVE_FASTCGI)
  find_package(FCGI++ REQUIRED)
endif()

if (HAVE_PCRE OR HAVE_PCREJIT)
  find_package(PCRE2 REQUIRED)
endif()

if (HAVE_ICU)
  find_package(ICU COMPONENTS uc i18n)
endif()

if (HAVE_TCMALLOC)
  find_package(Tcmalloc REQUIRED)
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

set(FLATBUSH_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/flatbush")

find_package(Osmium 2.17.0 REQUIRED COMPONENTS io)
include_directories(SYSTEM ${OSMIUM_INCLUDE_DIRS} ${PROTOZERO_INCLUDE_DIR} ${FLATBUSH_INCLUDE_DIR})


############### Libraries are found now ########################

list(APPEND LIBS ${LIBRT})

list(APPEND LIBS ${OSMIUM_LIBRARIES})

list(APPEND COMPRESSION_LIBS ${ZLIB_LIBRARIES})

if (HAVE_LZ4)
  list(APPEND LIBS ${LZ4_LIBRARY})
  list(APPEND COMPRESSION_LIBS ${LZ4_LIBRARY})
endif()  

list(APPEND LIBS ${fmt_LIBRARY})

if (HAVE_ICU)
  list(APPEND LIBS ${ICU_LIBRARIES})
endif()

if (HAVE_TCMALLOC)
  list(APPEND LIBS ${Tcmalloc_LIBRARY})
endif()

if (HAVE_PCRE OR HAVE_PCREJIT)
  list(APPEND LIBS ${PCRE2_LIBRARIES})
endif()

if (HAVE_FASTCGI)
  list(APPEND LIBS ${FCGI_LIBRARY})
  list(APPEND LIBS ${FCGIPP_LIBRARY})  
endif()  


list(APPEND LIBS ${BZIP2_LIBRARIES})

message(STATUS "Libraries used to build: ${LIBS}")

#############################################################
# Build the library and executable file
#############################################################

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")

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

if (ENABLE_INSTALL)
  install(TARGETS osm3s_query update_database update_from_dir dispatcher uncgi tocgi escape_xml map_demo
                  export_tables import_tables dump_file
          PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
          DESTINATION "overpass/bin")
  install(TARGETS interpreter timestamp DESTINATION "overpass/cgi-bin")
  install(FILES   src/rules/areas.osm3s src/rules/areas_delta.osm3s src/rules/areas.ql src/rules/areas_delta.ql
          PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
          DESTINATION "overpass/rules")
  install(FILES   src/cgi-bin/kill_my_queries src/cgi-bin/map src/cgi-bin/status src/cgi-bin/trigger_clone
                  src/cgi-bin/augmented_diff src/cgi-bin/augmented_diff_status src/cgi-bin/augmented_state_by_date
          PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
          DESTINATION "overpass/cgi-bin")
  install(FILES   src/bin/apply_osc_to_db.sh src/bin/download_clone.sh src/bin/fetch_osc.sh src/bin/rules_delta_loop.sh
                  src/bin/rules_loop.sh src/bin/compress_clone.sh src/bin/clone.sh
                  src/bin/fetch_osc_and_apply.sh src/bin/init_osm3s.sh src/bin/run_osm3s_minutely.sh
          PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
          DESTINATION "overpass/bin")
  install(FILES COPYING
          PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
          DESTINATION "overpass/")
endif()


#############################################################
# Debian packaging
#############################################################

set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_DEBIAN_PACKAGE_NAME "overpassapi")
set(CPACK_PACKAGE_CONTACT "mmd.osm@gmail.com")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "mmd.osm@gmail.com")
set(CPACK_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
)
set(CPACK_PACKAGE_DESCRIPTION "A comprehensive list of changes and enhancements can be found at https://github.com/mmd-osm/Overpass-API. Refer to https://overpass-api.de for the original Overpass API project. This fork is based on release 0.7.57.")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Overpass API fork, includes a few performance and functional enhancements")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS YES)
set(CPACK_PACKAGING_INSTALL_PREFIX "/opt")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/mmd-osm/Overpass-API")
set(CPACK_STRIP_FILES "TRUE")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/COPYING")
INCLUDE(CPack)

