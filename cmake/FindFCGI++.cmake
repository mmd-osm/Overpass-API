# CMake module to search for FastCGI++ headers
#
# If it's found it sets FCGIPP_FOUND to TRUE
# and following variables are set:
#    FCGIPP_INCLUDE_DIR
#    FCGIPP_LIBRARY
FIND_PATH(FCGIPP_INCLUDE_DIR
  fcgio.h
  PATHS
  /usr/include
  /usr/local/include
  /usr/include/fastcgi
  "$ENV{LIB_DIR}/include"
  $ENV{INCLUDE}
  )
FIND_LIBRARY(FCGIPP_LIBRARY NAMES fcgi++ libfcgi++ PATHS 
  /usr/local/lib 
  /usr/lib 
  "$ENV{LIB_DIR}/lib"
  "$ENV{LIB}"
  )

FIND_LIBRARY(FCGI_LIBRARY NAMES fcgi libfcgi PATHS 
  /usr/local/lib 
  /usr/lib 
  "$ENV{LIB_DIR}/lib"
  "$ENV{LIB}"
  )

IF (FCGIPP_INCLUDE_DIR AND FCGIPP_LIBRARY AND FCGI_LIBRARY)
   SET(FCGIPP_FOUND TRUE)
ENDIF (FCGIPP_INCLUDE_DIR AND FCGIPP_LIBRARY AND FCGI_LIBRARY)

IF (FCGIPP_FOUND)
   IF (NOT FCGIPP_FIND_QUIETLY)
      MESSAGE(STATUS "Found FCGI: ${FCGI_LIBRARY}")   
      MESSAGE(STATUS "Found FCGI++: ${FCGIPP_LIBRARY}")
   ENDIF (NOT FCGIPP_FIND_QUIETLY)
ELSE (FCGIPP_FOUND)
   IF (FCGIPP_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find FCGI++")
   ENDIF (FCGIPP_FIND_REQUIRED)
ENDIF (FCGIPP_FOUND)
